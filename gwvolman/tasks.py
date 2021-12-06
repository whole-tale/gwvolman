"""A set of WT related Girder tasks."""
from datetime import datetime, timedelta
import os
import shutil
import socket
import json
import time
import tempfile
import docker
import requests
import subprocess
from docker.errors import DockerException
import girder_client
from dateutil.parser import parse

import logging
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from girder_worker.utils import girder_job
from girder_worker.app import app
# from girder_worker.plugins.docker.executor import _pull_image
from .utils import \
    HOSTDIR, REGISTRY_USER, REGISTRY_PASS, \
    new_user, _safe_mkdir, _get_api_key, \
    _get_container_config, _launch_container, _get_user_and_instance, \
    _build_image, _recorded_run, DEPLOYMENT

from .lib.dataone.publish import DataONEPublishProvider
from .lib.zenodo import ZenodoPublishProvider

from .constants import GIRDER_API_URL, InstanceStatus, ENABLE_WORKSPACES, \
    DEFAULT_USER, DEFAULT_GROUP, MOUNTPOINTS, REPO2DOCKER_VERSION, TaleStatus, \
    RunStatus

CREATE_VOLUME_STEP_TOTAL = 2
LAUNCH_CONTAINER_STEP_TOTAL = 2
UPDATE_CONTAINER_STEP_TOTAL = 2
BUILD_TALE_IMAGE_STEP_TOTAL = 2
IMPORT_TALE_STEP_TOTAL = 2
RECORDED_RUN_STEP_TOTAL = 4


@girder_job(title='Create Tale Data Volume')
@app.task(bind=True)
def create_volume(self, instance_id):
    """Create a mountpoint and compose WT-fs."""
    user, instance = _get_user_and_instance(self.girder_client, instance_id)
    tale = self.girder_client.get('/tale/{taleId}'.format(**instance))

    vol_name = "%s_%s_%s" % (tale['_id'], user['login'], new_user(6))
    cli = docker.from_env(version='1.28')

    self.job_manager.updateProgress(
        message='Creating volume', total=CREATE_VOLUME_STEP_TOTAL,
        current=1, forceFlush=True)

    mountpoint = _create_docker_volume(cli, vol_name)

    homeDir = self.girder_client.loadOrCreateFolder(
        'Home', user['_id'], 'user')
    data_dir = os.path.join(mountpoint, 'data')
    versions_dir = os.path.join(mountpoint, 'versions')
    runs_dir = os.path.join(mountpoint, 'runs')
    if ENABLE_WORKSPACES:
        work_dir = os.path.join(mountpoint, 'workspace')

    _make_fuse_dirs(mountpoint, MOUNTPOINTS)

    api_key = _get_api_key(self.girder_client)

    session = _get_session(self.girder_client, tale=tale)

    if session['_id'] is not None:
        _mount_girderfs(mountpoint, 'data', 'wt_dms', session['_id'], api_key, hostns=True)

    #  webdav relies on mount.c module, don't use hostns for now
    _mount_girderfs(mountpoint, 'home', 'wt_home', homeDir['_id'], api_key)

    if ENABLE_WORKSPACES:
        _mount_girderfs(mountpoint, 'workspace', 'wt_work', tale['_id'], api_key)
        _mount_girderfs(mountpoint, 'versions', 'wt_versions', tale['_id'], api_key, hostns=True)
        _mount_girderfs(mountpoint, 'runs', 'wt_runs', tale['_id'], api_key, hostns=True)

    self.job_manager.updateProgress(
        message='Volume created', total=CREATE_VOLUME_STEP_TOTAL,
        current=CREATE_VOLUME_STEP_TOTAL, forceFlush=True)
    print("WT Filesystem created successfully.")

    return dict(
        nodeId=cli.info()['Swarm']['NodeID'],
        mountPoint=mountpoint,
        volumeName=vol_name,
        sessionId=session['_id'],
        instanceId=instance_id,
    )


@girder_job(title='Spawn Instance')
@app.task(bind=True)
def launch_container(self, payload):
    """Launch a container using a Tale object."""
    user, instance = _get_user_and_instance(
        self.girder_client, payload['instanceId'])
    tale = self.girder_client.get('/tale/{taleId}'.format(**instance))

    self.job_manager.updateProgress(
        message='Starting container', total=LAUNCH_CONTAINER_STEP_TOTAL,
        current=1, forceFlush=True)

    print("Launching container for a Tale...")
    if 'imageInfo' not in tale:

        # Wait for image to be built
        tic = time.time()
        timeout = 180.0
        time_interval = 5

        while time.time() - tic < timeout:
            tale = self.girder_client.get('/tale/{taleId}'.format(**instance))
            if 'imageInfo' in tale and 'digest' in tale['imageInfo']:
                break
            msg = f"Waiting for image build to complete. ({time_interval}s)"
            logging.info(msg)
            print(msg)
            time.sleep(5)

    # _pull_image() #FIXME
    container_config = _get_container_config(self.girder_client, tale)
    service, attrs = _launch_container(
        payload['volumeName'], payload['nodeId'],
        container_config,
        tale_id=tale['_id'], instance_id=payload['instanceId'])
    print(
        f"Started a container using volume: {payload['volumeName']} "
        f"on node: {payload['nodeId']}"
    )

    # wait until task is started
    tic = time.time()
    timeout = 300.0
    started = False

    print("Waiting for the environment to be accessible...")
    while time.time() - tic < timeout:
        try:
            status = service.tasks()[0]['Status']

            if status['State'] in {"failed", "rejected"}:
                raise ValueError("Failed to start environment: %s" % status['Err'])
            elif status['State'] == "running":
                started = True
                break

        except IndexError:
            started = False

        time.sleep(0.2)

    if not started:
        raise ValueError("Tale did not start before timeout exceeded")

    print("Environment is up and running.")
    self.job_manager.updateProgress(
        message='Container started', total=LAUNCH_CONTAINER_STEP_TOTAL,
        current=LAUNCH_CONTAINER_STEP_TOTAL, forceFlush=True)

    payload.update(attrs)
    payload['name'] = service.name
    return payload

@girder_job(title='Update Instance')
@app.task(bind=True)
def update_container(task, instanceId, digest=None):
    user, instance = _get_user_and_instance(task.girder_client, instanceId)

    cli = docker.from_env(version='1.28')
    if 'containerInfo' not in instance:
        return
    containerInfo = instance['containerInfo']  # VALIDATE
    try:
        service = cli.services.get(containerInfo['name'])
    except docker.errors.NotFound:
        logging.info("Service not present [%s].", containerInfo['name'])
        return

    task.job_manager.updateProgress(
        message='Restarting the Tale with a new image',
        total=UPDATE_CONTAINER_STEP_TOTAL,
        current=1, forceFlush=True)

    # Don't try to restart if the image hasn't changed
    try:
        previous_image = service.attrs['Spec']['TaskTemplate']['ContainerSpec']['Image']
    except KeyError:
        previous_image = ''

    if (previous_image == digest):
        task.job_manager.updateProgress(
            message='Image has not changed',
            total=UPDATE_CONTAINER_STEP_TOTAL,
            current=UPDATE_CONTAINER_STEP_TOTAL)
        return {'image_digest': digest}

    try:
        # NOTE: Only "image" passed currently, but this can be easily extended
        logging.info("Restarting container [%s].", service.name)
        service.update(image=digest)
        logging.info("Restart command has been sent to Container [%s].",
                     service.name)
    except Exception as e:
        logging.error("Unable to send restart command to container [%s]: %s",
                      service.id, e)

    updated = False
    expired = False
    timeout = datetime.now() + timedelta(minutes=3)
    while not (updated or expired or task.canceled):
        service = cli.services.get(containerInfo['name'])

        try:
            state = service.attrs['UpdateStatus']['State']
        except KeyError:
            state = ''

        if state == 'paused':
            raise RuntimeError(
                'Restarting the Tale failed with "{}"'.format(
                    service.attrs['UpdateStatus']['Message'])
            )

        updated = state == 'completed'
        expired = datetime.now() > timeout
        time.sleep(1.0)

    if task.canceled:
        raise RuntimeError('Tale restart cancelled')
    elif expired:
        raise RuntimeError('Tale update timed out')

    task.job_manager.updateProgress(
        message='Tale restarted with the new image',
        total=UPDATE_CONTAINER_STEP_TOTAL,
        current=UPDATE_CONTAINER_STEP_TOTAL)

    return {'image_digest': digest}


@girder_job(title='Shutdown Instance')
@app.task(bind=True)
def shutdown_container(self, instanceId):
    """Shutdown a running Tale."""
    user, instance = _get_user_and_instance(self.girder_client, instanceId)

    cli = docker.from_env(version='1.28')
    if 'containerInfo' not in instance:
        return
    containerInfo = instance['containerInfo']  # VALIDATE
    try:
        service = cli.services.get(containerInfo['name'])
    except docker.errors.NotFound:
        logging.info("Service not present [%s].",
                     containerInfo['name'])
        return

    try:
        logging.info("Releasing container [%s].", service.name)
        service.remove()
        logging.info("Container [%s] has been released.", service.name)
    except Exception as e:
        logging.error("Unable to release container [%s]: %s", service.id, e)


@girder_job(title='Remove Tale Data Volume')
@app.task(bind=True)
def remove_volume(self, instanceId):
    """Unmount WT-fs and remove mountpoint."""
    user, instance = _get_user_and_instance(self.girder_client, instanceId)

    if 'containerInfo' not in instance:
        return
    containerInfo = instance['containerInfo']  # VALIDATE

    cli = docker.from_env(version='1.28')
    # TODO: _remove_volumes()
    for suffix in MOUNTPOINTS:
        dest = os.path.join(containerInfo['mountPoint'], suffix)
        logging.info("Unmounting %s", dest)
        subprocess.call("umount %s" % dest, shell=True)

    logging.info("Unmounting licenses")
    subprocess.call("umount /licenses", shell=True)

    try:
        self.girder_client.delete('/dm/session/{sessionId}'.format(**instance))
    except Exception as e:
        logging.error("Unable to remove session. %s", e)
        pass

    try:
        volume = cli.volumes.get(containerInfo['volumeName'])
    except docker.errors.NotFound:
        logging.info("Volume not present [%s].", containerInfo['volumeName'])
        return
    try:
        logging.info("Removing volume: %s", volume.id)
        volume.remove()
    except Exception as e:
        logging.error("Unable to remove volume [%s]: %s", volume.id, e)
        pass


@girder_job(title='Build Tale Image')
@app.task(bind=True)
def build_tale_image(task, tale_id, force=False):
    """
    Build docker image from Tale workspace using repo2docker and push to Whole Tale registry.
    """
    logging.info('Building image for Tale %s', tale_id)

    task.job_manager.updateProgress(
        message='Building image', total=BUILD_TALE_IMAGE_STEP_TOTAL,
        current=1, forceFlush=True)

    tic = time.time()
    tale = task.girder_client.get('/tale/%s' % tale_id)
    while tale["status"] != TaleStatus.READY:
        time.sleep(2)
        tale = task.girder_client.get('/tale/{_id}'.format(**tale))
        if tale["status"] == TaleStatus.ERROR:
            raise ValueError("Cannot build image for a Tale in error state.")
        if time.time() - tic > 5 * 60.0:
            raise ValueError("Cannot build image. Tale preparing for more than 5 minutes.")

    last_build_time = -1
    try:
        last_build_time = tale['imageInfo']['last_build']
    except KeyError:
        pass

    logging.info('Last build time {}'.format(last_build_time))

    image_changed = tale["imageId"] != tale["imageInfo"].get("imageId")
    if image_changed:
        logging.info("Base image has changed. Forcing rebuild.")
        force = True

    # TODO: Move this check to the model?
    # Only rebuild if files have changed since last build or base image was changed
    if last_build_time > 0:
        workspace_folder = task.girder_client.get('/folder/{workspaceId}'.format(**tale))
        workspace_mtime = int(parse(workspace_folder['updated']).strftime('%s'))

        if not force and last_build_time > 0 and workspace_mtime < last_build_time:
            print('Workspace not modified since last build. Skipping.')
            task.job_manager.updateProgress(
                message='Workspace not modified, no need to build', total=BUILD_TALE_IMAGE_STEP_TOTAL,
                current=BUILD_TALE_IMAGE_STEP_TOTAL, forceFlush=True)

            return {
                'image_digest': tale['imageInfo']['digest'],
                'repo2docker_version': tale['imageInfo']['repo2docker_version'],
                'last_build': last_build_time
            }

    # Workspace modified so try to build.
    try:
        temp_dir = tempfile.mkdtemp(dir=HOSTDIR + '/tmp')
        logging.info('Copying workspace contents to %s (%s)', temp_dir, tale_id)
        workspace = task.girder_client.get('/folder/{workspaceId}'.format(**tale))
        task.girder_client.downloadFolderRecursive(workspace['_id'], temp_dir)

    except Exception as e:
        raise ValueError('Error accessing Girder: {}'.format(e))
    except KeyError:
        logging.info('KeyError')
        pass  # no workspace folderId
    except girder_client.HttpError:
        logging.warn("Workspace folder not found for tale: %s", tale_id)
        pass

    cli = docker.from_env(version='1.28')
    container_config = _get_container_config(task.girder_client, tale)
    # Ensure that we have proper version of r2d
    try:
        cli.images.pull(container_config.repo2docker_version)
    except docker.errors.NotFound:
        raise ValueError(
            f"Requested r2d image '{container_config.repo2docker_version}' not found."
        )
    cli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
              registry=DEPLOYMENT.registry_url)

    # Use the current time as the image build time and tag
    build_time = int(time.time())

    tag = '{}/{}/{}'.format(urlparse(DEPLOYMENT.registry_url).netloc,
                            tale_id, str(build_time))

    # Image is required for config information
    image = task.girder_client.get('/image/%s' % tale['imageId'])

    # Write the environment.json to the workspace
    with open(os.path.join(temp_dir, 'environment.json'), 'w') as fp:
        json.dump(image, fp)

    # Build the image from the workspace
    ret = _build_image(
        cli, tale_id, image, tag, temp_dir, container_config.repo2docker_version
    )

    # Remove the temporary directory whether the build succeeded or not
    shutil.rmtree(temp_dir, ignore_errors=True)

    if ret['StatusCode'] != 0:
        # repo2docker build failed
        raise ValueError('Error building tale {}'.format(tale_id))

    # If the repo2docker build succeeded, push the image to our registry
    apicli = docker.APIClient(base_url='unix://var/run/docker.sock')
    apicli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
                 registry=DEPLOYMENT.registry_url)

    # remove clone
    shutil.rmtree(temp_dir, ignore_errors=True)
    for line in apicli.push(tag, stream=True):
        print(line.decode('utf-8'))

    # TODO: if push succeeded, delete old image?

    # Get the built image digest
    image = cli.images.get(tag)
    digest = next((_ for _ in image.attrs['RepoDigests']
                   if _.startswith(urlparse(DEPLOYMENT.registry_url).netloc)), None)

    task.job_manager.updateProgress(
        message='Image build succeeded', total=BUILD_TALE_IMAGE_STEP_TOTAL,
        current=BUILD_TALE_IMAGE_STEP_TOTAL, forceFlush=True)

    logging.info('Successfully built image %s' % image.attrs['RepoDigests'][0])

    # Image digest used by updateBuildStatus handler
    return {
        'image_digest': digest,
        'repo2docker_version': container_config.repo2docker_version,
        'last_build': build_time
    }


@girder_job(title='Publish Tale')
@app.task(bind=True)
def publish(self,
            tale_id,
            token,
            version_id,
            repository=None,
            draft=False):
    """
    Publish a tale.

    :param tale_id: The tale id
    :param token: An access token for a given repository.
    :param version_id: The version of the Tale being published
    :param repository: Target repository.
    :param draft: If True, don't mint DOI.
    :type tale_id: str
    :type token: obj
    :type repository: str
    :type draft: boolean
    """

    provider_name = token["provider"].lower()
    if provider_name.startswith("dataone"):
        provider = DataONEPublishProvider(
            self.girder_client,
            tale_id,
            token,
            version_id,
            job_manager=self.job_manager,
            dataone_node=repository,
        )
    elif provider_name == "zenodo":
        provider = ZenodoPublishProvider(
            self.girder_client,
            tale_id,
            token,
            version_id,
            draft=draft,
            job_manager=self.job_manager
        )
    else:
        raise ValueError("Unsupported publisher ({})".format(token["provider"]))

    provider.publish()


@girder_job(title='Import Tale')
@app.task(bind=True)
def import_tale(self, lookup_kwargs, tale, spawn=True):
    """Create a Tale provided a url for an external data and an image Id.

    Currently, this task only handles importing raw data. In the future, it
    should also allow importing serialized Tales.
    """
    if spawn:
        total = 4
    else:
        total = 3

    if spawn:
        try:
            instance = self.girder_client.post(
                '/instance', parameters={'taleId': tale['_id']})
        except girder_client.HttpError as resp:
            try:
                message = json.loads(resp.responseText).get('message', '')
            except json.JSONDecodeError:
                message = str(resp)
            errormsg = 'Unable to create instance. Server returned {}: {}'
            errormsg = errormsg.format(resp.status, message)

    def set_tale_error_status():
        self.girder_client.put(
            "/tale/{_id}".format(**tale),
            json={
                "status": TaleStatus.ERROR,
                "imageId": str(tale["imageId"]),
                "public": tale["public"],
            }
        )

    self.job_manager.updateProgress(
        message='Gathering basic info about the dataset', total=total,
        current=1)
    dataId = lookup_kwargs.pop('dataId')
    try:
        parameters = dict(dataId=json.dumps(dataId))
        parameters.update(lookup_kwargs)
        dataMap = self.girder_client.get(
            '/repository/lookup', parameters=parameters)
    except girder_client.HttpError as resp:
        try:
            message = json.loads(resp.responseText).get('message', '')
        except json.JSONDecodeError:
            message = str(resp)
        errormsg = 'Unable to register \"{}\". Server returned {}: {}'
        errormsg = errormsg.format(dataId[0], resp.status, message)
        set_tale_error_status()
        raise ValueError(errormsg)

    if not dataMap:
        errormsg = 'Unable to register \"{}\". Source is not supported'
        errormsg = errormsg.format(dataId[0])
        set_tale_error_status()
        raise ValueError(errormsg)

    self.job_manager.updateProgress(
        message='Registering the dataset in Whole Tale', total=total,
        current=2)
    parameters = {'dataMap': json.dumps(dataMap)}
    try:
        parameters['base_url'] = lookup_kwargs.pop('base_url')
    except KeyError:
        pass
    self.girder_client.post(
        '/dataset/register', parameters=parameters)

    # Currently, we register resources in two different ways:
    #  1. DOIs (coming from Globus, Dataverse, DataONE, etc) create a root
    #     folder in the Catalog, that's named exactly the same as dataset.
    #  2. HTTP(S) files are registered into Catalog using a nested structure
    #     based on their url (see whole-tale/girder_wholetale#266)
    #  Knowing that, let's try to find the newly registered data by path.
    catalog_path = '/collection/WholeTale Catalog/WholeTale Catalog'
    if dataMap[0]['repository'].lower().startswith('http'):
        url = urlparse(dataMap[0]['dataId'])
        path = os.path.join(catalog_path, url.netloc, url.path[1:])
    else:
        path = os.path.join(catalog_path, dataMap[0]['name'])

    resource = self.girder_client.get(
        '/resource/lookup', parameters={'path': path})
    if not resource:
        errormsg = 'Registration of {} failed. Aborting!'.format(dataMap[0]['dataId'])
        set_tale_error_status()
        raise ValueError(errormsg)

    tale["dataSet"] = [
        {
            'mountPath': resource['name'],
            'itemId': resource['_id'],
            '_modelType': resource['_modelType']
        }
    ]
    tale = self.girder_client.put(
        '/tale/{_id}'.format(**tale),
        json={
            "dataSet": tale["dataSet"],
            "imageId": str(tale["imageId"]),
            "public": tale["public"],
            "status": TaleStatus.READY,
        }
    )

    if spawn:
        self.job_manager.updateProgress(
            message='Creating a Tale container', total=total, current=3)
        while instance['status'] == InstanceStatus.LAUNCHING:
            # TODO: Timeout? Raise error?
            time.sleep(1)
            instance = self.girder_client.get(
                '/instance/{_id}'.format(**instance))
    else:
        instance = None

    self.job_manager.updateProgress(
        message='Tale is ready!', total=total, current=total)
    # TODO: maybe filter results?
    return {'tale': tale, 'instance': instance}


@app.task
def rebuild_image_cache():
    logging.info("Rebuilding image cache")

    # Get the list of images
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    r = requests.get(GIRDER_API_URL + "/image", headers=headers)
    r.raise_for_status()
    images = r.json()

    cli = docker.from_env(version="1.28")
    version = REPO2DOCKER_VERSION.split(":")[1]

    # Build each base image
    for image in images:
        temp_dir = tempfile.mkdtemp(dir=HOSTDIR + "/tmp")
        tag = f"cache/{image['_id']}:{version}"

        logging.info(
            "Building %s %s in %s with %s", image["name"], tag, temp_dir, REPO2DOCKER_VERSION
        )

        with open(os.path.join(temp_dir, "environment.json"), "w") as fp:
            json.dump(image, fp)

        start = time.time()
        ret = _build_image(
            cli, image["_id"], image, tag, temp_dir, REPO2DOCKER_VERSION
        )

        elapsed = int(time.time() - start)
        if ret["StatusCode"] != 0:
            logging.error("Error building %s", image["name"])
        else:
            logging.info("Build time: %i seconds", elapsed)

        shutil.rmtree(temp_dir, ignore_errors=True)


def _mount_girderfs(mountpoint, directory, fs_type, obj_id, api_key, hostns=False):

    hostns_flag = '--hostns' if hostns else ''

    """Mount a girderfs directory given a type and id"""
    cmd = (
        f"girderfs {hostns_flag} -c {fs_type} --api-url {GIRDER_API_URL} --api-key {api_key}"
        f" {os.path.join(mountpoint, directory)} {obj_id}"
    )
    logging.info("Calling: %s", cmd)
    subprocess.check_call(cmd, shell=True)
    print(f"Mounted {fs_type} {directory}")


def _make_fuse_dirs(mountpoint, directories):
    """Create fuse directories"""

    # FUSE is silly and needs to have mirror inside container
    for suffix in directories:
        directory = os.path.join(mountpoint, suffix)
        _safe_mkdir(HOSTDIR + directory)
        if not os.path.isdir(directory):
            os.makedirs(directory)


def _create_docker_volume(cli, vol_name):
    """Creates a Docker volume with the specified name"""

    try:
        volume = cli.volumes.create(name=vol_name, driver='local')
    except DockerException as dex:
        logging.error(f"Error creating volume {vol_name} using local driver")
        logging.exception(dex)

    logging.info(f"Volume: {volume.name} created")
    mountpoint = volume.attrs['Mountpoint']
    logging.info(f"Mountpoint: {mountpoint}")

    os.chown(HOSTDIR + mountpoint, DEFAULT_USER, DEFAULT_GROUP)
    for root, dirs, files in os.walk(HOSTDIR + mountpoint):
        for obj in dirs + files:
            os.chown(os.path.join(root, obj), DEFAULT_USER, DEFAULT_GROUP)

    # Before calling girderfs and "escaping" container, we need to make
    # sure that shared objects we use are available on the host
    # TODO: this assumes overlayfs
    mounts = ''.join(open(HOSTDIR + '/proc/1/mounts').readlines())
    if 'overlay /usr/local' not in mounts:
        cont = cli.containers.get(socket.gethostname())
        libdir = cont.attrs['GraphDriver']['Data']['MergedDir']
        subprocess.call('mount --bind {}/usr/local /usr/local'.format(libdir),
                        shell=True)
    return mountpoint


def _get_session(gc, tale=None, version_id=None):
    """Returns the session for a tale or version"""
    session = {'_id': None}

    if tale is not None and tale.get('dataSet') is not None:
        session = gc.post(
            '/dm/session', parameters={'taleId': tale['_id']})
    elif version_id is not None:
        # Get the dataset for the version
        dataset = gc.get('/version/{}/dataSet'.format(version_id))
        if dataset is not None:
            session = gc.post(
                '/dm/session', parameters={'dataSet': json.dumps(dataset)})

    return session


def _write_env_json(workspace_dir, image):
    # TODO: I wanted to write to the .wholetale directory, but this would 
    # mean that the user's r2d config needs to be there too (e.g, apt.txt).
    # So for now, write to root of workspace.

    env_json = os.path.join(f"{HOSTDIR}{workspace_dir}", 'environment.json')
    # dot_dir = f"{HOSTDIR}{workspace_dir}/.wholetale/"
    # _safe_mkdir(dot_dir)
    # os.chown(dot_dir, DEFAULT_USER, DEFAULT_GROUP)
    # env_json = os.path.join(dot_dir, 'environment.json')

    print(f"Writing the environment to {env_json}")
    with open(env_json, 'w') as fp:
        json.dump(image, fp)
    return env_json


@girder_job(title='Recorded Run')
@app.task(bind=True)
def recorded_run(self, run_id, tale_id, entrypoint):
    """Start a recorded run for a tale version"""

    cli = docker.from_env(version='1.28')

    run = self.girder_client.get('/run/{}'.format(run_id))
    tale = self.girder_client.get('/tale/{}'.format(tale_id))
    user = self.girder_client.get('/user/me')

    def set_run_status(run, status):
        self.girder_client.patch(
            "/run/{_id}/status".format(**run), parameters={'status': status}
        )

    # UNKNOWN = 0 STARTING = 1 RUNNING = 2 COMPLETED = 3 FAILED = 4 CANCELLED = 5
    set_run_status(run, RunStatus.STARTING)

    self.job_manager.updateProgress(
        message='Preparing volumes', total=RECORDED_RUN_STEP_TOTAL,
        current=1, forceFlush=True)

    # Create Docker volume
    vol_name = "%s_%s_%s" % (run_id, user['login'], new_user(6))
    mountpoint = _create_docker_volume(cli, vol_name)

    # Create fuse directories
    _make_fuse_dirs(mountpoint, ['data', 'workspace'])

    # Mount data and workspace for the run
    api_key = _get_api_key(self.girder_client)
    session = _get_session(self.girder_client, version_id=run['runVersionId'])
    if session['_id'] is not None:
        _mount_girderfs(mountpoint, 'data', 'wt_dms', session['_id'], api_key, hostns=True)

    _mount_girderfs(mountpoint, 'workspace', 'wt_run', run_id, api_key)

    # Build the image for the run
    self.job_manager.updateProgress(
        message='Building image', total=RECORDED_RUN_STEP_TOTAL,
        current=2, forceFlush=True)

    # Setup image tag
    build_time = int(time.time())
    tag = '{}/{}/{}'.format(urlparse(DEPLOYMENT.registry_url).netloc,
                            run_id, str(build_time))

    work_dir = os.path.join(mountpoint, 'workspace')
    image = self.girder_client.get('/image/%s' % tale['imageId'])
    env_json = _write_env_json(work_dir, image)

    # TODO: What should we use here? Latest? What the tale was built with?
    repo2docker_version = REPO2DOCKER_VERSION
    print(f"Using repo2docker {repo2docker_version}")

    # Build currently assumes tmp directory, in this case mount the run workspace
    container_config = _get_container_config(self.girder_client, tale)
    work_target = os.path.join(container_config.target_mount, 'workspace')
    extra_volume = {
        work_dir: {
            'bind': work_target,
            'mode': 'rw'
        }
    }

    try:
        print("Building mage for recorded run " + tag)
        ret = _build_image(cli, tale_id, image, tag, work_target, repo2docker_version, extra_volume)
        if ret['StatusCode'] != 0:
            raise ValueError('Image build failed for recorded run {}'.format(run_id))
        # TODO: Do we push the image? Delete it at the end?

        self.job_manager.updateProgress(
            message='Recording run', total=RECORDED_RUN_STEP_TOTAL,
            current=3, forceFlush=True)

        set_run_status(run, RunStatus.RUNNING)
        _recorded_run(cli, mountpoint, container_config, tag, entrypoint)

        set_run_status(run, RunStatus.COMPLETED)
        self.job_manager.updateProgress(
            message='Finished recorded run', total=RECORDED_RUN_STEP_TOTAL,
            current=4, forceFlush=True)

    except Exception as e:
        logging.error("Recorded run failed. %s", e)
        logging.exception(e)
        set_run_status(run, RunStatus.FAILED)
    finally:
        # Remove the environment.json
        os.remove(env_json)

        # TODO: _cleanup_volumes
        for suffix in ['data', 'workspace']:
            dest = os.path.join(mountpoint, suffix)
            logging.info("Unmounting %s", dest)
            subprocess.call("umount %s" % dest, shell=True)

        # Delete the session
        try:
            self.girder_client.delete('/dm/session/{}'.format(session['_id']))
        except Exception as e:
            logging.error("Unable to remove session. %s", e)

        # Delete the Docker volume
        try:
            volume = cli.volumes.get(vol_name)
            try:
                logging.info("Removing volume: %s", volume.id)
                volume.remove()
            except Exception as e:
                logging.error("Unable to remove volume [%s]: %s", volume.id, e)
        except docker.errors.NotFound:
            logging.info("Volume not present [%s].", vol_name)
