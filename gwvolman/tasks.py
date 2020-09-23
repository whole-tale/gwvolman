"""A set of WT related Girder tasks."""
from datetime import datetime, timedelta
import os
import shutil
import socket
import json
import time
import tempfile
import docker
import subprocess
from docker.errors import DockerException
import girder_client

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
    _build_image, DEPLOYMENT

from .lib.dataone.publish import DataONEPublishProvider
from .lib.zenodo import ZenodoPublishProvider

from .constants import GIRDER_API_URL, InstanceStatus, ENABLE_WORKSPACES, \
    DEFAULT_USER, DEFAULT_GROUP, MOUNTPOINTS, REPO2DOCKER_VERSION, TaleStatus

CREATE_VOLUME_STEP_TOTAL = 2
LAUNCH_CONTAINER_STEP_TOTAL = 2
UPDATE_CONTAINER_STEP_TOTAL = 2
BUILD_TALE_IMAGE_STEP_TOTAL = 2
IMPORT_TALE_STEP_TOTAL = 2


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

    try:
        volume = cli.volumes.create(name=vol_name, driver='local')
    except DockerException as dex:
        logging.error('Error pulling Docker image blah')
        logging.exception(dex)
    logging.info("Volume: %s created", volume.name)
    mountpoint = volume.attrs['Mountpoint']
    logging.info("Mountpoint: %s", mountpoint)

    try:
        self.girder_client.downloadFolderRecursive(
            tale['narrativeId'], HOSTDIR + mountpoint)
    except KeyError:
        pass  # no narrativeId
    except girder_client.HttpError:
        logging.warn("Narrative folder not found for tale: %s",
                     str(tale['_id']))
        pass

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

    homeDir = self.girder_client.loadOrCreateFolder(
        'Home', user['_id'], 'user')
    data_dir = os.path.join(mountpoint, 'data')
    _safe_mkdir(HOSTDIR + data_dir)
    home_dir = os.path.join(mountpoint, 'home')
    _safe_mkdir(HOSTDIR + home_dir)
    if ENABLE_WORKSPACES:
        work_dir = os.path.join(mountpoint, 'workspace')
        _safe_mkdir(HOSTDIR + work_dir)
        if not os.path.isdir(work_dir):
            os.makedirs(work_dir)

    # FUSE is silly and needs to have mirror inside container
    for directory in (data_dir, home_dir):
        if not os.path.isdir(directory):
            os.makedirs(directory)
    api_key = _get_api_key(self.girder_client)

    if tale.get('dataSet') is not None:
        session = self.girder_client.post(
            '/dm/session', parameters={'taleId': tale['_id']})
    elif tale.get('folderId'):  # old format, keep it for now
        data_set = [
            {'itemId': folder['_id'], 'mountPath': '/' + folder['name']}
            for folder in self.girder_client.listFolder(tale['folderId'])
        ]
        data_set += [
            {'itemId': item['_id'], 'mountPath': '/' + item['name']}
            for item in self.girder_client.listItem(tale['folderId'])
        ]
        session = self.girder_client.post(
            '/dm/session', parameters={'dataSet': json.dumps(data_set)})
    else:
        session = {'_id': None}

    if session['_id'] is not None:
        cmd = "girderfs --hostns -c wt_dms --api-url {} --api-key {} {} {}"
        cmd = cmd.format(GIRDER_API_URL, api_key, data_dir, session['_id'])
        logging.info("Calling: %s", cmd)
        subprocess.call(cmd, shell=True)
    #  webdav relies on mount.c module, don't use hostns for now
    cmd = 'girderfs -c wt_home --api-url '
    cmd += '{} --api-key {} {} {}'.format(
        GIRDER_API_URL, api_key, home_dir, homeDir['_id'])
    logging.info("Calling: %s", cmd)
    subprocess.call(cmd, shell=True)

    if ENABLE_WORKSPACES:
        cmd = 'girderfs -c wt_work --api-url '
        cmd += '{} --api-key {} {} {}'.format(
            GIRDER_API_URL, api_key, work_dir, tale['_id'])
        logging.info("Calling: %s", cmd)
        subprocess.call(cmd, shell=True)

    self.job_manager.updateProgress(
        message='Volume created', total=CREATE_VOLUME_STEP_TOTAL,
        current=CREATE_VOLUME_STEP_TOTAL, forceFlush=True)

    return dict(
        nodeId=cli.info()['Swarm']['NodeID'],
        mountPoint=mountpoint,
        volumeName=volume.name,
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

    if 'imageInfo' not in tale:

        # Wait for image to be built
        tic = time.time()
        timeout = 180.0

        while time.time() - tic < timeout:

            logging.info("Waiting for image build to complete.")

            tale = self.girder_client.get('/tale/{taleId}'.format(**instance))

            if 'imageInfo' in tale and 'digest' in tale['imageInfo']:
                break

            time.sleep(5)

    # _pull_image() #FIXME
    container_config = _get_container_config(self.girder_client, tale)
    service, attrs = _launch_container(
        payload['volumeName'], payload['nodeId'],
        container_config,
        tale_id=tale['_id'], instance_id=payload['instanceId'])

    tic = time.time()
    timeout = 30.0

    # wait until task is started
    while time.time() - tic < timeout:
        try:
            started = service.tasks()[0]['Status']['State'] == 'running'
        except IndexError:
            started = False
        if started:
            break
        time.sleep(0.2)

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

    if previous_image == digest:
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
    for suffix in MOUNTPOINTS:
        dest = os.path.join(containerInfo['mountPoint'], suffix)
        logging.info("Unmounting %s", dest)
        subprocess.call("umount %s" % dest, shell=True)

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

    # TODO: Move this check to the model?
    # Only rebuild if files have changed since last build
    if last_build_time > 0:

        workspace_mtime = -1
        try:
            workspace_mtime = tale['workspaceModified']
        except KeyError:
            pass

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
    ret = _build_image(cli, tale_id, image, tag, temp_dir, REPO2DOCKER_VERSION)

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
        'repo2docker_version': REPO2DOCKER_VERSION,
        'last_build': build_time
    }


@girder_job(title='Publish Tale')
@app.task(bind=True)
def publish(self,
            tale_id,
            token,
            repository=None,
            draft=False):
    """
    Publish a tale.

    :param tale_id: The tale id
    :param token: An access token for a given repository.
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
            draft=False,
            job_manager=self.job_manager,
            dataone_node=repository,
        )
    elif provider_name == "zenodo":
        provider = ZenodoPublishProvider(
            self.girder_client,
            tale_id,
            token,
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
