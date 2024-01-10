"""A set of WT related Girder tasks."""
from datetime import datetime, timedelta
import os
import json
import time
import docker
from urllib.parse import urlparse
import girder_client

import logging
from girder_worker.utils import girder_job
from girder_worker.app import app
# from girder_worker.plugins.docker.executor import _pull_image
from .r2d import DockerImageBuilder, ImageBuilder
from .utils import \
    new_user, _safe_mkdir, _get_api_key, \
    _get_container_config, _launch_container, _get_user_and_instance, \
    _recorded_run, stop_container
from .fs_container import FSContainer

from .lib.zenodo import ZenodoPublishProvider

from .constants import GIRDER_API_URL, InstanceStatus, \
    TaleStatus, RunStatus, VOLUMES_ROOT

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

    self.job_manager.updateProgress(
        message='Creating volume', total=CREATE_VOLUME_STEP_TOTAL,
        current=1, forceFlush=True)

    vol_name = "%s_%s_%s" % (tale['_id'], user['login'], new_user(6))
    fs_sidecar = FSContainer.start_container(vol_name)
    payload = {
        "mounts": [
            {
                "type": "data",
                "protocol": "girderfs",
                "location": "data",
            },
            {
                "type": "home",
                "protocol": "bind",
                "location": "home",
            },
            {
                "type": "workspace",
                "protocol": "bind",
                "location": "workspace",
            },
            {
                "type": "versions",
                "protocol": "girderfs",
                "location": "versions",
            },
            {
                "type": "runs",
                "protocol": "girderfs",
                "location": "runs",
            },
        ],
        "taleId": tale["_id"],
        "userId": user["_id"],
        "girderApiUrl": GIRDER_API_URL,
        "girderApiKey": _get_api_key(self.girder_client),
        "root": vol_name,
    }
    FSContainer.mount(fs_sidecar, payload)
    self.job_manager.updateProgress(
        message='Volume created', total=CREATE_VOLUME_STEP_TOTAL,
        current=CREATE_VOLUME_STEP_TOTAL, forceFlush=True)
    print("WT Filesystem created successfully.")

    cli = docker.from_env()
    return dict(
        nodeId=cli.info()['Swarm']['NodeID'],
        fscontainerId=fs_sidecar.id,
        volumeName=vol_name,
        instanceId=instance_id,
        taleId=tale["_id"],
    )


@girder_job(title='Spawn Instance')
@app.task(bind=True)
def launch_container(self, service_info):
    """Launch a container using a Tale object."""
    user, instance = _get_user_and_instance(
        self.girder_client, service_info['instanceId'])
    tale = self.girder_client.get(f"/tale/{service_info['taleId']}")

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

    container_config = _get_container_config(self.girder_client, tale)
    service, attrs = _launch_container(service_info, container_config, self.girder_client)
    print(
        f"Started a container using volume: {service_info['volumeName']} "
        f"on node: {service_info['nodeId']}"
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

    service_info.update(attrs)
    service_info['name'] = service.name
    return service_info


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
    logging.info("Stopping FS container for instance %s", instanceId)
    user, instance = _get_user_and_instance(self.girder_client, instanceId)

    if 'containerInfo' not in instance:
        logging.warning("No containerInfo for instance %s", instanceId)
        return
    containerInfo = instance["containerInfo"]  # VALIDATE
    FSContainer.stop_container(containerInfo["fscontainerId"])
    logging.info("FS container %s stopped", containerInfo["fscontainerId"])


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
    image_builder = ImageBuilder(task.girder_client, tale=tale)
    image_builder.pull_r2d()

    tag = image_builder.get_tag(force=force)

    logging.info("Computed tag: %s (taleId:%s)", tag, tale_id)

    # Use the current time as the image build time and tag
    build_time = int(time.time())

    # Check if image already exists
    if not force and (image := image_builder.cached_image(tag)):
        print('Cached image exists for this Tale. Skipping build.')
        task.job_manager.updateProgress(
            message='Tale not modified, no need to build',
            total=BUILD_TALE_IMAGE_STEP_TOTAL,
            current=BUILD_TALE_IMAGE_STEP_TOTAL,
            forceFlush=True
        )
        return {
            'image_digest': f"{image['name']}:{image['tag']}@{image['digest']}",
            'repo2docker_version': image_builder.container_config.repo2docker_version,
            'last_build': last_build_time
        }

    print("Forcing build.")

    # Prepare build context
    ret, _ = image_builder.run_r2d(tag, task=task)
    if task.canceled:
        task.request.chain = None
        logging.info("Build canceled.")
        return

    if ret["StatusCode"] != 0:
        # repo2docker build failed
        print(ret)
        raise ValueError('Error building tale {}'.format(tale_id))

    # Push the image to the registry
    image_builder.push_image(tag)

    # Get the built image digest
    image = image_builder.cached_image(tag)

    task.job_manager.updateProgress(
        message='Image build succeeded', total=BUILD_TALE_IMAGE_STEP_TOTAL,
        current=BUILD_TALE_IMAGE_STEP_TOTAL, forceFlush=True)

    logging.info(f"Successfully built image {image['name']}:{image['tag']} ({image['digest']})")

    # Image digest used by updateBuildStatus handler
    return {
        'image_digest': image["digest"],
        'repo2docker_version': image_builder.container_config.repo2docker_version,
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
    if provider_name == "zenodo":
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

    if provider.published and provider.publication_info.get("versionId") == version_id:
        raise ValueError(f"This version of the Tale ({version_id}) has already been published.")
    provider.publish()
    return provider.publication_info


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


@app.task(bind=True)
def rebuild_image_cache(self):
    logging.info("Rebuilding image cache")

    # Get the list of images
    images = self.girder_client.get("/image")

    # Build each base image
    for image in images:
        image_builder = ImageBuilder(self.girder_client, imageId=image["_id"])
        tag = image_builder.get_tag()
        container_config = image_builder.container_config

        logging.info(
            "Building %s %s in %s with %s", image["name"], tag, image_builder.build_context,
            container_config.repo2docker_version
        )

        start = time.time()
        ret, _ = image_builder.run_r2d(tag)

        elapsed = int(time.time() - start)
        if ret["StatusCode"] != 0:
            logging.error("Error building %s", image["name"])
        else:
            logging.info("Build time: %i seconds", elapsed)


def _make_fuse_dirs(mountpoint, directories):
    """Create fuse directories"""
    for suffix in directories:
        _safe_mkdir(os.path.join(mountpoint, suffix))


def _write_env_json(workspace_dir, image):
    # TODO: I wanted to write to the .wholetale directory, but this would
    # mean that the user's r2d config needs to be there too (e.g, apt.txt).
    # So for now, write to root of workspace.

    env_json = os.path.join(workspace_dir, 'environment.json')
    # dot_dir = f"{workspace_dir}/.wholetale/"
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
    run = self.girder_client.get(f"/run/{run_id}")
    state = RecordedRunCleaner(run, self.girder_client)
    tale = self.girder_client.get(
        f"/tale/{tale_id}/restore", parameters={"versionId": run["runVersionId"]}
    )
    user = self.girder_client.get('/user/me')
    image_builder = DockerImageBuilder(self.girder_client, tale=tale)

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
    fs_sidecar = FSContainer.start_container(vol_name)
    payload = {
        "mounts": [
            {
                "type": "data",
                "protocol": "girderfs",
                "location": "data",
            },
            {
                "type": "run",
                "protocol": "bind",
                "location": "workspace",
            },
        ],
        "girderApiUrl": GIRDER_API_URL,
        "girderApiKey": _get_api_key(self.girder_client),
        "root": vol_name,
        "taleId": tale["_id"],
        "runId": run["_id"],
        "userId": user["_id"],
    }
    FSContainer.mount(fs_sidecar, payload)
    state.volume_created = vol_name

    # Build the image for the run
    self.job_manager.updateProgress(
        message='Building image', total=RECORDED_RUN_STEP_TOTAL,
        current=2, forceFlush=True)

    # Setup image tag
    tag = image_builder.get_tag()
    logging.info(
        "Computed tag: %s (taleId:%s, versionId:%s)", tag, tale_id, run["runVersionId"]
    )

    # Build currently assumes tmp directory, in this case mount the run workspace
    container_config = image_builder.container_config

    if self.canceled:
        state.cleanup()
        return

    try:
        if not image_builder.cached_image(tag):
            print("Building image for recorded run " + tag)
            ret, _ = image_builder.run_r2d(tag)
            if self.canceled:
                state.cleanup()
                return
            if ret['StatusCode'] != 0:
                raise ValueError('Image build failed for recorded run {}'.format(run_id))
            image_builder.push_image(tag)

        self.job_manager.updateProgress(
            message='Recording run', total=RECORDED_RUN_STEP_TOTAL,
            current=3, forceFlush=True)

        set_run_status(run, RunStatus.RUNNING)
        container_name = f"rrun-{new_user(8)}"

        self.girder_client.addMetadataToFolder(
            run["_id"],
            {
                "container_name": container_name,
                "volume_created": state.volume_created,
                "node_id": image_builder.dh.cli.info()["Swarm"]["NodeID"],
            }
        )

        mountpoint = os.path.join(VOLUMES_ROOT, "mountpoints", state.volume_created)

        _recorded_run(
            image_builder.dh.cli,
            mountpoint,
            container_config,
            tag,
            entrypoint,
            container_name,
            task=self
        )
        if self.canceled:
            state.cleanup()
            return

        set_run_status(run, RunStatus.COMPLETED)
        self.job_manager.updateProgress(
            message='Finished recorded run', total=RECORDED_RUN_STEP_TOTAL,
            current=4, forceFlush=True)
    except Exception as exc:
        logging.error(exc, exc_info=True)
        raise
    finally:
        state.cleanup(False)


@app.task()
def check_on_run(run_state):
    cli = docker.from_env(version='1.28')
    try:
        container = cli.containers.get(run_state["container_name"])
        return container.status == "running"
    except docker.errors.NotFound:
        return False


@girder_job(title='Clean failed Recorded Run')
@app.task(bind=True)
def cleanup_run(self, run_id):
    run = self.girder_client.get(f"/run/{run_id}")
    state = RecordedRunCleaner(run, self.girder_client)
    state.volume_created = run["meta"].get("volume_created")
    state.container_name = run["meta"].get("container_name")
    state.cleanup(canceled=False)
    state.set_run_status(RunStatus.FAILED)
    if (job_id := run["meta"]["jobId"]):
        self.girder_client.put(f"/job/{job_id}", parameters={"status": 4})


class RecordedRunCleaner:
    volume_created = None
    container_name = None

    def __init__(self, run, gc):
        self.gc = gc
        self.run = run
        self.docker_cli = docker.from_env(version='1.28')

    def set_run_status(self, status):
        self.gc.patch(
            "/run/{_id}/status".format(**self.run), parameters={'status': status}
        )

    def cleanup(self, canceled=True):
        if self.container_name:
            container = self.docker_cli.containers.get(self.container_name)
            stop_container(container)

        if self.volume_created:
            FSContainer.stop_container(self.volume_created)
            self.volume_created = None

        if canceled:
            self.set_run_status(RunStatus.CANCELED)
