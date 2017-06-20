from distutils.version import StrictVersion
import os
import time
import docker
import subprocess
from docker.errors import DockerException
import json
import logging
import girder_client
from girder_worker.app import app
# from girder_worker.plugins.docker.executor import _pull_image
from .utils import \
    HOSTDIR, API_VERSION, \
    parse_request_body, new_user, _safe_mkdir, _get_api_key, \
    get_container_config, _launch_container


@app.task
def create_volume(payload):
    api_check = payload.get('api_version', '1.0')
    if StrictVersion(api_check) != StrictVersion(API_VERSION):
        logging.error('Unsupported API (%s) (server API %s)' %
                      (api_check, API_VERSION))

    gc, user, tale = parse_request_body(payload)
    vol_name = "%s_%s_%s" % (tale['_id'], user['login'], new_user(6))
    cli = docker.from_env(version='auto')

    try:
        volume = cli.volumes.create(name=vol_name, driver='local')
    except DockerException as dex:
        logging.error('Error pulling Docker image blah')
        logging.exception(dex)
    logging.info("Volume: %s created", volume.name)
    mountpoint = volume.attrs['Mountpoint']
    logging.info("Mountpoint: %s", mountpoint)

    homeDir = gc.loadOrCreateFolder('Notebooks', user['_id'], 'user')
    items = [item['_id'] for item in gc.listItem(homeDir['_id'])
             if item["name"].endswith("pynb")]
    # TODO: should be done in one go with /resource endpoint
    #  but client doesn't have it yet
    for item in items:
        gc.downloadItem(item, HOSTDIR + mountpoint)

    # TODO: read uid/gid from env/config
    for item in os.listdir(HOSTDIR + mountpoint):
        if item == 'data':
            continue
        os.chown(os.path.join(HOSTDIR + mountpoint, item),
                 1000, 100)

    dest = os.path.join(mountpoint, "data")
    _safe_mkdir(HOSTDIR + dest)
    # FUSE is silly and needs to have mirror inside container
    if not os.path.isdir(dest):
        os.makedirs(dest)
    api_key = _get_api_key(gc)
    cmd = "girderfs -c remote --api-url {} --api-key {} {} {}".format(
        gc.urlBase, api_key, dest, tale['folderId'])
    logging.info("Calling: %s", cmd)
    subprocess.call(cmd, shell=True)
    return dict(
        nodeId=cli.info()['Swarm']['NodeID'],
        mountPoint=mountpoint,
        volumeName=volume.name
    )


@app.task
def launch_container(payload):
    api_check = payload.get('api_version', '1.0')
    if StrictVersion(api_check) != StrictVersion(API_VERSION):
        logging.error('Unsupported API (%s) (server API %s)' %
                      (api_check, API_VERSION))

    gc, user, tale = parse_request_body(payload)
    # _pull_image()
    container_config = get_container_config(gc, tale)  # FIXME
    service, urlPath = _launch_container(
        payload['volumeName'], payload['nodeId'],
        container_config=container_config)

    tic = time.time()
    timeout = 10.0

    # wait until task is started
    while time.time() - tic < timeout:
        try:
            started = service.tasks()[0]['Status']['State'] == 'running'
        except IndexError:
            started = False
        if started:
            break
        time.sleep(0.2)

    return dict(
        name=service.name,
        urlPath=urlPath
    )


@app.task
def shutdown_container(payload):
    gc, user, instance = parse_request_body(payload)

    cli = docker.from_env()
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
        raise


@app.task
def remove_volume(payload):
    gc, user, instance = parse_request_body(payload)
    containerInfo = instance['containerInfo']  # VALIDATE

    cli = docker.from_env()
    dest = os.path.join(containerInfo['mountPoint'], 'data')
    logging.info("Unmounting %s", dest)
    subprocess.call("umount %s" % dest, shell=True)

    # upload notebooks
    homeDir = gc.loadOrCreateFolder('Notebooks', user['_id'], 'user')
    try:
        gc.upload(HOSTDIR + containerInfo["mountPoint"] + '/*.ipynb',
                  homeDir['_id'], reuseExisting=True, blacklist=["data"])
    except girder_client.HttpError as err:
        logging.warn("Something went wrong with data upload: %s" %
                     err.responseText)
        pass  # upload failed, keep going

    volume = cli.volumes.get(containerInfo['volumeName'])
    try:
        logging.info("Removing volume: %s", volume.id)
        volume.remove()
    except Exception as e:
        logging.error("Unable to remove volume [%s]: %s", volume.id, e)
        pass


@app.task
def build_image(payload):
    logging.info('Got payload: %s' % json.dumps(payload))
