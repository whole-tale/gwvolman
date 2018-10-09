"""A set of WT related Girder tasks."""
from distutils.version import StrictVersion
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
    HOSTDIR, REGISTRY_USER, REGISTRY_URL, REGISTRY_PASS, \
    _parse_request_body, new_user, _safe_mkdir, _get_api_key, \
    _get_container_config, _launch_container
from .publish import publish_tale
from .constants import API_VERSION, GIRDER_API_URL

DEFAULT_USER = 1000
DEFAULT_GROUP = 100


@girder_job(title='Create Tale Data Volume')
@app.task
def create_volume(payload):
    """Create a mountpoint and compose WT-fs."""
    api_check = payload.get('api_version', '1.0')
    if StrictVersion(api_check) != StrictVersion(API_VERSION):
        logging.error('Unsupported API (%s) (server API %s)' %
                      (api_check, API_VERSION))

    gc, user, tale = _parse_request_body(payload)
    vol_name = "%s_%s_%s" % (tale['_id'], user['login'], new_user(6))
    cli = docker.from_env(version='1.28')

    try:
        volume = cli.volumes.create(name=vol_name, driver='local')
    except DockerException as dex:
        logging.error('Error pulling Docker image blah')
        logging.exception(dex)
    logging.info("Volume: %s created", volume.name)
    mountpoint = volume.attrs['Mountpoint']
    logging.info("Mountpoint: %s", mountpoint)

    try:
        gc.downloadFolderRecursive(tale['narrativeId'], HOSTDIR + mountpoint)
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

    homeDir = gc.loadOrCreateFolder('Home', user['_id'], 'user')
    data_dir = os.path.join(mountpoint, 'data')
    _safe_mkdir(HOSTDIR + data_dir)
    home_dir = os.path.join(mountpoint, 'home')
    _safe_mkdir(HOSTDIR + home_dir)
    work_dir = os.path.join(mountpoint, 'workspace')
    _safe_mkdir(HOSTDIR + work_dir)
    # FUSE is silly and needs to have mirror inside container
    for directory in (data_dir, home_dir):
        if not os.path.isdir(directory):
            os.makedirs(directory)
    api_key = _get_api_key(gc)

    if tale.get('folderId'):
        data_set = [
            {'itemId': folder['_id'], 'mountPath': '/' + folder['name']}
            for folder in gc.listFolder(tale['folderId'])
        ]
        data_set += [
            {'itemId': item['_id'], 'mountPath': '/' + item['name']}
            for item in gc.listItem(tale['folderId'])
        ]
        session = gc.post('/dm/session', parameters={'dataSet': json.dumps(data_set)})

        cmd = "girderfs --hostns -c wt_dms --api-url {} --api-key {} {} {}".format(
            GIRDER_API_URL, api_key, data_dir, session['_id'])
        logging.info("Calling: %s", cmd)
        subprocess.call(cmd, shell=True)
    else:
        session = {'_id': None}
    #  webdav relies on mount.c module, don't use hostns for now
    cmd = 'girderfs -c wt_home --api-url '
    cmd += '{} --api-key {} {} {}'.format(
        GIRDER_API_URL, api_key, home_dir, homeDir['_id'])
    logging.info("Calling: %s", cmd)
    subprocess.call(cmd, shell=True)

    cmd = 'girderfs -c wt_work --api-url '
    cmd += '{} --api-key {} {} {}'.format(
        GIRDER_API_URL, api_key, work_dir, tale['_id'])
    logging.info("Calling: %s", cmd)
    subprocess.call(cmd, shell=True)

    payload.update(
        dict(
            nodeId=cli.info()['Swarm']['NodeID'],
            mountPoint=mountpoint,
            volumeName=volume.name,
            sessionId=session['_id']
        )
    )
    return payload


@girder_job(title='Spawn Instance')
@app.task
def launch_container(payload):
    """Launch a container using a Tale object."""
    api_check = payload.get('api_version', '1.0')
    if StrictVersion(api_check) != StrictVersion(API_VERSION):
        logging.error('Unsupported API (%s) (server API %s)' %
                      (api_check, API_VERSION))

    gc, user, tale = _parse_request_body(payload)
    # _pull_image()
    container_config = _get_container_config(gc, tale)  # FIXME
    service, attrs = _launch_container(
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

    payload.update(attrs)
    payload['name'] = service.name
    return payload


@girder_job(title='Shutdown Instance')
@app.task
def shutdown_container(payload):
    """Shutdown a running Tale."""
    gc, user, instance = _parse_request_body(payload)

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
@app.task
def remove_volume(payload):
    """Unmount WT-fs and remove mountpoint."""
    gc, user, instance = _parse_request_body(payload)
    if 'containerInfo' not in instance:
        return
    containerInfo = instance['containerInfo']  # VALIDATE

    cli = docker.from_env(version='1.28')
    for suffix in ('data', 'home', 'workspace'):
        dest = os.path.join(containerInfo['mountPoint'], suffix)
        logging.info("Unmounting %s", dest)
        subprocess.call("umount %s" % dest, shell=True)

    try:
        gc.delete('/dm/session/{sessionId}'.format(**payload))
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


@girder_job(title='Build WT Image')
@app.task
def build_image(image_id, repo_url, commit_id):
    """Build docker image from WT Image object and push to a registry."""
    temp_dir = tempfile.mkdtemp()
    # Clone repository and set HEAD to chosen commitId
    cmd = 'git clone --recursive {} {}'.format(repo_url, temp_dir)
    subprocess.call(cmd, shell=True)
    subprocess.call('git checkout ' + commit_id, shell=True, cwd=temp_dir)

    apicli = docker.APIClient(base_url='unix://var/run/docker.sock')
    apicli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
                 registry=REGISTRY_URL)
    tag = urlparse(REGISTRY_URL).netloc + '/' + image_id
    for line in apicli.build(path=temp_dir, pull=True, tag=tag):
        print(line)

    # TODO: create tarball
    # remove clone
    shutil.rmtree(temp_dir, ignore_errors=True)
    for line in apicli.push(tag, stream=True):
        print(line)

    cli = docker.from_env(version='1.28')
    cli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
              registry=REGISTRY_URL)
    image = cli.images.get(tag)
    # Only image.attrs['Id'] is used in Girder right now
    return image.attrs


@girder_job(title='Publish Tale')
@app.task
def publish(item_ids,
            tale,
            dataone_node,
            dataone_auth_token,
            girder_token,
            userId,
            prov_info,
            license_id):
    """
    Publishes a Tale to DataONE

    :param item_ids: A list of item ids that are in the package
    :param tale: The tale id
    :param dataone_node: The DataONE member node endpoint
    :param dataone_auth_token: The user's DataONE JWT
    :param girder_token: The user's girder token
    :param userId: The user's ID
    :param prov_info: Additional information included in the tale yaml
    :param license_id: The spdx of the license used
    :type item_ids: list
    :type tale: str
    :type dataone_node: str
    :type dataone_auth_token: str
    :type girder_token: str
    :type userId: str
    :type prov_info: dict
    :type license_id: str
    """

    res = publish_tale(item_ids,
                       tale,
                       dataone_node,
                       dataone_auth_token,
                       girder_token,
                       userId,
                       prov_info,
                       license_id)
    return res
