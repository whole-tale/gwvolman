"""A set of WT related Girder tasks."""
from distutils.version import StrictVersion
import os
import shutil
import time
import tarfile
import tempfile
import pathlib
import docker
import subprocess
from docker.errors import DockerException
import logging
try:
    from urllib import urlretrieve
    from urlparse import urlparse
except ImportError:
    from urllib.request import urlretrieve
    from urllib.parse import urlparse
from girder_worker.app import app
# from girder_worker.plugins.docker.executor import _pull_image
from .utils import \
    HOSTDIR, API_VERSION, REGISTRY_USER, REGISTRY_URL, REGISTRY_PASS, \
    _parse_request_body, new_user, _safe_mkdir, _get_api_key, \
    _get_container_config, _launch_container


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
    os.chown(HOSTDIR + mountpoint, 1000, 100)

    # TODO: this assumes overlayfs
    mounts = ''.join(open(HOSTDIR + '/proc/1/mounts').readlines())
    if 'overlay /usr/local' not in mounts:
        cont = cli.containers.get('celery_worker')
        libdir = cont.attrs['GraphDriver']['Data']['MergedDir']
        subprocess.call('mount --bind {}/usr/local /usr/local'.format(libdir),
                        shell=True)

    homeDir = gc.loadOrCreateFolder('Home', user['_id'], 'user')
    data_dir = os.path.join(mountpoint, 'data')
    _safe_mkdir(HOSTDIR + data_dir)
    home_dir = os.path.join(mountpoint, 'home')
    _safe_mkdir(HOSTDIR + home_dir)
    # FUSE is silly and needs to have mirror inside container
    for directory in (data_dir, home_dir):
        if not os.path.isdir(directory):
            os.makedirs(directory)
    api_key = _get_api_key(gc)
    cmd = "girderfs --hostns -c remote --api-url {} --api-key {} {} {}".format(
        gc.urlBase, api_key, data_dir, tale['folderId'])
    logging.info("Calling: %s", cmd)
    subprocess.call(cmd, shell=True)
    #  webdav relies on mount.c module, don't use hostns for now
    cmd = 'girderfs -c wt_home --api-url '
    cmd += '{} --api-key {} {} {}'.format(
        gc.urlBase, api_key, home_dir, homeDir['_id'])
    logging.info("Calling: %s", cmd)
    subprocess.call(cmd, shell=True)
    return dict(
        nodeId=cli.info()['Swarm']['NodeID'],
        mountPoint=mountpoint,
        volumeName=volume.name
    )


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
    """Shutdown a running Tale."""
    gc, user, instance = _parse_request_body(payload)

    cli = docker.from_env(version='1.28')
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
    """Unmount WT-fs and remove mountpoint."""
    gc, user, instance = _parse_request_body(payload)
    containerInfo = instance['containerInfo']  # VALIDATE

    cli = docker.from_env(version='1.28')
    for suffix in ('data', 'home'):
        dest = os.path.join(containerInfo['mountPoint'], suffix)
        logging.info("Unmounting %s", dest)
        subprocess.call("umount %s" % dest, shell=True)

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


@app.task
def build_image(imageId, imageTag, sourceUrl):
    """Build docker image from WT Image object and push to a registry."""
    def strip_components(members, strip=1):
        for tarinfo in members:
            path = pathlib.Path(tarinfo.path)
            if len(path.parts) > strip:
                tarinfo.path = str(pathlib.Path(*path.parts[strip:]))
                yield tarinfo

    cli = docker.from_env(version='1.28')
    cli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
              registry=REGISTRY_URL)

    temp_dir = tempfile.mkdtemp()
    local_tarball = os.path.join(temp_dir, '{}.tar.gz'.format(imageId))
    urlretrieve(sourceUrl, local_tarball)
    with tarfile.open(local_tarball) as tar:
        tar.extractall(members=strip_components(tar), path=temp_dir)

    tag = urlparse(REGISTRY_URL).netloc + '/' + imageId

    apicli = docker.APIClient(base_url='unix://var/run/docker.sock')
    apicli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
                 registry=REGISTRY_URL)
    for line in apicli.build(path=temp_dir, pull=True, tag=tag):
        print(line)
    shutil.rmtree(temp_dir, ignore_errors=True)
    for line in apicli.push(tag, stream=True):
        print(line)

    image = cli.images.get(tag)
    # Only image.attrs['Id'] is used in Girder right now
    return image.attrs
