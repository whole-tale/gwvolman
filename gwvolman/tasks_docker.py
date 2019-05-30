"""A set of WT related Girder tasks."""
import os
import shutil
import socket
import json
import time
import tempfile
import textwrap
import docker
import subprocess
from docker.errors import DockerException
import girder_client
from .tasks_base import TasksBase

import logging
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from girder_worker.utils import girder_job
from girder_worker.app import app
# from girder_worker.plugins.docker.executor import _pull_image

from .utils import \
    HOSTDIR, REGISTRY_USER, REGISTRY_PASS, DOMAIN, TRAEFIK_ENTRYPOINT,\
    new_user, _safe_mkdir, _get_api_key, \
    _get_container_config, _get_user_and_instance, _render_config,\
    _new_container_name, _download_tale_folder

from .constants import GIRDER_API_URL, InstanceStatus, ENABLE_WORKSPACES, \
    DEFAULT_USER, DEFAULT_GROUP, MOUNTPOINTS

from .deployment_docker import DockerDeployment

class DockerTasks(TasksBase):
    def __init__(self):
        TasksBase.__init__(self)
        self.deployment = DockerDeployment()

    def create_volume(self, ctx, instanceId: str):
        """Create a mountpoint and compose WT-fs."""
        user, instance = _get_user_and_instance(ctx.girder_client, instanceId)
        tale = ctx.girder_client.get('/tale/{taleId}'.format(**instance))

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

        _download_tale_folder(ctx.girder_client, tale, HOSTDIR + mountpoint)

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

        homeDir = ctx.girder_client.loadOrCreateFolder(
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
        api_key = _get_api_key(ctx.girder_client)

        session = self._create_session(ctx, tale)

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

        return dict(
            nodeId=cli.info()['Swarm']['NodeID'],
            mountPoint=mountpoint,
            volumeName=volume.name,
            sessionId=session['_id'],
            instanceId=instanceId,
        )


    def launch_container(self, ctx, payload):
        """Launch a container using a Tale object."""
        user, instance = _get_user_and_instance(
            ctx.girder_client, payload['instanceId'])
        tale = self.girder_client.get('/tale/{taleId}'.format(**instance))

        # _pull_image() #FIXME
        container_config = _get_container_config(ctx.girder_client, tale, self.deployment)
        _render_config(container_config)

        service, attrs = self._launch_container(payload['volumeName'], payload['nodeId'],
                                                container_config=container_config,
                                                deployment=self.deployment)

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

        payload.update(attrs)
        payload['name'] = service.name
        return payload

    def _launch_container(volumeName, nodeId, container_config, deployment):
        logging.info('config = ' + str(container_config))
        cli = docker.from_env(version='1.28')
        cli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
                  registry=deployment.registry_url)
        # Fails with: 'starting container failed: error setting
        #              label on mount source ...: read-only file system'
        # mounts = [
        #     docker.types.Mount(type='volume', source=volumeName, no_copy=True,
        #                        target=container_config.target_mount)
        # ]

        # FIXME: get mountPoint
        source_mount = '/var/lib/docker/volumes/{}/_data'.format(volumeName)
        mounts = []
        for path in MOUNTPOINTS:
            source = os.path.join(source_mount, path)
            target = os.path.join(container_config.target_mount, path)
            mounts.append(
                docker.types.Mount(type='bind', source=source, target=target)
            )
        host = _new_container_name()

        # https://github.com/containous/traefik/issues/2582#issuecomment-354107053
        endpoint_spec = docker.types.EndpointSpec(mode="vip")

        service = cli.services.create(
            container_config.image,
            command=container_config.command,
            labels={
                'traefik.port': str(container_config.container_port),
                'traefik.enable': 'true',
                'traefik.frontend.rule': 'Host:{}.{}'.format(host, DOMAIN),
                'traefik.docker.network': deployment.traefik_network,
                'traefik.frontend.passHostHeader': 'true',
                'traefik.frontend.entryPoints': TRAEFIK_ENTRYPOINT
            },
            env=container_config.environment,
            mode=docker.types.ServiceMode('replicated', replicas=1),
            networks=[deployment.traefik_network],
            name=host,
            mounts=mounts,
            endpoint_spec=endpoint_spec,
            constraints=['node.id == {}'.format(nodeId)],
            resources=docker.types.Resources(mem_limit=container_config.mem_limit)
        )

        # Wait for the server to launch within the container before adding it
        # to the pool or serving it to a user.
        # _wait_for_server(host_ip, host_port, path) # FIXME

        url = '{proto}://{host}.{domain}/{path}'.format(
            proto=TRAEFIK_ENTRYPOINT, host=host, domain=DOMAIN,
            path=container_config.url_path)

        return service, {'url': url}

    def update_container(self, ctx, instanceId, **kwargs):
        user, instance = _get_user_and_instance(ctx.girder_client, instanceId)

        cli = docker.from_env(version='1.28')
        if 'containerInfo' not in instance:
            return
        containerInfo = instance['containerInfo']  # VALIDATE
        try:
            service = cli.services.get(containerInfo['name'])
        except docker.errors.NotFound:
            logging.info("Service not present [%s].", containerInfo['name'])
            return

        # Assume containers launched from gwvolman come from its configured registry
        repoLoc = urlparse(self.deployment.registry_url).netloc
        digest = repoLoc + '/' + kwargs['image']

        try:
            # NOTE: Only "image" passed currently, but this can be easily extended
            logging.info("Restarting container [%s].", service.name)
            service.update(image=digest)
            logging.info("Restart command has been sent to Container [%s].", service.name)
        except Exception as e:
            logging.error("Unable to send restart command to container [%s]: %s", service.id, e)

        return {'image_digest': digest}

    def shutdown_container(self, ctx, instanceId):
        """Shutdown a running Tale."""
        user, instance = _get_user_and_instance(ctx.girder_client, instanceId)

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

    def remove_volume(self, ctx, instanceId):
        """Unmount WT-fs and remove mountpoint."""
        user, instance = _get_user_and_instance(ctx.girder_client, instanceId)

        if 'containerInfo' not in instance:
            return
        containerInfo = instance['containerInfo']  # VALIDATE

        cli = docker.from_env(version='1.28')
        for suffix in MOUNTPOINTS:
            dest = os.path.join(containerInfo['mountPoint'], suffix)
            logging.info("Unmounting %s", dest)
            subprocess.call("umount %s" % dest, shell=True)

        try:
            ctx.girder_client.delete('/dm/session/{sessionId}'.format(**instance))
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

    def _build_image(cli, ctx, tale_id, image, tag, temp_dir, repo2docker_version):
        """
        Run repo2docker on the workspace using a shared temp directory. Note that
        this uses the "local" provider.  Use the same default user-id and
        user-name as BinderHub
        """
        r2d_cmd = ('jupyter-repo2docker '
                   '--target-repo-dir="/home/jovyan/work/workspace" '
                   '--template={} --buildpack-name={} '
                   '--user-id=1000 --user-name={} '
                   '--no-clean --no-run --debug '
                   '--image-name {} {}'.format(
                                               image['config']['template'],
                                               image['config']['buildpack'],
                                               image['config']['user'],
                                               tag, temp_dir))

        logging.debug('Calling %s (%s)', r2d_cmd, tale_id)

        container = cli.containers.run(
            image=repo2docker_version,
            command=r2d_cmd,
            environment=['DOCKER_HOST=unix://var/run/docker.sock'],
            privileged=True,
            detach=True,
            remove=True,
            volumes={
                '/var/run/docker.sock': {
                    'bind': '/var/run/docker.sock', 'mode': 'rw'
                },
                '/tmp': {
                    'bind': '/host/tmp', 'mode': 'ro'
                }
            }
        )

        # Job output must come from stdout/stderr
        for line in container.logs(stream=True):
            print(line.decode('utf-8'))

        # Since detach=True, then we need to explicitly check for the
        # container exit code
        return container.wait()
