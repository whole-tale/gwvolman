# -*- coding: utf-8 -*-
# Copyright (c) 2016, Data Exploration Lab
# Distributed under the terms of the Modified BSD License.

"""A set of helper routines for WT related tasks."""

from collections import namedtuple
import os
import random
import re
import string
import uuid
import logging
import docker

from .constants import LICENSE_PATH, MOUNTPOINTS, REPO2DOCKER_VERSION

DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")
HOSTDIR = os.environ.get("HOSTDIR", "/host")
MAX_FILE_SIZE = os.environ.get("MAX_FILE_SIZE", 200)
DOMAIN = os.environ.get('DOMAIN', 'dev.wholetale.org')
TRAEFIK_ENTRYPOINT = os.environ.get("TRAEFIK_ENTRYPOINT", "http")
REGISTRY_USER = os.environ.get('REGISTRY_USER', 'fido')
REGISTRY_PASS = os.environ.get('REGISTRY_PASS')
MOUNTS = {}
RETRIES = 5
container_name_pattern = re.compile('tmp\.([^.]+)\.(.+)\Z')

PooledContainer = namedtuple('PooledContainer', ['id', 'path', 'host'])
ContainerConfig = namedtuple('ContainerConfig', [
    'buildpack', 'repo2docker_version',
    'image', 'command', 'mem_limit', 'cpu_shares',
    'container_port', 'container_user', 'target_mount',
    'url_path', 'environment', 'csp'
])

SIZE_NOTATION_RE = re.compile("^(\d+)([kmg]?b?)$", re.IGNORECASE)
SIZE_TABLE = {
    '': 1, 'b': 1,
    'k': 1024, 'kb': 1024,
    'm': 1024 ** 2, 'mb': 1024 ** 2,
    'g': 1024 ** 3, 'gb': 1024 ** 3
}


def size_notation_to_bytes(size):
    if isinstance(size, int):
        return size
    match = SIZE_NOTATION_RE.match(size)
    if match:
        val, suffix = match.groups()
        return int(val) * SIZE_TABLE[suffix.lower()]
    raise ValueError


class Deployment(object):
    """Container for WT-specific docker stack deployment configuration.

    This class allows to read and store configuration of services in a WT
    deployment. It's meant to be used as a singleton across gwvolman.
    """

    _dashboard_url = None
    _girder_url = None
    _registry_url = None
    _traefik_network = None

    def __init__(self):
        self.docker_client = docker.from_env(version='1.28')

    @property
    def traefik_network(self):
        """str: Name of the overlay network used by traefik for ingress."""
        if self._traefik_network is None:
            try:
                service = self.docker_client.services.get('wt_dashboard')
                self._traefik_network = \
                    service.attrs['Spec']['Labels']['traefik.docker.network']
            except docker.errors.APIError:
                self._traefik_network = 'wt_traefik-net'  # Default...
        return self._traefik_network

    @property
    def dashboard_url(self):
        """str: Dashboard's public url."""
        if self._dashboard_url is None:
            self._dashboard_url = self.get_host_from_traefik_rule('wt_dashboard')
        return self._dashboard_url

    @property
    def girder_url(self):
        """str: Girder's public url."""
        if self._girder_url is None:
            self._girder_url = self.get_host_from_traefik_rule('wt_girder')
        return self._girder_url

    @property
    def registry_url(self):
        """str: Docker Registry's public url."""
        if self._registry_url is None:
            self._registry_url = self.get_host_from_traefik_rule('wt_registry')
        return self._registry_url

    def get_host_from_traefik_rule(self, service_name):
        """Infer service's hostname from traefik frontend rule label

        If services are unavailable (slave node), default to DOMAIN env settting
        """
        try:
            service = self.docker_client.services.get(service_name)
            rule = service.attrs['Spec']['Labels']['traefik.frontend.rule']
            return 'https://' + rule.split(':')[-1].split(',')[0].strip()
        except docker.errors.APIError:
            return '{}://{}.{}'.format(TRAEFIK_ENTRYPOINT, service_name[3:], DOMAIN)


DEPLOYMENT = Deployment()


def sample_with_replacement(a, size):
    """Get a random path."""
    return "".join([random.SystemRandom().choice(a) for x in range(size)])


def new_user(size):
    """Get a random path."""
    return sample_with_replacement(string.ascii_letters + string.digits, size)


def _safe_mkdir(dest):
    try:
        os.mkdir(dest)
    except OSError as e:
        if e.errno != 17:
            raise
        logging.warn("Failed to mkdir {}".format(dest))
        pass


def _get_api_key(gc):
    api_key = None
    for key in gc.get('/api_key'):
        if key['name'] == 'tmpnb' and key['active']:
            api_key = key['key']

    if api_key is None:
        api_key = gc.post('/api_key',
                          data={'name': 'tmpnb', 'active': True})['key']
    return api_key


def _get_user_and_instance(girder_client, instanceId):
    user = girder_client.get('/user/me')
    if user is None:
        logging.warn("Bad gider token")
        raise ValueError
    instance = girder_client.get('/instance/' + instanceId)
    return user, instance



def _get_container_config(gc, tale):
    if tale is None:
        container_config = {}  # settings['container_config']
    else:
        image = gc.get('/image/%s' % tale['imageId'])
        tale_config = image['config'] or {}
        if tale['config']:
            tale_config.update(tale['config'])

        image_info = tale.get("imageInfo", {})
        digest = image_info.get("digest")
        repo2docker_version = tale_config.get("repo2docker_version", REPO2DOCKER_VERSION)

        try:
            mem_limit = size_notation_to_bytes(tale_config.get('memLimit', '2g'))
        except (ValueError, TypeError):
            mem_limit = 2 * 1024 ** 3
        container_config = ContainerConfig(
            buildpack=tale_config.get("buildpack"),
            repo2docker_version=repo2docker_version,
            command=tale_config.get('command'),
            container_port=tale_config.get('port'),
            container_user=tale_config.get('user'),
            cpu_shares=tale_config.get('cpuShares'),
            environment=tale_config.get('environment'),
            image=digest,
            mem_limit=mem_limit,
            target_mount=tale_config.get('targetMount'),
            url_path=tale_config.get('urlPath'),
            csp=tale_config.get('csp')
        )
    return container_config


def _launch_container(volumeName, nodeId, container_config, tale_id='', instance_id=''):

    token = uuid.uuid4().hex
    # command
    if container_config.command:
        rendered_command = \
            container_config.command.format(
                base_path='', port=container_config.container_port,
                ip='0.0.0.0', token=token)
    else:
        rendered_command = None

    if container_config.url_path:
        rendered_url_path = \
            container_config.url_path.format(token=token)
    else:
        rendered_url_path = ''

    logging.info('config = ' + str(container_config))
    logging.info('command = ' + str(rendered_command))
    cli = docker.from_env(version='1.28')
    cli.login(username=REGISTRY_USER, password=REGISTRY_PASS,
              registry=DEPLOYMENT.registry_url)
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
    host = 'tmp-{}'.format(new_user(12).lower())

    # Add licences mount for STATA and Matlab support
    mounts.append(
            docker.types.Mount(type="bind", source=LICENSE_PATH, target="/licenses")
    )

    # https://github.com/containous/traefik/issues/2582#issuecomment-354107053
    endpoint_spec = docker.types.EndpointSpec(mode="vip")

    # Use the specified CSP for iframes or default to deployed host
    csp = ''
    if container_config.csp:
        csp = container_config.csp
    else:
        csp = "frame-ancestors ='self' {}".format(DEPLOYMENT.dashboard_url)

    service = cli.services.create(
        container_config.image,
        command=rendered_command,
        labels={
            'traefik.port': str(container_config.container_port),
            'traefik.enable': 'true',
            'traefik.frontend.rule': 'Host:{}.{}'.format(host, DOMAIN),
            'traefik.frontend.headers.contentSecurityPolicy': csp,
            'traefik.docker.network': DEPLOYMENT.traefik_network,
            'traefik.frontend.passHostHeader': 'true',
            'traefik.frontend.entryPoints': TRAEFIK_ENTRYPOINT,
            'wholetale.instanceId': instance_id,
            'wholetale.taleId': tale_id,
        },
        env=container_config.environment,
        mode=docker.types.ServiceMode('replicated', replicas=1),
        networks=[DEPLOYMENT.traefik_network],
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
        path=rendered_url_path)

    return service, {'url': url}


def _build_image(cli, tale_id, image, tag, temp_dir, repo2docker_version):
    """
    Run repo2docker on the workspace using a shared temp directory. Note that
    this uses the "local" provider.  Use the same default user-id and
    user-name as BinderHub
    """

    # Extra arguments for r2d
    extra_args = ''
    if image['config']['buildpack'] == "MatlabBuildPack":
        extra_args = ' --build-arg FILE_INSTALLATION_KEY={} '.format(
                os.environ.get("MATLAB_FILE_INSTALLATION_KEY"))

    r2d_cmd = ('jupyter-repo2docker '
               '--config="/wholetale/repo2docker_config.py" '
               '--target-repo-dir="/home/jovyan/work/workspace" '
               '--user-id=1000 --user-name={} '
               '--no-clean --no-run --debug {} '
               '--image-name {} {}'.format(
                                           image['config']['user'],
                                           extra_args,
                                           tag, temp_dir))

    logging.info('Calling %s (%s)', r2d_cmd, tale_id)

    container = cli.containers.run(
        image=repo2docker_version,
        command=r2d_cmd,
        environment=['DOCKER_HOST=unix:///var/run/docker.sock'],
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
        print(line.decode('utf-8').strip())

    # Since detach=True, then we need to explicitly check for the
    # container exit code
    return container.wait()
