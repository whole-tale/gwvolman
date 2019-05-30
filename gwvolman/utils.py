# -*- coding: utf-8 -*-
# Copyright (c) 2016, Data Exploration Lab
# Distributed under the terms of the Modified BSD License.

"""A set of helper routines for WT related tasks."""

import logging
import os
import random
import re
import string
import uuid
from collections import namedtuple

import docker

from .constants import MOUNTPOINTS

DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")
HOSTDIR = os.environ.get("HOSTDIR", "/host")
MAX_FILE_SIZE = os.environ.get("MAX_FILE_SIZE", 200)
DOMAIN = os.environ.get('DOMAIN', 'dev.wholetale.org')
REGISTRY_USER = os.environ.get('REGISTRY_USER', 'fido')
REGISTRY_PASS = os.environ.get('REGISTRY_PASS')
TRAEFIK_ENTRYPOINT = os.environ.get("TRAEFIK_ENTRYPOINT", "http")
MOUNTS = {}
RETRIES = 5
container_name_pattern = re.compile('tmp\.([^.]+)\.(.+)\Z')

PooledContainer = namedtuple('PooledContainer', ['id', 'path', 'host'])
ContainerConfig = namedtuple('ContainerConfig', [
    'image', 'command', 'mem_limit', 'cpu_shares',
    'container_port', 'container_user', 'target_mount',
    'url_path', 'environment'
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


def sample_with_replacement(a, size):
    """Get a random path."""
    return "".join([random.SystemRandom().choice(a) for x in range(size)])


def new_user(size):
    """Get a random path."""
    return sample_with_replacement(string.ascii_letters + string.digits, size)

def _new_container_name():
    return 'tmp-{}'.format(new_user(12).lower())


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


def get_env_with_csp(config, deployment):
    '''Ensure that environment in container config has CSP_HOSTS setting.

    This method handles 3 cases:
        * No 'environment' in config -> return ['CSP_HOSTS=...']
        * 'environment' in config, but no 'CSP_HOSTS=...' -> append
        * 'environment' in config and has 'CSP_HOSTS=...' -> replace

    '''
    csp = "CSP_HOSTS='self' {}".format(deployment.dashboard_url())
    try:
        env = config['environment']
        original_csp = next((_ for _ in env if _.startswith('CSP_HOSTS')), None)
        if original_csp:
            env[env.index(original_csp)] = csp  # replace
        else:
            env.append(csp)
    except KeyError as err:
        print('KeyError: %s' % err)
        env = [csp]
    return env

def _get_container_config(gc, tale, deployment):
    if tale is None:
        container_config = {}  # settings['container_config']
    else:
        image = gc.get('/image/%s' % tale['imageId'])
        tale_config = image['config'] or {}
        if tale['config']:
            tale_config.update(tale['config'])

        digest=tale['imageInfo']['digest']

        try:
            mem_limit = size_notation_to_bytes(tale_config.get('memLimit', '2g'))
        except (ValueError, TypeError):
            mem_limit = 2 * 1024 ** 3
        container_config = ContainerConfig(
            command=tale_config.get('command'),
            container_port=tale_config.get('port'),
            container_user=tale_config.get('user'),
            cpu_shares=tale_config.get('cpuShares'),
            environment=get_env_with_csp(tale_config, deployment),
            image=digest,
            mem_limit=mem_limit,
            target_mount=tale_config.get('targetMount'),
            url_path=tale_config.get('urlPath')
        )
    return container_config

def _render_config(container_config):
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
    return container_config._replace(command=rendered_command, url_path=rendered_url_path)

def _build_tale_workspace(girder_client, instanceId, mountpoint):
    user, instance = _get_user_and_instance(girder_client, instanceId)
    tale = girder_client.get('/tale/{taleId}'.format(**instance))

    _download_tale_folder(girder_client, tale, mountpoint)
    # That's really it. The rest is about creating mountpoints for various
    # mounts, which is done automatically by Kubernetes

def _download_tale_folder(girder_client, tale, dest):
    try:
        girder_client.downloadFolderRecursive(tale['narrativeId'], dest)
    except KeyError:
        pass  # no narrativeId
    except girder_client.HttpError:
        logging.warning("Narrative folder not found for tale: %s",
                     str(tale['_id']))
        pass
