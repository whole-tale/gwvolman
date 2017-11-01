# -*- coding: utf-8 -*-
# Copyright (c) 2016, Data Exploration Lab
# Distributed under the terms of the Modified BSD License.

from collections import namedtuple
import logging
import os
import random
import re
import string
import uuid

import docker
import girder_client


API_VERSION = '2.0'
GIRDER_API_URL = os.environ.get(
    "GIRDER_API_URL", "https://girder.wholetale.org/api/v1")
DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")
HOSTDIR = os.environ.get("HOSTDIR", "/host")
MAX_FILE_SIZE = os.environ.get("MAX_FILE_SIZE", 200)
TRAEFIK_NETWORK = os.environ.get("TRAEFIK_NETWORK", "traefik-net")

MOUNTS = {}
RETRIES = 5
container_name_pattern = re.compile('tmp\.([^.]+)\.(.+)\Z')

PooledContainer = namedtuple('PooledContainer', ['id', 'path', 'host'])
ContainerConfig = namedtuple('ContainerConfig', [
    'image', 'command', 'mem_limit', 'cpu_shares',
    'container_port', 'container_user', 'target_mount',
    'url_path'
])


def sample_with_replacement(a, size):
    '''Get a random path. If Python had sampling with replacement built in,
    I would use that. The other alternative is numpy.random.choice, but
    numpy is overkill for this tiny bit of random pathing.'''
    return "".join([random.SystemRandom().choice(a) for x in range(size)])


def new_user(size):
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


def parse_request_body(data):
    gc = girder_client.GirderClient(apiUrl=data.get('apiUrl', GIRDER_API_URL))
    gc.token = data['girder_token']
    user = gc.get('/user/me')
    if user is None:
        logging.warn("Bad gider token")
        raise ValueError

    if data.get('taleId'):
        path = '/tale/%s' % data['taleId']
    elif data.get('instanceId'):
        path = '/instance/%s' % data['instanceId']
    else:
        return gc, user, data

    try:
        obj = gc.get(path)
    except girder_client.HttpError as e:
        raise ValueError
    return gc, user, obj


def get_container_config(gc, tale):
    if tale is None:
        container_config = {}  # settings['container_config']
    else:
        image = gc.get('/image/%s' % tale['imageId'])
        tale_config = image['config'] or {}
        if tale['config']:
            tale_config.update(tale['config'])
        container_config = ContainerConfig(
            command=tale_config.get('command'),
            container_port=tale_config.get('port'),
            container_user=tale_config.get('user'),
            cpu_shares=tale_config.get('cpuShares'),
            image=image['fullName'],
            mem_limit=tale_config.get('memLimit'),
            target_mount=tale_config.get('targetMount'),
            url_path=tale_config.get('urlPath')
        )
    return container_config


def _launch_container(volumeName, nodeId, container_config):

    token = uuid.uuid4().hex
    # command
    rendered_command = \
        container_config.command.format(
            base_path='', port=container_config.container_port,
            ip='0.0.0.0', token=token)

    rendered_url_path = \
        container_config.url_path.format(token=token)

    logging.debug('config = ' + str(container_config))
    logging.debug('command = ' + rendered_command)
    cli = docker.from_env(version='1.28')
    # Fails with: 'starting container failed: error setting
    #              label on mount source ...: read-only file system'
    # mounts = [
    #     docker.types.Mount(type='volume', source=volumeName, no_copy=True,
    #                        target=container_config.target_mount)
    # ]

    # FIXME: get mountPoint
    source_mount = '/var/lib/docker/volumes/{}/_data'.format(volumeName)
    mounts = [
        docker.types.Mount(type='bind', source=source_mount,
                           target=container_config.target_mount)
    ]
    service = cli.services.create(
        container_config.image,
        command=rendered_command,
        labels={
            'traefik.port': str(container_config.container_port),
        },
        mode=docker.types.ServiceMode('replicated', replicas=1),
        networks=[TRAEFIK_NETWORK], 
        name='tmp-{}'.format(new_user(12)),
        mounts=mounts,
        constraints=['node.id == {}'.format(nodeId)]
    )

    # resources=docker.types.Resources(mem_limit='2g'),
    # FIXME for some reason causes 500

    # Wait for the server to launch within the container before adding it
    # to the pool or serving it to a user.
    # _wait_for_server(host_ip, host_port, path) # FIXME

    return service, rendered_url_path
