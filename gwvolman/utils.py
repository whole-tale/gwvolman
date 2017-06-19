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
import requests
import girder_client


API_VERSION = '2.0'
GIRDER_API_URL = os.environ.get(
    "GIRDER_API_URL", "https://girder.wholetale.org/api/v1")
DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")
HOSTDIR = os.environ.get("HOSTDIR", "/host")
MAX_FILE_SIZE = os.environ.get("MAX_FILE_SIZE", 200)

MOUNTS = {}
RETRIES = 5
container_name_pattern = re.compile('tmp\.([^.]+)\.(.+)\Z')

PooledContainer = namedtuple('PooledContainer', ['id', 'path', 'host'])
ContainerConfig = namedtuple('ContainerConfig', [
    'image', 'command', 'mem_limit', 'cpu_shares', 'container_ip',
    'container_port', 'container_user', 'host_network', 'host_directories',
    'extra_hosts'
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
    user = gc.get("/user/me")
    if user is None:
        logging.warn("Bad gider token")
        raise ValueError
    if data.get('taleId'):
        obj = gc.get('/tale/%s' % data['taleId'])
    elif data.get('instanceId'):
        obj = gc.get('/instance/%s' % data['instanceId'])
    else:
        obj = None
    if not obj:
        obj = data
    # TODO: catch possible errors
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
            image=image['fullName'],
            mem_limit=tale_config.get('memLimit'),
            cpu_shares=tale_config.get('cpuShares'),
            container_ip=os.environ.get('DOCKER_GATEWAY', '172.17.0.1'),
            container_port=tale_config.get('port'),
            container_user=tale_config.get('user'),
            host_network=False,
            host_directories=None,
            extra_hosts=[]
        )
    return container_config


def _launch_container(volume, container_config=None):

    user = new_user(12)
    container_name = 'tmp-{}'.format(user)
    volume_bindings = {volume.attrs['Name']: {
        'bind': '/home/jovyan/work', 'mode': 'rw'}}   # FIXME

    # f not container_name_pattern.match(container_name):
    #   pattern = container_name_pattern.pattern
    #   raise Exception("[{}] does not match [{}]!".format(container_name,
    #                                                      pattern))

    # logging.info("Launching new server [%s] at path [%s].",
    #             container_name, path)
    if container_config is None:
        container_config = {}  # FIXME
    nb_token = uuid.uuid4().hex

    # docker create --name "subdomain"
    #  --label traefik.port=...
    #  --network traefik-net
    #  image

    # command
    rendered_command = \
        container_config.command.format(
            base_path='', port=container_config.container_port,
            ip='0.0.0.0', token=nb_token)

    cli = docker.from_env(version='auto')
    container = cli.containers.run(
        container_config.image,
        command=rendered_command,
        detach=True,
        labels={
            'traefik.port': str(container_config.container_port),
            'traefik.docker.network': 'traefik-net',
            'traefik.frontend.rule': "Host:{}.dev.wholetale.org".format(container_name)},
        mem_limit='2g',
        name=container_name,
        network='traefik-net',
        volumes=volume_bindings
    )

    # FIXME
    # create_result = yield self.spawner.create_instance(
    # container_id, host_ip, host_port = create_result

    # logging.info(
    #    "Created notebook server [%s] for path [%s] at [%s:%s]",
    #    container_name, path, host_ip, host_port)

    # Wait for the server to launch within the container before adding it
    # to the pool or serving it to a user.
    # _wait_for_server(host_ip, host_port, path) # FIXME

    # container = PooledContainer(
    #    id=container_id, path='%s/login?token=%s' % (path, nb_token),
    #    host=host_ip)
    return container


def _shutdown_container(container, alive=True):
    if alive:
        _with_retries(container.kill)
    _with_retries(container.remove)


def _with_retries(fn, *args, **kwargs):
    '''Attempt a Docker API call.

    If an error occurs, retry up to "max_tries" times before letting the
    exception propagate up the stack.  '''

    max_tries = kwargs.get('max_tries', RETRIES)
    try:
        if 'max_tries' in kwargs:
            del kwargs['max_tries']
        result = fn(*args, **kwargs)
        return result
    except (docker.errors.APIError,
            requests.exceptions.RequestException) as e:
        logging.error("Encountered a Docker error with"
                      "{} ({} retries remain): {}".format(
                          fn.__name__, max_tries, e))
        if max_tries > 0:
            kwargs['max_tries'] = max_tries - 1
            result = _with_retries(fn, *args, **kwargs)
            return result
        else:
            raise e
