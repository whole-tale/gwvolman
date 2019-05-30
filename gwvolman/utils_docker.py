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

from .utils import size_notation_to_bytes, ContainerConfig, \
    REGISTRY_USER, REGISTRY_PASS, MOUNTPOINTS, new_user, \
    DOMAIN, TRAEFIK_ENTRYPOINT


def get_env_with_csp(config, deployment):
    '''Ensure that environment in container config has CSP_HOSTS setting.

    This method handles 3 cases:
        * No 'environment' in config -> return ['CSP_HOSTS=...']
        * 'environment' in config, but no 'CSP_HOSTS=...' -> append
        * 'environment' in config and has 'CSP_HOSTS=...' -> replace

    '''
    csp = "CSP_HOSTS='self' {}".format(deployment.dashboard_url)
    try:
        env = config['environment']
        original_csp = next((_ for _ in env if _.startswith('CSP_HOSTS')), None)
        if original_csp:
            env[env.index(original_csp)] = csp  # replace
        else:
            env.append(csp)
    except KeyError:
        env = [csp]
    return env

def _launch_container(volumeName, nodeId, container_config, deployment):

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
    host = 'tmp-{}'.format(new_user(12).lower())

    # https://github.com/containous/traefik/issues/2582#issuecomment-354107053
    endpoint_spec = docker.types.EndpointSpec(mode="vip")

    service = cli.services.create(
        container_config.image,
        command=rendered_command,
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
        path=rendered_url_path)

    return service, {'url': url}
