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

import logging
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from girder_worker.utils import girder_job
from girder_worker.app import app

from .constants import DEPLOYMENT_TYPE
from .tasks_factory import TasksFactory

tasks = TasksFactory(DEPLOYMENT_TYPE).getTasksInstance()


@girder_job(title='Create Tale Data Volume')
@app.task(bind=True)
def create_volume(ctx, instanceId: str):
    """Create a mountpoint and compose WT-fs."""
    return tasks.create_volume(ctx, instanceId)


@girder_job(title='Spawn Instance')
@app.task(bind=True)
def launch_container(ctx, payload):
    """Launch a container using a Tale object."""
    return tasks.launch_container(ctx, payload)


@girder_job(title='Update Instance')
@app.task(bind=True)
def update_container(ctx, instanceId, **kwargs):
    return tasks.update_container(ctx, instanceId, kwargs)


@girder_job(title='Shutdown Instance')
@app.task(bind=True)
def shutdown_container(ctx, instanceId):
    """Shutdown a running Tale."""
    return tasks.shutdown_container(ctx, instanceId)


@girder_job(title='Remove Tale Data Volume')
@app.task(bind=True)
def remove_volume(ctx, instanceId):
    """Unmount WT-fs and remove mountpoint."""
    return tasks.remove_volume(ctx, instanceId)


@girder_job(title='Build Tale Image')
@app.task(bind=True)
def build_tale_image(ctx, tale_id):
    """
    Build docker image from Tale workspace using repo2docker
    and push to Whole Tale registry.
    """
    return tasks.build_tale_image(ctx, tale_id)


@girder_job(title='Publish Tale')
@app.task(bind=True)
def publish(ctx, tale, dataone_node, dataone_auth_token, user_id):
    """
    :param tale: The tale id
    :param dataone_node: The DataONE member node endpoint
    :param dataone_auth_token: The user's DataONE JWT
    :param user_id: The user's ID
    :type tale: str
    :type dataone_node: str
    :type dataone_auth_token: str
    :type user_id: str
    """
    return tasks.publish(ctx, tale, dataone_node, dataone_auth_token, user_id)


@girder_job(title='Import Tale')
@app.task(bind=True)
def import_tale(ctx, lookup_kwargs, tale_kwargs, spawn=True):
    """Create a Tale provided a url for an external data and an image Id.

    Currently, this task only handles importing raw data. In the future, it
    should also allow importing serialized Tales.
    """
    return tasks.import_tale(ctx, lookup_kwargs, tale_kwargs, spawn)
