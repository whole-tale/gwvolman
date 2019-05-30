#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import socket

API_VERSION = '2.1'
DEFAULT_USER = 1000
DEFAULT_GROUP = 100
ENABLE_WORKSPACES = True
MOUNTPOINTS = ['data', 'home']
if ENABLE_WORKSPACES:
    MOUNTPOINTS.append('workspace')

try:
    DEFAULT_GIRDER_API_URL = 'http://' + socket.gethostbyname('girder') + ':8080/api/v1'
except socket.gaierror:
    DEFAULT_GIRDER_API_URL = 'https://girder.dev.wholetale.org/api/v1'
GIRDER_API_URL = os.environ.get('GIRDER_API_URL', DEFAULT_GIRDER_API_URL)


class InstanceStatus(object):
    LAUNCHING = 0
    RUNNING = 1
    ERROR = 2

DEPLOYMENT_TYPE = os.environ.get('DEPLOYMENT_TYPE', 'docker')