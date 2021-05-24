#!/usr/bin/env python3
"""Utility to prime r2d image cache on worker nodes

Intended to be run in-container via cron 

docker exec celery_worker /gwvolman/build_base_images.py

"""
import docker
import json
import os
import requests
import shutil
import tempfile
import time

from gwvolman.utils import _build_image, HOSTDIR
from gwvolman.constants import GIRDER_API_URL, REPO2DOCKER_VERSION


# Get the list of images
headers = {"Content-Type": "application/json", "Accept": "application/json"}
r = requests.get(GIRDER_API_URL + "/image", headers=headers)
r.raise_for_status()
images = r.json()

cli = docker.from_env(version='1.28')

version = REPO2DOCKER_VERSION.split(":")[1]

# Build each image using gwvolman
for image in images:
    temp_dir = tempfile.mkdtemp(dir=HOSTDIR + '/tmp')

    with open(os.path.join(temp_dir, 'environment.json'), 'w') as fp:
        json.dump(image, fp)

    tag = 'cache/{}:{}'.format(image["_id"], version)

    print(f'Building {image["name"]} {tag} in {temp_dir} with {REPO2DOCKER_VERSION}')
    start = time.time()
    ret = _build_image(
        cli, image["_id"], image, tag, temp_dir, REPO2DOCKER_VERSION
    )
    elapsed = int(time.time() - start)
    print(f'Build time: {elapsed} seconds')


    shutil.rmtree(temp_dir, ignore_errors=True)
