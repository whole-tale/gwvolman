#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import socket

API_VERSION = "2.1"
DEFAULT_USER = 1000
DEFAULT_GROUP = 100
ENABLE_WORKSPACES = True
MOUNTPOINTS = ["data"]
if ENABLE_WORKSPACES:
    # MOUNTPOINTS.append("workspace")
    MOUNTPOINTS.append("versions")
    MOUNTPOINTS.append("runs")


try:
    DEFAULT_GIRDER_API_URL = "http://" + socket.gethostbyname("girder") + ":8080/api/v1"
except socket.gaierror:
    DEFAULT_GIRDER_API_URL = "https://girder.dev.wholetale.org/api/v1"
GIRDER_API_URL = os.environ.get("GIRDER_API_URL", DEFAULT_GIRDER_API_URL)
LICENSE_PATH = os.environ.get("WT_LICENSE_PATH", "/licenses/")

REPO2DOCKER_VERSION = os.environ.get(
    "REPO2DOCKER_VERSION", "wholetale/repo2docker_wholetale:latest"
)
GIRDERFS_IMAGE = os.environ.get("GIRDERFS_IMAGE", "wholetale/girderfs:latest")
CPR_VERSION = os.environ.get("CPR_VERSION", "wholetale/wt-cpr:latest")
VOLUMES_ROOT = os.environ.get("WT_VOLUMES_PATH", "/mnt/homes")
NFS_PATH = os.environ.get("NFS_PATH", "/srv/vc_crypt/IMQCAM/girder")
NFS_SERVER = os.environ.get("NFS_SERVER")
NAMESPACE = os.environ.get("NAMESPACE", "wt")

RUN_WT_BUTTON_IMG = (
    "https://img.shields.io/badge/WholeTale-Run!-579ACA.svg?"
    "logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABQAAAAUCAYAAACNiR0NAAAABHNCSVQICAgIfAhki"
    "AAAAAlwSFlzAAABDgAAAQ4B6Vk72QAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOGSURBVDiNj"
    "ZVdTJtVGMd/py+0IC0DNuhcwQKTC8OHY6WTZHGOxJiYmKGYLSQm6Mxu5jREix+7WGJ0xs+LSsbijdlQExSEscwroyNhY"
    "7ghxi1AFDdaRusG0zG2VunH+z7elDpsS3wuz3me3/n/n5zzHMUaISImoBLYmFi6BswopSRTTVYGUBnw+o2wb0/w9mRxK"
    "HITAulCEd+zYKI9ALvK6UC/61VaWCv+BbH3z3r7zbPhy6nVWG33s/DFc9Fygu2vqGU8mYE6qIfPevr3n8h0Ee2yYKzsJ"
    "51OeUYohHXbzCzOEY4upgsfahsN9vL245qSjuQYllEXh72Hdt/IdDHAyWPcTW0i09+1gneiQJgM2s8WtFCk3Oci793Ez"
    "einJ/rBdQLIjKtlPo4qVBEHL7F8Sv9E4csWx1tdP3kZvrP5bR2rWaNt3f8xcT1t4gbUUCxu/adiLNgS5VSas6UyDt4xn"
    "/cUrm+kU8vblsFe8Rpo6OxhI7GEnbcZyMU1Tk0fA8P3rt3xRtnfMcsQAeASURMC+GZPQuhKyjTk1xa+DsJ69i2gWuXzr"
    "HvcBf7DncxP3EOj3sDoajO6dl6rOb1AFwP/cYfYX+riCgTUBFcmirOtxQzfLUwCdvptHHy29MM/DBFXDeI6wb9o1N88/"
    "0QO502vvOHqShyJ/ODt6dKgHITsEmXGIW5pcwuRZMJDRtzOfXjryk9HDz/C9sduYSiBtmaHQCT0ogZEQCHCZCbk3k444"
    "+Tk/XvLdJMKaxkxBPvRBEnNzufLTl7WZyyAogSkc09PT2XOzs7eaL5KYo216DlWsmLh7m1dIuYxcZnY3MAtLnLyI7coW"
    "BdActZedjis8xMz3HyxCna29tpbW2tzAJm6uvr52OxmP3E171Ab4qiZ559HhHhi4/ezKja5XItAP4spZSISF91dfWLk5"
    "OTaZO/+vx4Zv9AbW0tVVVVXyqlZKVT73k8nmWz2Zy2wDAMDMNIu2c2m/F4PBHgQwATgFIqWFdXd9Dr9WK329dUc3fY7X"
    "a8Xi81NTWvrkyeVcNBRI7oun5gaGiIgYEBxsbGEFk9+pRSuN1uWlpaaGpqQtO0I0qplzKeKiLtIhIWERkZGZGGhgZxuV"
    "zicrnE7XbL6OioJCIsIimglHmYgDqA14CnBwcHHYFAAKUUpaWlNDc3B4F+4AOlVPB/Ae8CK6AC2JRYCgL+tb6AfwC+yH"
    "oMI9RIjwAAAABJRU5ErkJggg=="
)


# NOTE: changing order of ENV_FILES will result in cache invalidation.
# You have been warned.
R2D_FILENAMES = (
    "environment.yml",
    "Pipfile",
    "Pipfile.lock",
    "requirements.txt",
    "setup.py",
    "Project.toml",
    "REQUIRE",
    "install.R",
    "apt.txt",
    "DESCRIPTION",
    "postBuild",
    "start",
    "runtime.txt",
    "default.nix",
    "install.do",
    "JuliaProject.toml",
    "requirements3.txt",
    "toolboxes.txt",
    "Dockerfile",
)


class InstanceStatus(object):
    LAUNCHING = 0
    RUNNING = 1
    ERROR = 2


class TaleStatus(object):
    PREPARING = 0
    READY = 1
    ERROR = 2


class RunStatus(object):
    UNKNOWN = 0
    STARTING = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    CANCELED = 5


CREATE_VOLUME_STEP_TOTAL = 2
LAUNCH_CONTAINER_STEP_TOTAL = 2
UPDATE_CONTAINER_STEP_TOTAL = 2
IMPORT_TALE_STEP_TOTAL = 2
RECORDED_RUN_STEP_TOTAL = 4
BUILD_TALE_IMAGE_STEP_TOTAL = 2
