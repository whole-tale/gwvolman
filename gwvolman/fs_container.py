# Description: Context manager for changing the current working directory

import logging
import time
import docker
import requests
from requests.adapters import HTTPAdapter, Retry

from .constants import GIRDERFS_IMAGE, VOLUMES_ROOT
from .utils import stop_container


retries = Retry(
    connect=5,
    backoff_factor=0.5,
)


class FSContainer(object):
    @staticmethod
    def start_container(name):
        cli = docker.from_env()
        # Create container for handling FUSE mounts
        print("Creating WT Filesystem container...")
        fscontainer = cli.containers.run(
            image=GIRDERFS_IMAGE,
            name=name,
            detach=True,
            labels={"traefik.enable": "false"},
            mounts=[
                docker.types.Mount(
                    target=VOLUMES_ROOT,
                    source=VOLUMES_ROOT,
                    type="bind",
                    propagation="rshared",
                ),
            ],
            environment={
                "WT_VOLUMES_PATH": VOLUMES_ROOT,
            },
            devices=["/dev/fuse"],
            cap_add=["SYS_ADMIN"],
            security_opt=["apparmor:unconfined"],
            network="wt_celery",
            remove=True,
        )
        # wait for the container to be up and running
        # fail after 30s
        t = 0
        while True:
            time.sleep(1)
            try:
                fscontainer.reload()
            except docker.errors.NotFound:
                raise Exception("Failed to create WT Filesystem container")
            if fscontainer.status == "running":
                break
            if t > 30 or fscontainer.status == "exited":
                raise Exception("Failed to create WT Filesystem container")
            t += 1
        return fscontainer

    @staticmethod
    def mount(container, payload):
        # send payload to fscontainer using requests
        print("Sending payload to WT Filesystem container...")
        with requests.Session() as session:
            session.mount("http://", HTTPAdapter(max_retries=retries))
            response = session.post(
                f"http://{container.name}:8888/",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

    @staticmethod
    def stop_container(name):
        print("Sending shutdown request to WT Filesystem container...")
        cli = docker.from_env()
        try:
            container = cli.containers.get(name)
        except docker.errors.NotFound:
            return
        resp = requests.delete(f"http://{container.name}:8888/")
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            # log error from resp with traceback and continue
            logging.warning(
                f"Failed to shutdown WT Filesystem container {name} "
                f"with status code {resp.status_code}"
            )
            logging.warning(resp.text)
            pass

        stop_container(container)
