import hashlib
import logging
import os
import tempfile

import docker

from ..utils import (
    DEPLOYMENT,
    DummyTask,
    stop_container,
)
from .builder import ImageBuilderBase


class DockerHelper:
    def __init__(
        self, registry_user=None, registry_password=None, registry_url=None, auth=True
    ):
        username = registry_user or os.environ.get("REGISTRY_USER", "fido")
        password = registry_password or os.environ.get("REGISTRY_PASS")
        registry_url = registry_url or DEPLOYMENT.registry_url
        self.cli = docker.from_env(version="1.28")
        self.apicli = docker.APIClient(base_url="unix://var/run/docker.sock")
        if auth:
            self.cli.login(username=username, password=password, registry=registry_url)
            self.apicli.login(
                username=username, password=password, registry=registry_url
            )


class DockerImageBuilder(ImageBuilderBase):
    def __init__(
        self,
        gc,
        imageId=None,
        tale=None,
        registry_user=None,
        registry_password=None,
        registry_url=None,
        auth=True,
    ):
        super().__init__(gc, imageId=imageId, tale=tale, auth=auth)
        self.dh = DockerHelper(
            registry_user=registry_user,
            registry_password=registry_password,
            registry_url=registry_url,
            auth=auth,
        )

    def pull_r2d(self):
        try:
            self.dh.cli.images.pull(self.container_config.repo2docker_version)
        except docker.errors.NotFound:
            raise ValueError(
                f"Requested r2d image '{self.container_config.repo2docker_version}' not found."
            )

    def push_image(self, image):
        """Push image to the registry"""
        repository, tag = image.split(":", 1)
        for line in self.dh.apicli.push(repository, tag=tag, stream=True, decode=True):
            print(line)

    def run_r2d(self, tag, dry_run=False, task=None):
        """
        Run repo2docker on the workspace using a shared temp directory. Note that
        this uses the "local" provider.  Use the same default user-id and
        user-name as BinderHub
        """

        task = task or DummyTask

        # Extra arguments for r2d
        r2d_cmd = self.r2d_command(tag, dry_run=dry_run)
        r2d_context_dir = os.path.relpath(self.build_context, tempfile.gettempdir())
        host_r2d_context_dir = os.path.join(DEPLOYMENT.tmpdir_mount, r2d_context_dir)

        logging.info("Calling %s", r2d_cmd)

        volumes = {
            "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
            host_r2d_context_dir: {"bind": self.build_context, "mode": "ro"},
        }

        print(f"Using repo2docker {self.container_config.repo2docker_version}")
        container = self.dh.cli.containers.run(
            image=self.container_config.repo2docker_version,
            command=r2d_cmd,
            environment=["DOCKER_HOST=unix:///var/run/docker.sock"],
            privileged=True,
            detach=True,
            remove=False,
            volumes=volumes,
        )

        # Job output must come from stdout/stderr
        h = hashlib.md5("R2D output".encode())
        for line in container.logs(stream=True):
            if task.canceled:
                task.request.chain = None
                stop_container(container)
                break
            output = line.decode("utf-8").strip()
            if not output.startswith("Using local repo"):  # contains variable path
                h.update(output.encode("utf-8"))
            if not dry_run:  # We don't want to see it.
                print(output)

        try:
            ret = container.wait()
        except docker.errors.NotFound:
            ret = {"StatusCode": -123}
        container.remove()

        if ret["StatusCode"] != 0:
            logging.error("Error building image")
        # Since detach=True, then we need to explicitly check for the
        # container exit code
        return ret, h.hexdigest()
