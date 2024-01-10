import base64
import hashlib
import logging
import os
import tempfile

import docker

from ..utils import (
    DEPLOYMENT,
    DummyTask,
    _get_stata_license_path,
    stop_container,
)
from .builder import ImageBuilderBase


class DockerHelper:
    def __init__(self, auth=True):
        username = os.environ.get("REGISTRY_USER", "fido")
        password = os.environ.get("REGISTRY_PASS")
        self.cli = docker.from_env(version="1.28")
        self.apicli = docker.APIClient(base_url="unix://var/run/docker.sock")
        if auth:
            self.cli.login(
                username=username, password=password, registry=DEPLOYMENT.registry_url
            )
            self.apicli.login(
                username=username, password=password, registry=DEPLOYMENT.registry_url
            )


class DockerImageBuilder(ImageBuilderBase):
    def __init__(self, gc, imageId=None, tale=None, auth=True):
        super().__init__(gc, imageId=imageId, tale=tale, auth=auth)
        self.dh = DockerHelper(auth)

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
        extra_args = ""
        if self.container_config.buildpack == "MatlabBuildPack":
            extra_args = " --build-arg FILE_INSTALLATION_KEY={} ".format(
                os.environ.get("MATLAB_FILE_INSTALLATION_KEY")
            )
        elif self.container_config.buildpack == "StataBuildPack" and not dry_run:
            # License is also needed at build time but can't easily
            # be mounted. Pass it as a build arg
            with open(_get_stata_license_path(), "r") as license_file:
                stata_license = license_file.read()
                encoded = base64.b64encode(stata_license.encode("ascii")).decode(
                    "ascii"
                )
                extra_args = " --build-arg STATA_LICENSE_ENCODED='{}' ".format(encoded)

        op = "--no-build" if dry_run else "--no-run"
        target_repo_dir = os.path.join(self.container_config.target_mount, "workspace")
        r2d_cmd = (
            f"jupyter-repo2docker {self.engine} "
            "--config='/wholetale/repo2docker_config.py' "
            f"--target-repo-dir='{target_repo_dir}' "
            f"--user-id=1000 --user-name={self.container_config.container_user} "
            f"--no-clean {op} --debug {extra_args} "
            f"--image-name {tag} {self.build_context}"
        )

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
