import base64
import docker
import hashlib
import json
import logging
import os
import shutil
import tempfile
from urllib.parse import urlparse

from .constants import R2D_FILENAMES
from .utils import _get_container_config, DEPLOYMENT, _get_stata_license_path


class DockerHelper:
    def __init__(self):
        username = os.environ.get("REGISTRY_USER", "fido")
        password = os.environ.get("REGISTRY_PASS")
        self.cli = docker.from_env(version='1.28')
        self.cli.login(
            username=username, password=password, registry=DEPLOYMENT.registry_url
        )
        self.apicli = docker.APIClient(base_url="unix://var/run/docker.sock")
        self.apicli.login(
            username=username, password=password, registry=DEPLOYMENT.registry_url
        )


class ImageBuilder:
    _build_context = None

    @property
    def build_context(self):
        if not self._build_context:
            self._build_context = self._create_build_context()
        return self._build_context

    def __init__(self, gc, imageId=None, tale=None):
        if (imageId is None) == (tale is None):
            raise ValueError("Only one of 'imageId' and 'tale' can be set")

        self.gc = gc
        self.dh = DockerHelper()
        if tale is None:
            tale = {
                "_id": None,
                "imageId": imageId,
                "workspaceId": None,
                "config": {"extra_build_files": []},
            }
        self.tale = tale
        self.container_config = _get_container_config(gc, self.tale)

    def pull_r2d(self):
        try:
            self.dh.cli.images.pull(self.container_config.repo2docker_version)
        except docker.errors.NotFound:
            raise ValueError(
                f"Requested r2d image '{self.container_config.repo2docker_version}' not found."
            )

    def _create_build_context(self):
        temp_dir = tempfile.mkdtemp(dir=os.environ.get("HOSTDIR", "/host") + "/tmp")
        logging.info("Downloading r2d files to %s (taleId:%s)", temp_dir, self.tale["_id"])
        extra_build_files = self.tale["config"].get("extra_build_files", [])
        workspaceId = self.tale.get("workspaceId")
        if workspaceId:
            if "**" in extra_build_files:
                # A special case when we want to have an entire workspace in the build context
                self.gc.downloadFolderRecursive(workspaceId, temp_dir)
            else:
                # Download standard r2d files
                for name in R2D_FILENAMES:
                    if item := next(self.gc.listItem(workspaceId, name=name), None):
                        self.gc.downloadItem(item["_id"], temp_dir)
                # Download any extra files specified by the Tale's config
                root_path = self.gc.get(
                    f"/resource/{workspaceId}/path", parameters={"type": "folder"}
                )
                for path in extra_build_files:
                    if resource := self.gc.get(
                        "/resource/lookup",
                        parameters={
                            "path": os.path.join(root_path, path),
                            "test": False,
                        },
                    ):
                        if resource["_modelType"] == "item":
                            self.gc.downloadItem(resource["_id"], temp_dir)
                        elif resource["_modelType"] == "folder":
                            self.gc.downloadFolderRecursive(resource["_id"], temp_dir)

        # Write the environment.json to the r2d context directory
        with open(os.path.join(temp_dir, "environment.json"), "w") as fp:
            json.dump(
                {
                    "config": {
                        "buildpack": self.container_config.buildpack,
                        "environment": self.container_config.environment,
                        "user": self.container_config.container_user,
                    }
                },
                fp,
            )
        return temp_dir

    def get_tag(self, force=False):
        """Compute a unique docker image tag.

        Tag is created as combination of 1) checksum of repo2docker files (apt.txt, etc)
        and the tale/image environment file, 2) checksum of Dockerfile created by r2d
        using files from 1).
        """
        env_hash = hashlib.md5("Environment checksum".encode())
        for root, dirs, files in os.walk(self.build_context):
            dirs.sort()
            for fname in sorted(files):
                env_hash.update(fname.encode())
                with open(os.path.join(root, fname), "rb") as fp:
                    env_hash.update(fp.read())
        if force and self.tale["_id"]:
            env_hash.update(self.tale["_id"].encode())

        # Perform dry run to get the Dockerfile's checksum
        registry_netloc = urlparse(DEPLOYMENT.registry_url).netloc
        ret, output_digest = self.run_r2d(
            f"{registry_netloc}/placeholder_env/placeholder_dockerfile",
            self.build_context,
            dry_run=True,
        )

        # Remove the temporary directory, cause we want entire workspace for build
        # NOTE: or maybe not? That would avoid bloating image with things we override anyway
        # shutil.rmtree(self.build_context, ignore_errors=True)

        return f"{registry_netloc}/tale/{env_hash.hexdigest()}:{output_digest}"

    def run_r2d(
        self,
        tag,
        build_dir,
        extra_volume=None,
        dry_run=False,
    ):
        """
        Run repo2docker on the workspace using a shared temp directory. Note that
        this uses the "local" provider.  Use the same default user-id and
        user-name as BinderHub
        """

        # Extra arguments for r2d
        extra_args = ''
        if self.container_config.buildpack == "MatlabBuildPack":
            extra_args = ' --build-arg FILE_INSTALLATION_KEY={} '.format(
                os.environ.get("MATLAB_FILE_INSTALLATION_KEY")
            )
        elif self.container_config.buildpack == "StataBuildPack":
            # License is also needed at build time but can't easily
            # be mounted. Pass it as a build arg

            source_path = _get_stata_license_path()
            with open("/host/" + source_path, "r") as license_file:
                stata_license = license_file.read()
                encoded = base64.b64encode(stata_license.encode("ascii")).decode("ascii")
                extra_args = " --build-arg STATA_LICENSE_ENCODED='{}' ".format(encoded)

        op = "--no-build" if dry_run else "--no-run"
        target_repo_dir = os.path.join(self.container_config.target_mount, "workspace")
        r2d_cmd = (
            "jupyter-repo2docker "
            "--config='/wholetale/repo2docker_config.py' "
            f"--target-repo-dir='{target_repo_dir}' "
            f"--user-id=1000 --user-name={self.container_config.container_user} "
            f"--no-clean {op} --debug {extra_args} "
            f"--image-name {tag} {build_dir}"
        )

        logging.info('Calling %s', r2d_cmd)

        volumes = {
            '/var/run/docker.sock': {
                'bind': '/var/run/docker.sock', 'mode': 'rw'
            },
            '/tmp': {
                'bind': '/host/tmp', 'mode': 'ro'
            }
        }

        if extra_volume is not None:
            volumes.update(extra_volume)

        container = self.dh.cli.containers.run(
            image=self.container_config.repo2docker_version,
            command=r2d_cmd,
            environment=['DOCKER_HOST=unix:///var/run/docker.sock'],
            privileged=True,
            detach=True,
            remove=True,
            volumes=volumes
        )

        # Job output must come from stdout/stderr
        h = hashlib.md5("R2D output".encode())
        for line in container.logs(stream=True):
            output = line.decode("utf-8").strip()
            if not output.startswith("Using local repo"):  # contains variable path
                h.update(output.encode("utf-8"))
            if not dry_run:  # We don't want to see it.
                print(output)

        ret = container.wait()
        if ret["StatusCode"] != 0:
            logging.error("Error building image")
        # Since detach=True, then we need to explicitly check for the
        # container exit code
        return ret, h.hexdigest()

    def __del__(self):
        if self._build_context is not None:
            shutil.rmtree(self._build_context, ignore_errors=True)
