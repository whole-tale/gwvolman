import hashlib
import json
import logging
import os
import shutil
import tempfile
from urllib.parse import urlparse

import requests
from packaging import version

from ..constants import R2D_FILENAMES
from ..utils import (
    DEPLOYMENT,
    _get_container_config,
)


class ImageBuilderBase:
    _build_context = None

    @property
    def build_context(self):
        if not self._build_context:
            self._build_context = self._create_build_context()
        return self._build_context

    @property
    def engine(self):
        # See https://github.com/whole-tale/repo2docker_wholetale/pull/44
        tag = self.container_config.repo2docker_version.rsplit(":")[-1]
        try:
            if version.parse(tag[1:]) < version.Version("1.2dev0"):
                return ""
        except version.InvalidVersion:
            # i.e. not something following v{version} which in our case
            # will be either "latest" or some specific manual tag
            pass
        return "--engine dockercli"

    def __init__(self, gc, imageId=None, tale=None, auth=True):
        if (imageId is None) == (tale is None):
            raise ValueError("Only one of 'imageId' and 'tale' can be set")

        self.gc = gc
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
        raise NotImplementedError()

    def _create_build_context(self):
        temp_dir = tempfile.mkdtemp()
        logging.info(
            "Downloading r2d files to %s (taleId:%s)", temp_dir, self.tale["_id"]
        )
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
            dry_run=True,
        )
        if ret["StatusCode"] != 0:
            raise ValueError(f"Failed to compute a tag {ret=}")

        # Remove the temporary directory, cause we want entire workspace for build
        # NOTE: or maybe not? That would avoid bloating image with things we override anyway
        # shutil.rmtree(self.build_context, ignore_errors=True)

        return f"{registry_netloc}/tale/{env_hash.hexdigest()}:{output_digest}"

    def run_r2d(self, tag, dry_run=False, task=None):
        raise NotImplementedError()

    def push_image(self, image):
        raise NotImplementedError()

    def __del__(self):
        if self._build_context is not None:
            shutil.rmtree(self._build_context, ignore_errors=True)

    def cached_image(self, image):
        """Check if image exists in the registry"""
        _, full_name = image.split("/", 1)
        name, tag = full_name.split(":", 1)
        try:
            with requests.Session() as session:
                session.auth = (
                    os.environ.get("REGISTRY_USER", "fido"),
                    os.environ.get("REGISTRY_PASS"),
                )
                base_url = (
                    urlparse(DEPLOYMENT.registry_url)._replace(path="/v2/").geturl()
                )

                req = session.get(base_url)
                req.raise_for_status()

                req = session.get(
                    f"{base_url}{name}/manifests/{tag}",
                    headers={
                        "Accept": "application/vnd.docker.distribution.manifest.v2+json"
                    },
                )
                req.raise_for_status()
                manifest = req.json()
                content_digest = req.headers["Docker-Content-Digest"]

                config_digest = manifest["config"]["digest"]

                req = session.get(
                    f"{base_url}{name}/blobs/{config_digest}",
                    headers={"Accept": manifest["config"]["mediaType"]},
                )
                req.raise_for_status()
                config = req.json()

                return {
                    "name": f"{urlparse(base_url).netloc}/{name}",
                    "tag": tag,
                    "digest": content_digest,
                    "created": config["created"],
                    "labels": config["config"]["Labels"],
                    "architecture": config["architecture"],
                    "os": config["os"],
                }
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                return
            raise
