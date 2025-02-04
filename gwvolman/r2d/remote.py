import json
import os

import requests

from .builder import ImageBuilderBase


class RemoteImageBuilder(ImageBuilderBase):
    def __init__(
        self,
        gc,
        imageId=None,
        tale=None,
        builder_url=None,
        registry_user=None,
        registry_password=None,
        registry_url=None,
        auth=True,
    ):
        super().__init__(gc, imageId=imageId, tale=tale, auth=auth)
        self.builder_url = builder_url or os.environ.get(
            "BUILDER_URL", "https://builder.local.xarthisius.xyz"
        )
        self.registry_url = registry_url or "https://registry.local.xarthisius.xyz"
        self.registry_user = registry_user or os.environ.get("REGISTRY_USER", "fido")
        self.registry_password = registry_password or os.environ.get("REGISTRY_PASS")

    def pull_r2d(self):
        response = requests.put(
            f"{self.builder_url}/pull",
            params={
                "repo2docker_version": self.container_config.repo2docker_version,
            },
            stream=True,
        )
        for chunk in response.iter_lines():  # Adjust chunk size as needed
            try:
                msg = json.loads(chunk)
                try:
                    print(msg["status"])
                except KeyError:
                    raise json.jsonJSONDecodeError
            except json.JSONDecodeError:
                print(chunk)

    def push_image(self, image):
        """Push image to the registry"""
        repository, tag = image.split(":", 1)
        response = requests.put(
            f"{self.builder_url}/push",
            params={
                "image": image,
                "registry_user": self.registry_user,
                "registry_password": self.registry_password,
                "registry_url": self.registry_url,
            },
            stream=True,
        )
        for chunk in response.iter_lines():  # Adjust chunk size as needed
            print(chunk)

    def run_r2d(self, tag, dry_run=False, task=None):
        """
        Run repo2docker on the workspace using a shared temp directory. Note that
        this uses the "local" provider.  Use the same default user-id and
        user-name as BinderHub
        """
        response = requests.post(
            f"{self.builder_url}/build",
            params={
                "taleId": self.tale["_id"],
                "apiUrl": self.gc.urlBase,
                "token": self.gc.token,
                "registry_url": self.registry_url,
                "dry_run": dry_run,
                "tag": tag,
            },
            stream=True,
        )
        for chunk in response.iter_lines():
            try:
                msg = json.loads(chunk)
                if "message" in msg:
                    msg = msg["message"]
                    if isinstance(msg, dict) and "error" in msg.keys():
                        return {"StatusCode": 1, "error": msg["error"]}, None
                    print(msg)
                elif "return" in msg:
                    data = msg["return"]
                    return data["ret"], data["digest"]
                elif "error" in msg:
                    return {"StatusCode": 1, "error": msg["error"]}, None
            except json.JSONDecodeError:
                print(chunk)
