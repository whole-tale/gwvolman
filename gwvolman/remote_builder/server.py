import hashlib
import json
import logging
import os
import tempfile

import docker
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from girder_client import GirderClient

from ..r2d.docker import DockerImageBuilder

app = FastAPI()
client = docker.from_env()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@app.put("/pull")
async def pull_docker_r2d_image(
    repo2docker_version: str = Query(
        ..., description="Repository and version of the image"
    ),
):
    async def pull_stream():
        try:
            for line in client.api.pull(repository=repo2docker_version, stream=True):
                line = json.loads(line.decode("utf-8").strip())
                yield json.dumps(line) + "\n"
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"

    return StreamingResponse(pull_stream(), media_type="application/json")


@app.put("/push")
async def push_tale_image(
    image: str = Query(..., description="Repository and version of the image"),
    registry_url: str = Query(..., description="Docker registry URL"),
    registry_user: str = Query(..., description="Docker registry username"),
    registry_password: str = Query(..., description="Docker registry password"),
):
    async def push_stream():
        try:
            repository, tag = image.split(":", 1)
            client.api.login(
                registry=registry_url,
                username=registry_user,
                password=registry_password,
            )
            for line in client.api.push(repository, tag=tag, stream=True, decode=True):
                yield json.dumps(line) + "\n"
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"

    return StreamingResponse(push_stream(), media_type="application/json")


@app.post("/build")
async def build_tale(
    taleId: str = Query(..., description="Tale identifier"),
    apiUrl: str = Query(..., description="Girder API URL"),
    token: str = Query(..., description="Girder authentication token"),
    registry_url: str = Query(..., description="Docker registry URL"),
    dry_run: bool = Query(..., description="If true, do not build the image"),
    tag: str = Query(..., description="Repository and version of the image"),
):
    girder_client = GirderClient(apiUrl=apiUrl)
    girder_client.token = token
    try:
        tale = girder_client.get("tale/%s" % taleId)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Tale ID")

    image_builder = DockerImageBuilder(
        girder_client, tale=tale, registry_url=registry_url, auth=False
    )

    async def build_stream():
        try:
            yield json.dumps({"message": f"Building image {tag}"}) + "\n"
            r2d_cmd = image_builder.r2d_command(tag, dry_run=dry_run)
            r2d_context_dir = os.path.relpath(
                image_builder.build_context, tempfile.gettempdir()
            )
            host_r2d_context_dir = os.path.join("/tmp", r2d_context_dir)

            volumes = {
                host_r2d_context_dir: {
                    "bind": image_builder.build_context,
                    "mode": "rw",
                },
                "/var/run/docker.sock": {
                    "bind": "/var/run/docker.sock",
                    "mode": "rw",
                },
            }

            container = client.containers.run(
                image=image_builder.container_config.repo2docker_version,
                command=r2d_cmd,
                environment={"DOCKER_HOST": "unix:///var/run/docker.sock"},
                privileged=True,
                detach=True,
                remove=False,
                volumes=volumes,
            )

            yield json.dumps({"message": f"Calling {r2d_cmd}"}) + "\n"

            h = hashlib.md5("R2D ouptut".encode())
            for line in container.logs(stream=True):
                output = line.decode("utf-8").strip()
                if not output.startswith("Using local repo"):
                    h.update(output.encode("utf-8"))
                    logger.info(output)
                if not dry_run:
                    yield json.dumps({"message": output}) + "\n"

            try:
                ret = container.wait(timeout=10)
            except (docker.errors.TimeoutError, docker.errors.NotFound):
                ret = {"StatusCode": -123}

            if ret["StatusCode"] != 0:
                yield json.dumps({"error": f"Error building image {tag}"}) + "\n"

            yield json.dumps({"return": {"ret": ret, "digest": h.hexdigest()}}) + "\n"
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"

    return StreamingResponse(build_stream(), media_type="application/json")
