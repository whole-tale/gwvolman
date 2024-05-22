"""A set of WT related Girder tasks."""
import logging
import os
import time

from girder_worker.app import app
from girder_worker.utils import girder_job

from .r2d import ImageBuilder
from .tasks_factory import TasksFactory

# from girder_worker.plugins.docker.executor import _pull_image
from .utils import (
    _safe_mkdir,
)

tasks = TasksFactory(os.environ.get("DEPLOYMENT", "docker")).getTasksInstance()


@girder_job(title="Create Tale Data Volume")
@app.task(bind=True)
def create_volume(task, instance_id, mounts):
    return tasks.create_volume(task, instance_id, mounts=mounts)


@girder_job(title="Spawn Instance")
@app.task(bind=True)
def launch_container(task, service_info):
    return tasks.launch_container(task, service_info)


@girder_job(title="Update Instance")
@app.task(bind=True)
def update_container(task, instanceId, digest=None):
    return tasks.update_container(task, instanceId, digest=digest)


@girder_job(title="Shutdown Instance")
@app.task(bind=True)
def shutdown_container(task, instanceId):
    return tasks.shutdown_container(task, instanceId)


@girder_job(title="Remove Tale Data Volume")
@app.task(bind=True)
def remove_volume(task, instanceId):
    return tasks.remove_volume(task, instanceId)


@girder_job(title="Build Tale Image")
@app.task(bind=True)
def build_tale_image(task, tale_id, force=False):
    return tasks.build_tale_image(task, tale_id, force=force)


@girder_job(title="Publish Tale")
@app.task(bind=True)
def publish(task, tale_id, token, version_id, repository=None, draft=False):
    return tasks.publish(
        task, tale_id, token, version_id, repository=repository, draft=draft
    )


@girder_job(title="Import Tale")
@app.task(bind=True)
def import_tale(task, lookup_kwargs, tale, spawn=True):
    return tasks.import_tale(task, lookup_kwargs, tale, spawn=spawn)


@app.task(bind=True)
def rebuild_image_cache(self):
    logging.info("Rebuilding image cache")

    # Get the list of images
    images = self.girder_client.get("/image")

    # Build each base image
    for image in images:
        image_builder = ImageBuilder(self.girder_client, imageId=image["_id"])
        tag = image_builder.get_tag()
        container_config = image_builder.container_config

        logging.info(
            "Building %s %s in %s with %s",
            image["name"],
            tag,
            image_builder.build_context,
            container_config.repo2docker_version,
        )

        start = time.time()
        ret, _ = image_builder.run_r2d(tag)

        elapsed = int(time.time() - start)
        if ret["StatusCode"] != 0:
            logging.error("Error building %s", image["name"])
        else:
            logging.info("Build time: %i seconds", elapsed)


def _make_fuse_dirs(mountpoint, directories):
    """Create fuse directories"""
    for suffix in directories:
        _safe_mkdir(os.path.join(mountpoint, suffix))


@girder_job(title="Recorded Run")
@app.task(bind=True)
def recorded_run(task, run_id, tale_id, entrypoint):
    return tasks.recorded_run(task, run_id, tale_id, entrypoint)


@app.task()
def check_on_run(run_state):
    return tasks.check_on_run(run_state)


@girder_job(title="Clean failed Recorded Run")
@app.task(bind=True)
def cleanup_run(task, run_id):
    return tasks.cleanup_run(task, run_id)
