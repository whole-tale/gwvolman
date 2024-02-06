import docker
from datetime import datetime, timedelta
import os
import time
import logging
import json

from .constants import (
    CREATE_VOLUME_STEP_TOTAL,
    LAUNCH_CONTAINER_STEP_TOTAL,
    RECORDED_RUN_STEP_TOTAL,
    UPDATE_CONTAINER_STEP_TOTAL,
)
from .utils import (
    new_user,
    _get_api_key,
    _get_container_config,
    _launch_container,
    _get_user_and_instance,
    _recorded_run,
    stop_container,
)
from .fs_container import FSContainer
from .tasks_base import TasksBase
from .constants import GIRDER_API_URL, RunStatus, VOLUMES_ROOT

from .r2d import DockerImageBuilder


class DockerTasks(TasksBase):
    def create_volume(self, task, instance_id, mounts=None):
        """Create a mountpoint and compose WT-fs."""
        user, instance = _get_user_and_instance(task.girder_client, instance_id)
        tale = task.girder_client.get("/tale/{taleId}".format(**instance))

        task.job_manager.updateProgress(
            message="Creating volume",
            total=CREATE_VOLUME_STEP_TOTAL,
            current=1,
            forceFlush=True,
        )

        vol_name = "%s_%s_%s" % (tale["_id"], user["login"], new_user(6))
        fs_sidecar = FSContainer.start_container(vol_name)
        if mounts is None:
            mounts = [
                {
                    "type": "data",
                    "protocol": "girderfs",
                    "location": "data",
                },
                {
                    "type": "home",
                    "protocol": "bind",
                    "location": "home",
                },
                {
                    "type": "workspace",
                    "protocol": "bind",
                    "location": "workspace",
                },
                {
                    "type": "versions",
                    "protocol": "girderfs",
                    "location": "versions",
                },
                {
                    "type": "runs",
                    "protocol": "girderfs",
                    "location": "runs",
                },
            ]

        payload = {
            "mounts": mounts,
            "taleId": tale["_id"],
            "userId": user["_id"],
            "girderApiUrl": GIRDER_API_URL,
            "girderApiKey": _get_api_key(task.girder_client),
            "girderToken": task.girder_client.token,
            "root": vol_name,
        }
        print(json.dumps(payload))
        FSContainer.mount(fs_sidecar, payload)
        task.job_manager.updateProgress(
            message="Volume created",
            total=CREATE_VOLUME_STEP_TOTAL,
            current=CREATE_VOLUME_STEP_TOTAL,
            forceFlush=True,
        )
        print("WT Filesystem created successfully.")

        cli = docker.from_env()
        return dict(
            nodeId=cli.info()["Swarm"]["NodeID"],
            fscontainerId=fs_sidecar.id,
            volumeName=vol_name,
            instanceId=instance_id,
            taleId=tale["_id"],
        )

    def launch_container(self, task, service_info):
        """Launch a container using a Tale object."""
        user, instance = _get_user_and_instance(
            task.girder_client, service_info["instanceId"]
        )
        tale = task.girder_client.get(f"/tale/{service_info['taleId']}")

        task.job_manager.updateProgress(
            message="Starting container",
            total=LAUNCH_CONTAINER_STEP_TOTAL,
            current=1,
            forceFlush=True,
        )

        print("Launching container for a Tale...")
        if "imageInfo" not in tale:
            # Wait for image to be built
            tic = time.time()
            timeout = 180.0
            time_interval = 5

            while time.time() - tic < timeout:
                tale = task.girder_client.get("/tale/{taleId}".format(**instance))
                if "imageInfo" in tale and "digest" in tale["imageInfo"]:
                    break
                msg = f"Waiting for image build to complete. ({time_interval}s)"
                logging.info(msg)
                print(msg)
                time.sleep(5)

        container_config = _get_container_config(task.girder_client, tale)
        service, attrs = _launch_container(
            service_info, container_config, task.girder_client
        )
        print(
            f"Started a container using volume: {service_info['volumeName']} "
            f"on node: {service_info['nodeId']}"
        )

        # wait until task is started
        tic = time.time()
        timeout = 300.0
        started = False

        print("Waiting for the environment to be accessible...")
        while time.time() - tic < timeout:
            try:
                status = service.tasks()[0]["Status"]

                if status["State"] in {"failed", "rejected"}:
                    raise ValueError("Failed to start environment: %s" % status["Err"])
                elif status["State"] == "running":
                    started = True
                    break

            except IndexError:
                started = False

            time.sleep(0.2)

        if not started:
            raise ValueError("Tale did not start before timeout exceeded")

        print("Environment is up and running.")
        task.job_manager.updateProgress(
            message="Container started",
            total=LAUNCH_CONTAINER_STEP_TOTAL,
            current=LAUNCH_CONTAINER_STEP_TOTAL,
            forceFlush=True,
        )

        service_info.update(attrs)
        service_info["name"] = service.name
        return service_info

    def update_container(self, task, instanceId, digest=None):
        user, instance = _get_user_and_instance(task.girder_client, instanceId)

        cli = docker.from_env(version="1.28")
        if "containerInfo" not in instance:
            return
        containerInfo = instance["containerInfo"]  # VALIDATE
        try:
            service = cli.services.get(containerInfo["name"])
        except docker.errors.NotFound:
            logging.info("Service not present [%s].", containerInfo["name"])
            return

        task.job_manager.updateProgress(
            message="Restarting the Tale with a new image",
            total=UPDATE_CONTAINER_STEP_TOTAL,
            current=1,
            forceFlush=True,
        )

        # Don't try to restart if the image hasn't changed
        try:
            previous_image = service.attrs["Spec"]["TaskTemplate"]["ContainerSpec"][
                "Image"
            ]
        except KeyError:
            previous_image = ""

        if previous_image == digest:
            task.job_manager.updateProgress(
                message="Image has not changed",
                total=UPDATE_CONTAINER_STEP_TOTAL,
                current=UPDATE_CONTAINER_STEP_TOTAL,
            )
            return {"image_digest": digest}

        try:
            # NOTE: Only "image" passed currently, but this can be easily extended
            logging.info("Restarting container [%s].", service.name)
            service.update(image=digest)
            logging.info(
                "Restart command has been sent to Container [%s].", service.name
            )
        except Exception as e:
            logging.error(
                "Unable to send restart command to container [%s]: %s", service.id, e
            )

        updated = False
        expired = False
        timeout = datetime.now() + timedelta(minutes=3)
        while not (updated or expired or task.canceled):
            service = cli.services.get(containerInfo["name"])

            try:
                state = service.attrs["UpdateStatus"]["State"]
            except KeyError:
                state = ""

            if state == "paused":
                raise RuntimeError(
                    'Restarting the Tale failed with "{}"'.format(
                        service.attrs["UpdateStatus"]["Message"]
                    )
                )

            updated = state == "completed"
            expired = datetime.now() > timeout
            time.sleep(1.0)

        if task.canceled:
            raise RuntimeError("Tale restart cancelled")
        elif expired:
            raise RuntimeError("Tale update timed out")

        task.job_manager.updateProgress(
            message="Tale restarted with the new image",
            total=UPDATE_CONTAINER_STEP_TOTAL,
            current=UPDATE_CONTAINER_STEP_TOTAL,
        )

        return {"image_digest": digest}

    def shutdown_container(self, task, instanceId):
        """Shutdown a running Tale."""
        user, instance = _get_user_and_instance(task.girder_client, instanceId)

        cli = docker.from_env(version="1.28")
        if "containerInfo" not in instance:
            return
        containerInfo = instance["containerInfo"]  # VALIDATE
        try:
            service = cli.services.get(containerInfo["name"])
        except docker.errors.NotFound:
            logging.info("Service not present [%s].", containerInfo["name"])
            return

        try:
            logging.info("Releasing container [%s].", service.name)
            service.remove()
            logging.info("Container [%s] has been released.", service.name)
        except Exception as e:
            logging.error("Unable to release container [%s]: %s", service.id, e)

    def remove_volume(self, task, instanceId):
        """Unmount WT-fs and remove mountpoint."""
        logging.info("Stopping FS container for instance %s", instanceId)
        user, instance = _get_user_and_instance(task.girder_client, instanceId)

        if "containerInfo" not in instance:
            logging.warning("No containerInfo for instance %s", instanceId)
            return
        containerInfo = instance["containerInfo"]  # VALIDATE
        FSContainer.stop_container(containerInfo["fscontainerId"])
        logging.info("FS container %s stopped", containerInfo["fscontainerId"])

    def recorded_run(self, task, run_id, tale_id, entrypoint):
        """Start a recorded run for a tale version"""
        run = task.girder_client.get(f"/run/{run_id}")
        state = RecordedRunCleaner(run, task.girder_client)
        tale = task.girder_client.get(
            f"/tale/{tale_id}/restore", parameters={"versionId": run["runVersionId"]}
        )
        user = task.girder_client.get("/user/me")
        image_builder = DockerImageBuilder(task.girder_client, tale=tale)

        def set_run_status(run, status):
            task.girder_client.patch(
                "/run/{_id}/status".format(**run), parameters={"status": status}
            )

        # UNKNOWN = 0 STARTING = 1 RUNNING = 2 COMPLETED = 3 FAILED = 4 CANCELLED = 5
        set_run_status(run, RunStatus.STARTING)

        task.job_manager.updateProgress(
            message="Preparing volumes",
            total=RECORDED_RUN_STEP_TOTAL,
            current=1,
            forceFlush=True,
        )

        # Create Docker volume
        vol_name = "%s_%s_%s" % (run_id, user["login"], new_user(6))
        fs_sidecar = FSContainer.start_container(vol_name)
        payload = {
            "mounts": [
                {
                    "type": "data",
                    "protocol": "girderfs",
                    "location": "data",
                },
                {
                    "type": "run",
                    "protocol": "bind",
                    "location": "workspace",
                },
            ],
            "girderApiUrl": GIRDER_API_URL,
            "girderApiKey": _get_api_key(task.girder_client),
            "root": vol_name,
            "taleId": tale["_id"],
            "runId": run["_id"],
            "userId": user["_id"],
        }
        FSContainer.mount(fs_sidecar, payload)
        state.volume_created = vol_name

        # Build the image for the run
        task.job_manager.updateProgress(
            message="Building image",
            total=RECORDED_RUN_STEP_TOTAL,
            current=2,
            forceFlush=True,
        )

        # Setup image tag
        tag = image_builder.get_tag()
        logging.info(
            "Computed tag: %s (taleId:%s, versionId:%s)",
            tag,
            tale_id,
            run["runVersionId"],
        )

        # Build currently assumes tmp directory, in this case mount the run workspace
        container_config = image_builder.container_config

        if task.canceled:
            state.cleanup()
            return

        try:
            if not image_builder.cached_image(tag):
                print("Building image for recorded run " + tag)
                ret, _ = image_builder.run_r2d(tag)
                if task.canceled:
                    state.cleanup()
                    return
                if ret["StatusCode"] != 0:
                    raise ValueError(
                        "Image build failed for recorded run {}".format(run_id)
                    )
                image_builder.push_image(tag)

            task.job_manager.updateProgress(
                message="Recording run",
                total=RECORDED_RUN_STEP_TOTAL,
                current=3,
                forceFlush=True,
            )

            set_run_status(run, RunStatus.RUNNING)
            container_name = f"rrun-{new_user(8)}"

            task.girder_client.addMetadataToFolder(
                run["_id"],
                {
                    "container_name": container_name,
                    "volume_created": state.volume_created,
                    "node_id": image_builder.dh.cli.info()["Swarm"]["NodeID"],
                },
            )

            mountpoint = os.path.join(VOLUMES_ROOT, "mountpoints", state.volume_created)

            _recorded_run(
                image_builder.dh.cli,
                mountpoint,
                container_config,
                tag,
                entrypoint,
                container_name,
                task=task,
            )
            if task.canceled:
                state.cleanup()
                return

            set_run_status(run, RunStatus.COMPLETED)
            task.job_manager.updateProgress(
                message="Finished recorded run",
                total=RECORDED_RUN_STEP_TOTAL,
                current=4,
                forceFlush=True,
            )
        except Exception as exc:
            logging.error(exc, exc_info=True)
            raise
        finally:
            state.cleanup(False)

    def check_on_run(self, run_state):
        cli = docker.from_env(version="1.28")
        try:
            container = cli.containers.get(run_state["container_name"])
            return container.status == "running"
        except docker.errors.NotFound:
            return False

    def cleanup_run(self, task, run_id):
        run = task.girder_client.get(f"/run/{run_id}")
        state = RecordedRunCleaner(run, task.girder_client)
        state.volume_created = run["meta"].get("volume_created")
        state.container_name = run["meta"].get("container_name")
        state.cleanup(canceled=False)
        state.set_run_status(RunStatus.FAILED)
        if job_id := run["meta"]["jobId"]:
            task.girder_client.put(f"/job/{job_id}", parameters={"status": 4})


class RecordedRunCleaner:
    volume_created = None
    container_name = None

    def __init__(self, run, gc):
        self.gc = gc
        self.run = run
        self.docker_cli = docker.from_env(version="1.28")

    def set_run_status(self, status):
        self.gc.patch(
            "/run/{_id}/status".format(**self.run), parameters={"status": status}
        )

    def cleanup(self, canceled=True):
        if self.container_name:
            container = self.docker_cli.containers.get(self.container_name)
            stop_container(container)

        if self.volume_created:
            FSContainer.stop_container(self.volume_created)
            self.volume_created = None

        if canceled:
            self.set_run_status(RunStatus.CANCELED)
