import json
import logging
import os
import time
from urllib.parse import urlparse

import girder_client

from .constants import BUILD_TALE_IMAGE_STEP_TOTAL, InstanceStatus, TaleStatus
from .lib.zenodo import ZenodoPublishProvider
from .r2d import ImageBuilder


class TasksBase:
    def create_volume(self, task, instance_id: str):
        raise NotImplementedError()

    def launch_container(self, task, service_info):
        raise NotImplementedError()

    def update_container(self, task, instanceId, digest=None):
        raise NotImplementedError()

    def shutdown_container(self, task, instanceId):
        raise NotImplementedError()

    def remove_volume(self, task, instanceId):
        raise NotImplementedError()

    def build_tale_image(self, task, tale_id, force=False):
        """
        Build docker image from Tale workspace using repo2docker and push to Whole Tale registry.
        """
        logging.info("Building image for Tale %s", tale_id)

        task.job_manager.updateProgress(
            message="Building image",
            total=BUILD_TALE_IMAGE_STEP_TOTAL,
            current=1,
            forceFlush=True,
        )

        tic = time.time()
        tale = task.girder_client.get("/tale/%s" % tale_id)
        while tale["status"] != TaleStatus.READY:
            time.sleep(2)
            tale = task.girder_client.get("/tale/{_id}".format(**tale))
            if tale["status"] == TaleStatus.ERROR:
                raise ValueError("Cannot build image for a Tale in error state.")
            if time.time() - tic > 5 * 60.0:
                raise ValueError(
                    "Cannot build image. Tale preparing for more than 5 minutes."
                )

        last_build_time = -1
        try:
            last_build_time = tale["imageInfo"]["last_build"]
        except KeyError:
            pass

        logging.info("Last build time {}".format(last_build_time))
        image_builder = ImageBuilder(task.girder_client, tale=tale)
        image_builder.pull_r2d()

        tag = image_builder.get_tag(force=force)

        logging.info("Computed tag: %s (taleId:%s)", tag, tale_id)

        # Use the current time as the image build time and tag
        build_time = int(time.time())

        # Check if image already exists
        print("Checking if image exists...")
        print(f"Forced build: {force}")
        print(f"Last build time: {last_build_time}")
        print(f"image_builder.cached_image(tag): {image_builder.cached_image(tag)}")
        if not force and (image := image_builder.cached_image(tag)):
            print("Cached image exists for this Tale. Skipping build.")
            task.job_manager.updateProgress(
                message="Tale not modified, no need to build",
                total=BUILD_TALE_IMAGE_STEP_TOTAL,
                current=BUILD_TALE_IMAGE_STEP_TOTAL,
                forceFlush=True,
            )
            return {
                "image_digest": f"{image['name']}:{image['tag']}@{image['digest']}",
                "repo2docker_version": image_builder.container_config.repo2docker_version,
                "last_build": last_build_time,
            }

        print("Forcing build.")

        # Prepare build context
        ret, _ = image_builder.run_r2d(tag, task=task)
        if task.canceled:
            task.request.chain = None
            logging.info("Build canceled.")
            return

        if ret["StatusCode"] != 0:
            # repo2docker build failed
            print(ret)
            raise ValueError("Error building tale {}".format(tale_id))

        # Push the image to the registry
        logging.info("Pushing image %s", tag)
        image_builder.push_image(tag)
        logging.info("Image pushed")

        # Get the built image digest
        logging.info("Getting image from cache...")
        image = image_builder.cached_image(tag)
        logging.info("Image: %s", image)

        task.job_manager.updateProgress(
            message="Image build succeeded",
            total=BUILD_TALE_IMAGE_STEP_TOTAL,
            current=BUILD_TALE_IMAGE_STEP_TOTAL,
            forceFlush=True,
        )

        logging.info(
            f"Successfully built image {image['name']}:{image['tag']} ({image['digest']})"
        )

        # Image digest used by updateBuildStatus handler
        return {
            "image_digest": f"{image['name']}:{image['tag']}@{image['digest']}",
            "repo2docker_version": image_builder.container_config.repo2docker_version,
            "last_build": build_time,
        }

    def publish(self, task, tale_id, token, version_id, repository=None, draft=False):
        """
        Publish a tale.

        :param tale_id: The tale id
        :param token: An access token for a given repository.
        :param version_id: The version of the Tale being published
        :param repository: Target repository.
        :param draft: If True, don't mint DOI.
        :type tale_id: str
        :type token: obj
        :type repository: str
        :type draft: boolean
        """

        provider_name = token["provider"].lower()
        if provider_name == "zenodo":
            provider = ZenodoPublishProvider(
                task.girder_client,
                tale_id,
                token,
                version_id,
                draft=draft,
                job_manager=task.job_manager,
            )
        else:
            raise ValueError("Unsupported publisher ({})".format(token["provider"]))

        if (
            provider.published
            and provider.publication_info.get("versionId") == version_id
        ):
            raise ValueError(
                f"This version of the Tale ({version_id}) has already been published."
            )
        provider.publish()
        return provider.publication_info

    def import_tale(self, task, lookup_kwargs, tale, spawn=True):
        """Create a Tale provided a url for an external data and an image Id.

        Currently, this task only handles importing raw data. In the future, it
        should also allow importing serialized Tales.
        """
        if spawn:
            total = 4
        else:
            total = 3

        if spawn:
            try:
                instance = task.girder_client.post(
                    "/instance", parameters={"taleId": tale["_id"]}
                )
            except girder_client.HttpError as resp:
                try:
                    message = json.loads(resp.responseText).get("message", "")
                except json.JSONDecodeError:
                    message = str(resp)
                errormsg = "Unable to create instance. Server returned {}: {}"
                errormsg = errormsg.format(resp.status, message)

        def set_tale_error_status():
            task.girder_client.put(
                "/tale/{_id}".format(**tale),
                json={
                    "status": TaleStatus.ERROR,
                    "imageId": str(tale["imageId"]),
                    "public": tale["public"],
                },
            )

        task.job_manager.updateProgress(
            message="Gathering basic info about the dataset", total=total, current=1
        )
        dataId = lookup_kwargs.pop("dataId")
        try:
            parameters = dict(dataId=json.dumps(dataId))
            parameters.update(lookup_kwargs)
            dataMap = task.girder_client.get(
                "/repository/lookup", parameters=parameters
            )
        except girder_client.HttpError as resp:
            try:
                message = json.loads(resp.responseText).get("message", "")
            except json.JSONDecodeError:
                message = str(resp)
            errormsg = 'Unable to register "{}". Server returned {}: {}'
            errormsg = errormsg.format(dataId[0], resp.status, message)
            set_tale_error_status()
            raise ValueError(errormsg)

        if not dataMap:
            errormsg = 'Unable to register "{}". Source is not supported'
            errormsg = errormsg.format(dataId[0])
            set_tale_error_status()
            raise ValueError(errormsg)

        task.job_manager.updateProgress(
            message="Registering the dataset in Whole Tale", total=total, current=2
        )
        parameters = {"dataMap": json.dumps(dataMap)}
        try:
            parameters["base_url"] = lookup_kwargs.pop("base_url")
        except KeyError:
            pass
        task.girder_client.post("/dataset/register", parameters=parameters)

        # Currently, we register resources in two different ways:
        #  1. DOIs (coming from Globus, Dataverse, DataONE, etc) create a root
        #     folder in the Catalog, that's named exactly the same as dataset.
        #  2. HTTP(S) files are registered into Catalog using a nested structure
        #     based on their url (see whole-tale/girder_wholetale#266)
        #  Knowing that, let's try to find the newly registered data by path.
        catalog_path = "/collection/WholeTale Catalog/WholeTale Catalog"
        if dataMap[0]["repository"].lower().startswith("http"):
            url = urlparse(dataMap[0]["dataId"])
            path = os.path.join(catalog_path, url.netloc, url.path[1:])
        else:
            path = os.path.join(catalog_path, dataMap[0]["name"])

        resource = task.girder_client.get("/resource/lookup", parameters={"path": path})
        if not resource:
            errormsg = "Registration of {} failed. Aborting!".format(
                dataMap[0]["dataId"]
            )
            set_tale_error_status()
            raise ValueError(errormsg)

        tale["dataSet"] = [
            {
                "mountPath": resource["name"],
                "itemId": resource["_id"],
                "_modelType": resource["_modelType"],
            }
        ]
        tale = task.girder_client.put(
            "/tale/{_id}".format(**tale),
            json={
                "dataSet": tale["dataSet"],
                "imageId": str(tale["imageId"]),
                "public": tale["public"],
                "status": TaleStatus.READY,
            },
        )

        if spawn:
            task.job_manager.updateProgress(
                message="Creating a Tale container", total=total, current=3
            )
            while instance["status"] == InstanceStatus.LAUNCHING:
                # TODO: Timeout? Raise error?
                time.sleep(1)
                instance = task.girder_client.get("/instance/{_id}".format(**instance))
        else:
            instance = None

        task.job_manager.updateProgress(
            message="Tale is ready!", total=total, current=total
        )
        # TODO: maybe filter results?
        return {"tale": tale, "instance": instance}

    def recorded_run(self, task, run_id, tale_id, entrypoint):
        raise NotImplementedError()

    def check_on_run(self, run_state):
        raise NotImplementedError()

    def cleanup_run(self, task, run_id):
        raise NotImplementedError()
