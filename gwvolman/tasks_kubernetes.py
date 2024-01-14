import logging
import time
import uuid

import kubernetes

from .constants import ENABLE_WORKSPACES
from .tasks_base import TasksBase
from .utils import (
    DOMAIN,
    K8SDeployment,
    _get_user_and_instance,
    new_user,
    _get_container_config,
)
from .utils_k8s import tale_deployment, tale_service, tale_ingress

CREATE_VOLUME_STEP_TOTAL = 2
LAUNCH_CONTAINER_STEP_TOTAL = 2
DMS_ENABLED = False


class KubernetesTasks(TasksBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        kubernetes.config.load_incluster_config()
        self.deployment = K8SDeployment()

    @staticmethod
    def _claim_from_volume(volume_name):
        return f"claim-{volume_name}"

    def create_volume(self, task, instance_id: str):
        user, instance = _get_user_and_instance(task.girder_client, instance_id)
        tale = task.girder_client.get("/tale/{taleId}".format(**instance))

        task.job_manager.updateProgress(
            message="Creating volume",
            total=CREATE_VOLUME_STEP_TOTAL,
            current=1,
            forceFlush=True,
        )

        vol_name = "_".join((tale["_id"], user["login"], new_user(6)))

        # We don't need to create a PVC for now...
        # _k8s_create_from_file(
        #    self.deployment.namespace,
        #    "templates/tale-volume-claim.yaml",
        #    {
        #        "claimName": self._claim_from_volume(vol_name),
        #        "instanceId": instance["_id"],
        #        "volumeSize": "1Gi",
        #    },
        # )

        return {
            "nodeId": None,
            "fscontainerId": None,
            "volumeName": vol_name,
            "instanceId": instance["_id"],
            "taleId": tale["_id"],
        }

    def _ensure_one(self, list, type, instanceId):
        if len(list) == 0:
            # TODO: use exception
            # The girder side happily creates the instance even if launch_container throws
            # an exception, which it shouldn't.
            # raise LookupError('No %s found for instanceId %s' % (type, instanceId))
            logging.warning("No %s found for instanceId %s" % (type, instanceId))
            return None
        if len(list) > 1:
            raise LookupError(
                "Multiple %s found for instanceId %s" % (type, instanceId)
            )

        return list[0]

    def remove_volume(self, task, instanceId):
        return

    def _wait_for_pod(self, instanceId):
        api = kubernetes.client.CoreV1Api()
        tic = time.time()
        timeout = 30.0

        # wait until task is started
        while time.time() - tic < timeout:
            pods = api.list_namespaced_pod(
                self.deployment.namespace, label_selector="instanceId=%s" % instanceId
            )
            if len(pods.items) == 0:
                logging.info("_wait_for_pod with instance id %s no match" % instanceId)
            elif len(pods.items) > 1:
                logging.error(
                    "_wait_for_pod %s multiple matches; this should not be happening"
                    % instanceId
                )
            else:
                pod = pods.items[0]
                if pod.status.phase == "Running":
                    return
                elif pod.status.phase == "Pending":
                    # Reset deadline
                    tic = time.time()
                elif pod.status.phase == "Failed":
                    raise Exception(
                        "Pod %s failed. Reason: %s, message: %s"
                        % (instanceId, pod.status.reason, pod.status.message)
                    )
            time.sleep(5)
        raise Exception("Pod %s startup timed out" % instanceId)

    def _configure_volumes(self, template_params, session, home_dir, tale):
        # DMS
        template_params["dmsMountEnabled"] = True
        template_params["dmsSessionId"] = str(session["_id"])

        # Home
        template_params["homeDirId"] = str(home_dir["_id"])

        # Workspace
        if ENABLE_WORKSPACES:
            template_params["workspacesEnabled"] = True
            template_params["taleId"] = str(tale["_id"])
        else:
            template_params["workspacesEnabled"] = False

    def _render_config(self, container_config):
        token = uuid.uuid4().hex
        # command
        if container_config.command:
            rendered_command = container_config.command.format(
                base_path="",
                port=container_config.container_port,
                ip="0.0.0.0",
                token=token,
            )
        else:
            rendered_command = None

        if container_config.url_path:
            rendered_url_path = container_config.url_path.format(token=token)
        else:
            rendered_url_path = ""
        return container_config._replace(
            command=rendered_command, url_path=rendered_url_path
        )

    def _add_ingress_rule(self, service_name, port, host):
        pass

    def launch_container(self, task, payload):
        """Launch a container using a Tale object."""
        instanceId = payload["instanceId"]
        user, instance = _get_user_and_instance(task.girder_client, instanceId)
        tale = task.girder_client.get("/tale/{taleId}".format(**instance))

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

        host = f"tmp{new_user(12).lower()}"
        deployment_name = "tale-" + host

        # Must match ingress host
        service_name = host

        girder_api_url = f"{self.deployment.girder_url}/api/v1"
        template_params = {
            "deploymentName": deployment_name,
            "host": host,
            "domain": DOMAIN,
            "deploymentNamespace": self.deployment.namespace,
            "claimName": "girder-data",  # TODO pass from deployment
            "girderApiUrl": girder_api_url,
            "mounterImage": self.deployment.mounter_image,
            "instanceId": instanceId,
            "girderToken": task.girder_client.token,
            "homeSubPath": f"homes/{user['login'][0]}/{user['login']}",
            "workspaceSubPath": f"workspaces/{tale['_id'][0]}/{tale['_id']}",
        }
        container_config = _get_container_config(task.girder_client, tale)

        container_config = self._render_config(container_config)
        self._render_config(container_config)
        template_params.update(
            {
                "command": container_config.command,
                "mountPoint": container_config.target_mount,
                "instancePort": container_config.container_port,
                "instanceImage": container_config.image,
            }
        )

        if DMS_ENABLED:
            session = self._create_session(task, tale)
            template_params["mountDms"] = (
                f"girderfs --api-url {girder_api_url} --token {task.girder_client.token}"
                f" -c wt_dms /data {session['_id']}"
            )
        else:
            template_params["mountDms"] = (
                "passthrough-fuse -o allow_other "
                f"--girder-url={girder_api_url}/tale/{tale['_id']}/listing "
                f"--token={task.girder_client.token} "
                "/data"
            )

        # create deployment and service
        tale_deployment(template_params)

        # wait until task is started
        self._wait_for_pod(instanceId)

        tale_service(template_params)
        tale_ingress(template_params)

        print("Environment is up and running.")
        task.job_manager.updateProgress(
            message="Container started",
            total=LAUNCH_CONTAINER_STEP_TOTAL,
            current=LAUNCH_CONTAINER_STEP_TOTAL,
            forceFlush=True,
        )

        payload["url"] = f"https://{host}.{DOMAIN}/{container_config.url_path}"
        payload["name"] = service_name
        return payload

    def shutdown_container(self, task, instanceId):
        """Shutdown a running Tale."""
        logging.info("Shutting down container for instance %s" % instanceId)
        api = kubernetes.client.AppsV1Api()
        deployments = api.list_namespaced_deployment(
            namespace=self.deployment.namespace,
            label_selector=f"instanceId={instanceId}",
        )

        deployment = self._ensure_one(deployments.items, "deployments", instanceId)
        if deployment is None:
            return

        api.delete_namespaced_deployment(
            name=deployment.metadata.name, namespace=self.deployment.namespace
        )

        api = kubernetes.client.CoreV1Api()
        services = api.list_namespaced_service(
            namespace=self.deployment.namespace,
            label_selector="instanceId=%s" % instanceId,
        )
        service = self._ensure_one(services.items, "services", instanceId)
        if service is not None:
            api.delete_namespaced_service(
                name=service.metadata.name, namespace=self.deployment.namespace
            )

        api = kubernetes.client.NetworkingV1Api()
        ingresses = api.list_namespaced_ingress(
            namespace=self.deployment.namespace,
            label_selector="instanceId=%s" % instanceId,
        )
        ingress = self._ensure_one(ingresses.items, "ingresses", instanceId)
        if ingress is not None:
            api.delete_namespaced_ingress(
                name=ingress.metadata.name, namespace=self.deployment.namespace
            )

    def _create_session(task, tale):
        raise NotImplementedError()
