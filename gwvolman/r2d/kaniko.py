import hashlib
import logging
import os
import threading
import time

from kubernetes import client, config

from ..constants import NAMESPACE, REPO2DOCKER_VERSION
from ..utils import (
    DOMAIN,
    DummyTask,
    new_user,
)
from .builder import ImageBuilderBase


def create_configmap(api_instance, configmap_name, data):
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": configmap_name},
        "data": data,
    }
    api_instance.create_namespaced_config_map(namespace=NAMESPACE, body=body)


def get_pod_logs(api_instance, pod_name, container_name, state):
    # wait for the pod to start
    timeout = 60 * 5
    start_time = time.time()
    while True:
        try:
            pod = api_instance.read_namespaced_pod(name=pod_name, namespace=NAMESPACE)
            if pod.status.phase in ("Running", "Succeeded"):
                break
            elif pod.status.phase == "Failed":
                print("Pod failed to start")
                return
            time.sleep(0.5)
            if timeout and time.time() - start_time > timeout:
                print("Timed out waiting for pod to start")
                return
        except client.exceptions.ApiException as e:
            if e.status != 404:
                print("Exception when calling CoreV1Api->read_namespaced_pod: %s\n" % e)
            pass

    try:
        pod_logs = api_instance.read_namespaced_pod_log(
            name=pod_name,
            namespace=NAMESPACE,
            container="r2d",
            follow=True,
            _preload_content=False,
        )
        for line in pod_logs:
            output = line.decode("utf-8").strip()
            if not (
                output.startswith("Using local repo")
                or output.startswith("[Repo2Docker]")
            ):
                state["state"].update(output.encode("utf-8"))
            if not state["dry_run"]:
                print(output, end="\n")
    except client.exceptions.ApiException as e:
        if e.status != 404:
            print("Exception when calling CoreV1Api->read_namespaced_pod_log: %s\n" % e)
        pass
    except Exception:
        pass


class KanikoImageBuilder(ImageBuilderBase):
    def __init__(self, gc, imageId=None, tale=None, auth=True):
        super().__init__(gc, imageId=imageId, tale=tale, auth=auth)

    def pull_r2d(self):
        pass

    def push_image(self, image):
        pass

    @property
    def engine(self):
        return (
            "--engine=kaniko "
            f"--KanikoEngine.registry_credentials=registry=registry.{DOMAIN} "
            "--KanikoEngine.registry_credentials=username=fido "
            "--KanikoEngine.registry_credentials=password=secretpass "
            f"--KanikoEngine.cache_registry=registry.{DOMAIN}/cache "
            "--KanikoEngine.cache_registry_credentials=username=fido "
            "--KanikoEngine.cache_registry_credentials=password=secretpass"
        )

    @staticmethod
    def _cleanup(pod_name, job_name, configmap_name, batch_api_instance, api_instance):
        batch_api_instance.delete_namespaced_job(name=job_name, namespace=NAMESPACE)
        api_instance.delete_namespaced_config_map(
            name=configmap_name, namespace=NAMESPACE
        )
        api_instance.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE)

    def run_r2d(self, tag, dry_run=False, task=None):
        task = task or DummyTask
        suffix = new_user(8).lower()
        # Load Kubernetes configuration
        # config.load_kube_config()

        # Load in-cluster configuration
        config.load_incluster_config()

        # Define the directory to be uploaded
        local_directory_path = self.build_context
        configmap_name = f"job-configmap-{suffix}"

        # Read the contents of the local directory and create a ConfigMap
        # TODO: handle subdirectories
        data = {}
        for file_name in os.listdir(local_directory_path):
            file_path = os.path.join(local_directory_path, file_name)
            if os.path.isfile(file_path):
                with open(file_path, "r") as file:
                    data[file_name] = file.read()
            else:
                print(f"Skipping {file_path} as it is not a file.")

        # Create a ConfigMap with the contents of the local directory
        api_instance = client.CoreV1Api()
        create_configmap(api_instance, configmap_name, data)

        # Define Job manifest
        job_name = f"r2d-job-{suffix}"
        cmd = (
            f"mkdir -p {local_directory_path} && "
            f"cp -Lr /data/* {local_directory_path} && "
            f"{self.r2d_command(tag, dry_run=dry_run)}"
        )
        docker_secret_name = os.environ.get(
            "DOCKER_PULL_SECRET", "xarth-dockerhub-creds"
        )
        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name},
            "spec": {
                "template": {
                    "metadata": {"name": "r2d"},
                    "spec": {
                        "containers": [
                            {
                                "name": "r2d",
                                "image": REPO2DOCKER_VERSION,
                                "command": ["/bin/bash", "-c", cmd],
                                "lifecycle": {
                                    "postStart": {
                                        "exec": {
                                            "command": [
                                                "/bin/sh",
                                                "-c",
                                                (
                                                    "echo $TRAEFIK_SERVICE_HOST registry.local.wholetale.org >> /etc/hosts && "
                                                    "mkdir -p /root/.docker && "
                                                    "cp /secrets/config.json /root/.docker/config.json"
                                                ),
                                            ]
                                        }
                                    }
                                },
                                "volumeMounts": [
                                    {
                                        "name": "job-volume",
                                        "mountPath": "/data",
                                    },
                                    {
                                        "name": "docker-secret",
                                        "mountPath": "/secrets/",
                                    },
                                ],
                                "workingDir": local_directory_path,  # Set working directory
                            }
                        ],
                        "imagePullSecrets": [{"name": docker_secret_name}],
                        "restartPolicy": "Never",
                        "volumes": [
                            {
                                "name": "job-volume",
                                "configMap": {"name": configmap_name},
                            },
                            {
                                "name": "docker-secret",
                                "secret": {
                                    "secretName": docker_secret_name,
                                    "items": [
                                        {
                                            "key": ".dockerconfigjson",
                                            "path": "config.json",
                                        }
                                    ],
                                },
                            },
                        ],
                    },
                },
                "backoffLimit": 1,
            },
        }

        # Create Job
        batch_api_instance = client.BatchV1Api()
        logging.info(f"Creating a r2d job in namespace '{NAMESPACE}'")
        batch_api_instance.create_namespaced_job(namespace=NAMESPACE, body=job_manifest)

        # Wait for the Job to complete
        logging.info("Waiting for the r2d job to complete... (5s)")
        time.sleep(5)
        logging.info("Listing associated pods")
        pods = api_instance.list_namespaced_pod(
            namespace=NAMESPACE, label_selector=f"job-name={job_name}"
        )
        pod_name = pods.items[0].metadata.name
        container_name = "r2d"
        logging.info(f"Pod name: {pod_name}")
        try:
            # Print Job logs while waiting
            logging.info("Starting logs thread (dry_run=%s)" % dry_run)
            state = {"state": hashlib.md5("R2D output".encode()), "dry_run": dry_run}
            logging.info("Printing logs (in a thread)")
            pod_thread = threading.Thread(
                target=get_pod_logs,
                args=(api_instance, pod_name, container_name, state),
            )
            pod_thread.start()

            while True:
                if task.canceled:
                    logging.info("Job canceled by user")
                    ret = {"StatusCode": -123, "error": "Canceled by user"}
                    self._cleanup(
                        pod_name,
                        job_name,
                        configmap_name,
                        batch_api_instance,
                        api_instance,
                    )
                    logging.info("Cleanup complete")
                    break

                job_status = batch_api_instance.read_namespaced_job_status(
                    name=job_name, namespace=NAMESPACE
                )
                if (
                    job_status.status.succeeded is not None
                    and job_status.status.succeeded > 0
                ):
                    logging.info("Job succeeded")
                    self._cleanup(
                        pod_name,
                        job_name,
                        configmap_name,
                        batch_api_instance,
                        api_instance,
                    )
                    logging.info("Cleanup complete")
                    ret = {"StatusCode": 0}
                    break
                elif (
                    job_status.status.failed is not None
                    and job_status.status.failed > 0
                ):
                    ret = {"StatusCode": -1, "error": "Job failed"}
                    break
                time.sleep(5)
        finally:
            # Stop the pod logs thread when done
            logging.info("Stopping logs thread")
            pod_thread.join()
            logging.info("Logs thread stopped")

        return ret, state["state"].hexdigest()
