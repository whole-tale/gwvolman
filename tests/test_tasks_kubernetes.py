import mock
import pytest

from girder_client import GirderClient
from gwvolman.tasks_kubernetes import KubernetesTasks
from celery import Task


def mock_gc_patch(path, parameters=None):
    global status
    if path in "/run/123abc/status":
        status = parameters["status"]
        return 200


def mock_gc_get(path, parameters=None):
    if path == "/instance/instance_id":
        return {"_id": "instance_id", "taleId": "abc123"}
    elif path == "/tale/abc123":
        return {
            "_id": "abc123",
            "imageId": "imageId",
            "imageInfo": {},
            "digest": "imageDigest",
        }
    elif path == "/tale/abc123/restore":
        return {"_id": "abc123", "imageId": "abc123"}
    elif path == "/user/me":
        return {"_id": "user1_id", "login": "user1"}
    elif path == "/image/imageId":
        return {
            "_id": "imageId",
            "config": {
                "command": "jupyter notebook",
                "port": 8888,
                "targetMount": "/home/jovyan/work",
            },
        }
    elif path == "/run/123abc":
        return {"_id": "123abc", "name": "run1", "runVersionId": "xyz234"}


@pytest.fixture
def task_handler():
    with mock.patch("kubernetes.config.load_incluster_config"):
        return KubernetesTasks()


@pytest.fixture
def task():
    obj = mock.MagicMock(spec=Task)
    obj.girder_client = mock.MagicMock(spec=GirderClient)
    obj.girder_client.get = mock_gc_get
    obj.girder_client.patch = mock_gc_patch
    obj.girder_client.token = "girderToken"
    obj.job_manager = mock.MagicMock()
    return obj


@pytest.fixture
def mounts():
    return [{"name": "mount1"}, {"name": "mount2"}]


def test_create_volume(task_handler, task, mounts):
    instance_id = "instance_id"
    result = task_handler.create_volume(task, instance_id, mounts)
    assert result["instanceId"] == instance_id
    assert result["mounts"] == mounts


def test_remove_volume(task_handler):
    instance_id = "instance_id"
    result = task_handler.remove_volume(mock.Mock(), instance_id)
    assert result is None


def test_wait_for_pod(task_handler):
    instance_id = "instance_id"
    with mock.patch("kubernetes.client.CoreV1Api") as api_mock, mock.patch(
        "time.sleep"
    ):
        api_mock.return_value.list_namespaced_pod.side_effect = [
            mock.MagicMock(items=[]),
            mock.MagicMock(
                items=[
                    mock.MagicMock(
                        status=mock.MagicMock(
                            phase="Failed", reason="reason", message="message"
                        )
                    )
                ]
            ),
        ]
        with pytest.raises(Exception) as exc:
            task_handler._wait_for_pod(instance_id)
        assert str(exc.value) == "Pod %s failed. Reason: %s, message: %s" % (
            instance_id,
            "reason",
            "message",
        )

        api_mock.return_value.list_namespaced_pod.side_effect = [
            mock.MagicMock(
                items=[
                    mock.MagicMock(
                        status=mock.MagicMock(phase="Running", reason="reason")
                    )
                ]
            ),
        ]
        task_handler._wait_for_pod(instance_id)


def test_launch_container(task_handler, task, mounts):
    payload = {"instanceId": "instance_id", "mounts": mounts}
    task_handler._wait_for_pod = mock.Mock()
    with mock.patch("kubernetes.client.CoreV1Api") as api_mock, mock.patch(
        "kubernetes.config.load_incluster_config"
    ), mock.patch("gwvolman.tasks_kubernetes.stream") as stream_mock, mock.patch(
        "kubernetes.client.AppsV1Api"
    ) as apps_api_mock, mock.patch("kubernetes.client.NetworkingV1Api") as net_api_mock:
        api_mock.return_value.list_namespaced_pod.return_value.items = []
        result = task_handler.launch_container(task, payload)
        apps_api_mock.return_value.create_namespaced_deployment.assert_called_once()
        api_mock.return_value.create_namespaced_service.assert_called_once()
        net_api_mock.return_value.create_namespaced_ingress.assert_called_once()
        stream_mock.assert_called_once()
        assert result["instanceId"] == payload["instanceId"]


def test_shutdown_container(task_handler, task):
    instance_id = "instance_id"
    with mock.patch("kubernetes.client.AppsV1Api") as apps_api_mock, mock.patch(
        "kubernetes.client.CoreV1Api"
    ) as api_mock, mock.patch("kubernetes.client.NetworkingV1Api") as net_api_mock:
        apps_api_mock.return_value.list_namespaced_deployment.return_value.items = []
        task_handler.shutdown_container(task, instance_id)
        apps_api_mock.return_value.delete_namespaced_deployment.assert_not_called()

        deployment = mock.MagicMock()
        deployment.metadata.name = "deployment_name"
        apps_api_mock.return_value.list_namespaced_deployment.return_value.items = [
            deployment
        ]
        service = mock.MagicMock()
        service.metadata.name = "service_name"
        api_mock.return_value.list_namespaced_service.return_value.items = [service]
        ingress = mock.MagicMock()
        ingress.metadata.name = "ingress_name"
        net_api_mock.return_value.list_namespaced_ingress.return_value.items = [ingress]
        task_handler.shutdown_container(task, instance_id)
        apps_api_mock.return_value.delete_namespaced_deployment.assert_called_once()
        apps_api_mock.return_value.delete_namespaced_deployment.assert_called_with(
            name=deployment.metadata.name, namespace=task_handler.deployment.namespace
        )
        api_mock.return_value.delete_namespaced_service.assert_called_once()
        api_mock.return_value.delete_namespaced_service.assert_called_with(
            name=service.metadata.name, namespace=task_handler.deployment.namespace
        )
        net_api_mock.return_value.delete_namespaced_ingress.assert_called_once()
        net_api_mock.return_value.delete_namespaced_ingress.assert_called_with(
            name=ingress.metadata.name, namespace=task_handler.deployment.namespace
        )
