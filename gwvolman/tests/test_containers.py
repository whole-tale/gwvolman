import pytest
import docker
import mock

import girder_worker
from gwvolman.tasks import update_container
from girder_client import GirderClient


@pytest.fixture(scope="session")
def celery_config():
    return {"result_backend": "rpc"}


@mock.patch("docker.client.DockerClient.services")
def test_update_container(services):
    mock_service = mock.MagicMock()
    mock_service.attrs = {"ala": 1, "UpdateStatus": {"State": "completed"}}
    services.get.return_value = mock_service
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get.side_effect = ["user", {"containerInfo": {"name": "blah"}}]
    update_container.girder_client = mock_gc
    update_container.job_manager = mock.MagicMock()
    girder_worker.task.Task.canceled = mock.PropertyMock(return_value=False)

    task = update_container("123", digest="digest_hash")
    assert task == {"image_digest": "digest_hash"}
