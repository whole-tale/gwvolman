from unittest.mock import patch

from fastapi.testclient import TestClient
from gwvolman.remote_builder.server import app

client = TestClient(app)


@patch("gwvolman.remote_builder.server.client")
def test_pull_docker_r2d_image(mock_docker_client):
    mock_docker_client.api.pull.return_value = [
        b'{"status": "Pulling from some/repo", "progressDetail": {}}',
    ]
    response = client.put("/pull", params={"repo2docker_version": "some/repo:tag"})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"


@patch("gwvolman.remote_builder.server.client")
def test_push_tale_image(mock_docker_client):
    mock_docker_client.api.push.return_value = [
        b'{"status": "Pushing to some/repo", "progressDetail": {}}',
    ]
    response = client.put(
        "/push",
        params={
            "image": "some/repo:tag",
            "registry_url": "https://registry.example.com",
            "registry_user": "user",
            "registry_password": "password",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"


@patch("gwvolman.remote_builder.server.GirderClient")
@patch("gwvolman.remote_builder.server.client")
@patch("gwvolman.r2d.docker.DockerImageBuilder")
def test_build_tale(mock_docker_image_builder, mock_docker_client, mock_gc):
    mock_gc.return_value.get.return_value = {
        "_id": "some-tale-id",
        "imageId": "some-image-id",
        "workspaceId": "workspaceId",
        "config": {
            "targetMount": "/home/jovyan/work",
        },
    }
    mock_docker_client.logs.return_value = [
        b'{"stream": "Step 1/2 : FROM ubuntu:latest\\n"}',
    ]
    response = client.post(
        "/build",
        params={
            "taleId": "some-tale-id",
            "apiUrl": "https://girder.example.com/api/v1",
            "token": "some-token",
            "registry_url": "https://registry.example.com",
            "dry_run": "true",
            "tag": "some/repo:tag",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
