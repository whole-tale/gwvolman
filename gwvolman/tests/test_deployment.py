# Write a pytest based test for ..utils.Deployment class
# mocking all docker.Client calls
import mock
import pytest

from gwvolman.utils import Deployment


@pytest.fixture
def deployment():
    return Deployment()


def test_tmpdir_mount(deployment):
    with mock.patch.object(deployment, "docker_client") as mock_docker_client:
        mock_docker_client.containers.get.return_value = mock.Mock()
        mock_docker_client.containers.get.return_value.attrs = {
            "Mounts": [
                {
                    "Destination": "/tmp",
                    "Source": "/blah/tmp",
                    "Type": "bind",
                    "Mode": "rw",
                    "RW": True,
                    "Propagation": "rprivate",
                }
            ]
        }
        assert deployment.tmpdir_mount == "/blah/tmp"


def test_traefik_network(deployment):
    with mock.patch.object(deployment, "docker_client") as mock_docker_client:
        mock_docker_client.services.get.return_value = mock.Mock()
        mock_docker_client.services.get.return_value.attrs = {
            "Spec": {"Labels": {"traefik.docker.network": "traefik"}}
        }
        assert deployment.traefik_network == "traefik"


def test_dashboard_url(deployment):
    with mock.patch.object(deployment, "docker_client") as mock_docker_client:
        mock_docker_client.services.get.return_value = mock.Mock()
        mock_docker_client.services.get.return_value.attrs = {
            "Spec": {
                "Labels": {
                    "com.docker.stack.namespace": "wt_traefik",
                    "traefik.http.routers.wt_dashboard.rule": "Host(`dashboard.example.com`)",
                }
            }
        }
        assert deployment.dashboard_url == "https://dashboard.example.com"


def test_girder_url(deployment):
    with mock.patch.object(deployment, "docker_client") as mock_docker_client:
        mock_docker_client.services.get.return_value = mock.Mock()
        mock_docker_client.services.get.return_value.attrs = {
            "Spec": {
                "Labels": {
                    "com.docker.stack.namespace": "wt_traefik",
                    "traefik.http.routers.wt_girder.rule": "Host(`girder.example.com`)",
                }
            }
        }
        assert deployment.girder_url == "https://girder.example.com"
