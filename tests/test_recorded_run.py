from girder_client import GirderClient
import mock
import os
import pytest

from gwvolman.utils import ContainerConfig
from gwvolman.tasks import recorded_run
from gwvolman.constants import RunStatus, VOLUMES_ROOT


_mount_point = os.path.join(VOLUMES_ROOT, "mountpoints", "123abc_user1_123456")


def mock_gc_patch(path, parameters=None):
    global status
    if path in "/run/123abc/status":
        status = parameters["status"]
        return 200


def mock_gc_get(path, parameters=None):
    if path in ("/image/abc123"):
        return {"_id": "abc123", "buildPack": "JupyterBuildPack"}
    elif path in ("/tale/abc123"):
        return {"_id": "abc123", "imageId": "abc123"}
    elif path in ("/tale/abc123/restore"):
        return {"_id": "abc123", "imageId": "abc123"}
    elif path in ("/user/me"):
        return {"_id": "user1_id", "login": "user1"}
    elif path in "/run/123abc":
        return {"_id": "123abc", "name": "run1", "runVersionId": "xyz234"}


CONTAINER_CONFIG = ContainerConfig(
    buildpack="JupyterBuildPack",
    repo2docker_version="wholetale/repo2docker_wholetale:v1.2",
    image="abc123",
    command="test",
    mem_limit=2,
    cpu_shares=1,
    container_port=8080,
    container_user="jovyan",
    target_mount="/work",
    url_path="",
    environment=[],
    csp="",
)

RPZ_RUN_CALL = mock.call(
    image="registry.test.wholetale.org/123abc/1624994605",
    command="sh entrypoint.sh",
    detach=True,
    name="rrun-123456",
    volumes={
        os.path.join(_mount_point, "data"): {"bind": "/work/data", "mode": "ro"},
        os.path.join(_mount_point, "workspace"): {
            "bind": "/work/workspace",
            "mode": "rw",
        },
    },
    working_dir="/work/workspace",
)
CPR_RUN_CALL = mock.call(
    image="wholetale/wt-cpr:latest",
    command='bash -c "/cpr/bin/run_reports.sh /work/workspace"',
    environment=["DOCKER_HOST=unix:///var/run/docker.sock"],
    detach=True,
    remove=True,
    volumes={
        "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
        "/tmp": {"bind": "/tmp", "mode": "ro"},
        os.path.join(_mount_point, "data"): {"bind": "/work/data", "mode": "rw"},
        os.path.join(_mount_point, "workspace"): {
            "bind": "/work/workspace",
            "mode": "rw",
        },
    },
)

@mock.patch("time.time", return_value=1624994605)
@mock.patch("gwvolman.tasks_docker.new_user", return_value="123456")
@mock.patch("docker.client.DockerClient.containers")
@mock.patch("subprocess.Popen")
@mock.patch("subprocess.check_call")
@mock.patch("os.remove", return_value=True)
@mock.patch("gwvolman.tasks_docker._get_container_config", return_value=CONTAINER_CONFIG)
@mock.patch("gwvolman.tasks_docker._get_api_key", return_value="key123")
@mock.patch("gwvolman.tasks_docker.DockerImageBuilder")
@mock.patch(
    "girder_worker.app.Task.canceled",
    new_callable=mock.PropertyMock,
    return_value=False,
)
def test_recorded_run(
    task, image_builder, gak, gcc, osr, spcc, sp, containers, nu, time
):
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get = mock_gc_get
    mock_gc.patch = mock_gc_patch

    # This should succeed
    image_builder.return_value.run_r2d.return_value = ({"StatusCode": 0}, "")
    image_builder.return_value.container_config.target_mount = "/work"
    image_builder.return_value.dh.cli.containers.get.return_value.wait.return_value = {
        "StatusCode": 0
    }
    image_builder.return_value.dh.cli.containers.create.return_value.id = "container_id"
    image_builder.return_value.dh.cli.containers.get.return_value.id = "container_id"
    image_builder.return_value.get_tag.return_value = (
        "registry.test.wholetale.org/123abc/1624994605"
    )

    with mock.patch(
        "gwvolman.utils.DockerDeployment.registry_url", new_callable=mock.PropertyMock
    ) as mock_dep, mock.patch("builtins.open", mock.mock_open()), mock.patch(
        "docker.from_env", return_value=mock.MagicMock()
    ) as mock_docker, mock.patch(
        "requests.Session.post", return_value=mock.MagicMock()
    ) as mock_post, mock.patch(
        "requests.delete", return_value=mock.MagicMock()
    ) as mock_delete:
        mock_status = mock.PropertyMock(side_effect=["starting", "running", "running"])
        fscontainer = mock.MagicMock(
            id="container1",
            name="123abc_user1_123456",
        )
        type(fscontainer).status = mock_status
        mock_docker.return_value.containers.run.return_value = fscontainer
        mock_docker.return_value.containers.get.return_value = fscontainer

        mock_dep.return_value = "https://registry.test.wholetale.org"
        recorded_run.girder_client = mock_gc
        recorded_run.job_manager = mock.MagicMock()
        recorded_run("123abc", "abc123", "entrypoint.sh")
        assert status == RunStatus.COMPLETED
        mock_post.assert_called_once_with(
            f"http://{fscontainer.name}:8888/",
            json={
                "mounts": [
                    {"type": "data", "protocol": "girderfs", "location": "data"},
                    {"type": "run", "protocol": "bind", "location": "workspace"},
                ],
                "girderApiUrl": "https://girder.dev.wholetale.org/api/v1",
                "girderApiKey": "key123",
                "root": "123abc_user1_123456",
                "taleId": "abc123",
                "runId": "123abc",
                "userId": "user1_id",
            },
            headers={"Content-Type": "application/json"},
        )
        mock_delete.assert_called_once_with(f"http://{fscontainer.name}:8888/")

    image_builder.return_value.dh.cli.containers.create.assert_has_calls(
        [RPZ_RUN_CALL], any_order=True
    )

    # Test execution failure
    image_builder.return_value.dh.cli.containers.get.return_value.wait.side_effect = (
        ValueError("foo")
    )

    with pytest.raises(ValueError):
        with mock.patch(
            "gwvolman.utils.DockerDeployment.registry_url", new_callable=mock.PropertyMock
        ) as mock_dep, mock.patch("builtins.open", mock.mock_open()), mock.patch(
            "docker.from_env", return_value=mock.MagicMock()
        ) as mock_docker, mock.patch(
            "requests.Session.post", return_value=mock.MagicMock()
        ) as mock_post, mock.patch(
            "requests.delete", return_value=mock.MagicMock()
        ) as mock_delete:
            mock_status = mock.PropertyMock(
                side_effect=["starting", "running", "running"]
            )
            fscontainer = mock.MagicMock(
                id="container1",
                name="123abc_user1_123456",
            )
            type(fscontainer).status = mock_status
            mock_docker.return_value.containers.run.return_value = fscontainer
            mock_docker.return_value.containers.get.return_value = fscontainer

            mock_dep.return_value = "https://registry.test.wholetale.org"
            recorded_run("123abc", "abc123", "entrypoint.sh")

    image_builder.return_value.dh.cli.containers.create.assert_has_calls(
        [RPZ_RUN_CALL], any_order=True
    )
