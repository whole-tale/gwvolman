from girder_client import GirderClient
import mock
import pytest

from gwvolman.utils import ContainerConfig
from gwvolman.tasks import recorded_run, _write_env_json
from gwvolman.constants import RunStatus


class MockVolume:
    @property
    def id(self):
        return "abc"

    def remove(self):
        return True


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
        return {"login": "user1"}
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
    csp=""
)

RPZ_RUN_CALL = mock.call(
    image='registry.test.wholetale.org/123abc/1624994605',
    command="sh entrypoint.sh",
    detach=True,
    volumes={
        '/path/to/mountpoint/data': {'bind': '/work/data', 'mode': 'rw'},
        '/path/to/mountpoint/workspace': {'bind': '/work/workspace', 'mode': 'rw'}
    }
)
CPR_RUN_CALL = mock.call(
    image='wholetale/wt-cpr:latest',
    command='bash -c "/cpr/bin/run_reports.sh /work/workspace"',
    environment=['DOCKER_HOST=unix:///var/run/docker.sock'],
    detach=True,
    remove=True,
    volumes={
        '/var/run/docker.sock': {
            'bind': '/var/run/docker.sock', 'mode': 'rw'
        },
        '/tmp': {
            'bind': '/host/tmp', 'mode': 'ro'
        },
        '/path/to/mountpoint/data': {
            'bind': '/work/data', 'mode': 'rw'
        },
        '/path/to/mountpoint/workspace': {
            'bind': '/work/workspace', 'mode': 'rw'
        }
    }
)


@mock.patch("time.time", return_value=1624994605)
@mock.patch("docker.client.DockerClient.volumes")
@mock.patch("docker.client.DockerClient.containers")
@mock.patch("subprocess.Popen")
@mock.patch("subprocess.check_call")
@mock.patch("os.remove", return_value=True)
@mock.patch("gwvolman.tasks._get_container_config", return_value=CONTAINER_CONFIG)
@mock.patch("gwvolman.tasks._get_api_key", return_value="key123")
@mock.patch("gwvolman.tasks._write_env_json", return_value="/path/to/environment.json")
@mock.patch("gwvolman.tasks._get_session", return_value={"_id": None})
@mock.patch("gwvolman.tasks._create_docker_volume", return_value="/path/to/mountpoint/")
@mock.patch("gwvolman.tasks._make_fuse_dirs", return_value=True)
@mock.patch("gwvolman.tasks._mount_girderfs", return_value=True)
@mock.patch("gwvolman.tasks.ImageBuilder")
def test_recorded_run(
    image_builder,
    mg,
    mfd,
    cdv,
    gs,
    wej,
    gak,
    gcc,
    osr,
    spcc,
    sp,
    containers,
    volumes,
    time
):
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get = mock_gc_get
    mock_gc.patch = mock_gc_patch

    volumes.get.return_value = MockVolume()

    # This should succeed
    image_builder.return_value.run_r2d.return_value = ({"StatusCode": 0}, "")
    image_builder.return_value.container_config.target_mount = "/work"
    image_builder.return_value.dh.cli.containers.get.return_value.wait.return_value = \
        {"StatusCode": 0}
    image_builder.return_value.dh.cli.containers.create.return_value.id = \
        "container_id"
    image_builder.return_value.dh.cli.containers.get.return_value.id = \
        "container_id"
    image_builder.return_value.get_tag.return_value = \
        "registry.test.wholetale.org/123abc/1624994605"

    try:
        with mock.patch(
            'gwvolman.utils.Deployment.registry_url', new_callable=mock.PropertyMock
        ) as mock_dep, mock.patch('builtins.open', mock.mock_open()) as bo:
            mock_dep.return_value = 'https://registry.test.wholetale.org'
            recorded_run.girder_client = mock_gc
            recorded_run.job_manager = mock.MagicMock()
            recorded_run("123abc", "abc123", "entrypoint.sh")
            assert status == RunStatus.COMPLETED
    except ValueError:
        raise AssertionError

    image_builder.return_value.dh.cli.containers.create.assert_has_calls(
        [RPZ_RUN_CALL], any_order=True
    )
    sp.assert_has_calls(
        [
            mock.call(
                [
                    "/host/usr/bin/docker",
                    "stats",
                    "--format",
                    '"{{.CPUPerc}},{{.MemUsage}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}"',
                    "container_id",
                ],
                stdout=-1,
                universal_newlines=True,
            ),
            mock.call(
                ["ts", '"%Y-%m-%dT%H:%M:%.S"'],
                stdin=sp.return_value.stdout,
                stdout=bo.return_value,
            )
        ],
        any_order=True,
    )

    # Test execution failure
    image_builder.return_value.dh.cli.containers.get.return_value.wait.side_effect = \
        ValueError("foo")

    with pytest.raises(ValueError):
        with mock.patch(
            'gwvolman.utils.Deployment.registry_url', new_callable=mock.PropertyMock
        ) as mock_dep, mock.patch('builtins.open', mock.mock_open()):
            mock_dep.return_value = 'https://registry.test.wholetale.org'
            recorded_run("123abc", "abc123", "entrypoint.sh")

    image_builder.return_value.dh.cli.containers.create.assert_has_calls(
        [RPZ_RUN_CALL], any_order=True
    )


def test_write_env_json():
    mock_image = {
        '_id': 'image1',
        'name': 'Mock Image'
    }

    workspace_dir = '/path/to/mountpoint/workspace'

    with mock.patch('builtins.open', mock.mock_open()):
        env_json = _write_env_json(workspace_dir, mock_image)

        assert env_json == '/host/path/to/mountpoint/workspace/environment.json'
