from girder_client import GirderClient
import mock

from gwvolman.tasks import recorded_run
from gwvolman.utils import ContainerConfig
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
    elif path in ("/user/me"):
        return {"login": "user1"}
    elif path in "/run/123abc":
        return {"_id": "123abc", "name": "run1", "runVersionId": "xyz234"}


CONTAINER_CONFIG = ContainerConfig(
    buildpack="JupyterBuildPack",
    repo2docker_version="wholetale/repo2docker_wholetale:latest",
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
    command='bash -c "mkdir -p .wholetale/.reprozip-trace ;reprozip trace '
            '--dir .wholetale/.reprozip-trace --overwrite ./run.sh"',
    environment=['DOCKER_HOST=unix:///var/run/docker.sock'],
    cap_add = ['SYS_PTRACE'],
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
@mock.patch("subprocess.call", return_value=True)
@mock.patch("os.remove", return_value=True)
@mock.patch("gwvolman.tasks._build_image", return_value={"StatusCode": 0})
@mock.patch("gwvolman.tasks._get_container_config", return_value=CONTAINER_CONFIG)
@mock.patch("gwvolman.tasks._get_api_key", return_value="key123")
@mock.patch("gwvolman.tasks._write_env_json", return_value="/path/to/environment.json")
@mock.patch("gwvolman.tasks._get_session", return_value={"_id": None})
@mock.patch("gwvolman.tasks._create_docker_volume", return_value="/path/to/mountpoint/")
@mock.patch("gwvolman.tasks._make_fuse_dirs", return_value=True)
@mock.patch("gwvolman.tasks._mount_girderfs", return_value=True)
def test_recorded_run(mg, mfd, cdv, gs, wej, gak, gcc, bi, osr, sp, containers, volumes, time):

    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get = mock_gc_get
    mock_gc.patch = mock_gc_patch

    recorded_run.girder_client = mock_gc
    recorded_run.job_manager = mock.MagicMock()

    volumes.get.return_value = MockVolume()

    # This should succeed
    containers.run.return_value.wait.return_value = {"StatusCode": 0}
    try:
        with mock.patch('gwvolman.utils.Deployment.registry_url', new_callable=mock.PropertyMock) as mock_dep:
            mock_dep.return_value = 'https://registry.test.wholetale.org'
            recorded_run("123abc", "abc123")
            assert status == RunStatus.COMPLETED
    except ValueError:
        assert False

    containers.run.assert_has_calls([RPZ_RUN_CALL, CPR_RUN_CALL], any_order=True)

    # This should fail
    containers.run.return_value.wait.return_value = {"StatusCode": 1}
    try:
        with mock.patch('gwvolman.utils.Deployment.registry_url', new_callable=mock.PropertyMock) as mock_dep:
            mock_dep.return_value = 'https://registry.test.wholetale.org'
            recorded_run("123abc", "abc123")
    except ValueError:
        assert True
        assert status == RunStatus.FAILED

    containers.run.assert_has_calls([RPZ_RUN_CALL], any_order=True)
