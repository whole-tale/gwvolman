from girder_client import GirderClient
from docker import DockerClient, APIClient
from gwvolman.utils import ContainerConfig
import mock
import girder_worker
import os

from gwvolman.tasks import build_tale_image


class MockVolume:
    @property
    def id(self):
        return "abc"

    def remove(self):
        return True


def mock_gc_get(path, parameters=None):
    if path in ("/image/jupyter"):
        return {
            "_id": "jupyter",
            "config": {
                "buildpack": "JupyterBuildPack",
                "user": "jovyan",
            }
        }
    elif path in ("/image/stata"):
        return {
            "_id": "stata",
            "config": {
                "buildpack": "StataBuildPack",
                "user": "jovyan",
            }
        }
    elif path in ("/image/matlab"):
        return {
            "_id": "matlab",
            "config": {
                "buildpack": "MatlabBuildPack",
                "user": "jovyan",
            }
        }
    elif path in ("/folder/workspace1"):
        return {
            "_id": "workspace1",
            "updated": "2",
        }
    elif path in ("/tale/tale1"):
        return {
            "_id": "tale1",
            "imageId": "jupyter",
            "workspaceId": "workspace1",
            "status": 1,
            "imageInfo": {
                "last_build": 1,
                "imageId": "image1",
                "repo2docker_version": "wholetale/r2d_wt",
            }
        }
    elif path in ("/tale/tale2"):
        return {
            "_id": "tale2",
            "imageId": "stata",
            "workspaceId": "workspace1",
            "status": 1,
            "imageInfo": {
                "last_build": 1,
                "imageId": "stata",
                "repo2docker_version": "wholetale/r2d_wt",
            }
        }
    elif path in ("/tale/tale3"):
        return {
            "_id": "tale3",
            "imageId": "matlab",
            "workspaceId": "workspace1",
            "status": 1,
            "imageInfo": {
                "last_build": 1,
                "imageId": "matlab",
                "repo2docker_version": "wholetale/r2d_wt",
            }
        }
    elif path in ("/user/me"):
        return {"login": "user1"}


CONTAINER_CONFIG = ContainerConfig(
    buildpack="JupyterBuildPack",
    repo2docker_version="wholetale/repo2docker_wholetale:latest",
    image="image1",
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

JUPYTER_R2D_CALL = mock.call(
    image='wholetale/repo2docker_wholetale:latest',
    command="jupyter-repo2docker --config='/wholetale/repo2docker_config.py'"
            " --target-repo-dir='/home/jovyan/work/workspace'"
            " --user-id=1000 --user-name=jovyan --no-clean --no-run --debug"
            "  --image-name registry.test.wholetale.org/tale1/1624994605 /tmp/xxx",
    environment=['DOCKER_HOST=unix:///var/run/docker.sock'],
    privileged=True,
    detach=True,
    remove=True,
    volumes={
        '/var/run/docker.sock': {
            'bind': '/var/run/docker.sock',
            'mode': 'rw'
        },
        '/tmp': {
            'bind': '/host/tmp',
            'mode': 'ro'
        }
    }
)

STATA_R2D_CALL = mock.call(
    image='wholetale/repo2docker_wholetale:latest',
    command="jupyter-repo2docker --config='/wholetale/repo2docker_config.py'"
            " --target-repo-dir='/home/jovyan/work/workspace'"
            " --user-id=1000 --user-name=jovyan --no-clean --no-run --debug"
            "  --build-arg STATA_LICENSE_ENCODED='dGhpcyBpcyBhIGZha2Ugc3RhdGEgbGljZW5zZQo='"
            "  --image-name registry.test.wholetale.org/tale2/1624994605 /tmp/xxx",
    environment=['DOCKER_HOST=unix:///var/run/docker.sock'],
    privileged=True,
    detach=True,
    remove=True,
    volumes={
        '/var/run/docker.sock': {
            'bind': '/var/run/docker.sock',
            'mode': 'rw'
        },
        '/tmp': {
            'bind': '/host/tmp',
            'mode': 'ro'
        }
    }
)

MATLAB_R2D_CALL = mock.call(
    image='wholetale/repo2docker_wholetale:latest',
    command="jupyter-repo2docker --config='/wholetale/repo2docker_config.py'"
            " --target-repo-dir='/home/jovyan/work/workspace'"
            " --user-id=1000 --user-name=jovyan --no-clean --no-run --debug"
            "  --build-arg FILE_INSTALLATION_KEY=fake-matlab-key"
            "  --image-name registry.test.wholetale.org/tale3/1624994605 /tmp/xxx",
    environment=['DOCKER_HOST=unix:///var/run/docker.sock'],
    privileged=True,
    detach=True,
    remove=True,
    volumes={
        '/var/run/docker.sock': {
            'bind': '/var/run/docker.sock',
            'mode': 'rw'
        },
        '/tmp': {
            'bind': '/host/tmp',
            'mode': 'ro'
        }
    }
)


@mock.patch.dict(os.environ, {"MATLAB_FILE_INSTALLATION_KEY": "fake-matlab-key"})
@mock.patch("base64.b64encode", return_value=bytes("dGhpcyBpcyBhIGZha2Ugc3RhdGEgbGljZW5zZQo=",
            encoding='ascii'))
@mock.patch("time.time", return_value=1624994605)
@mock.patch("docker.APIClient")
@mock.patch("builtins.open", new_callable=mock.mock_open(), read_data=bytes("blah",
            encoding="ascii"))
@mock.patch("tempfile.mkdtemp", return_value="/tmp/xxx")
@mock.patch("docker.DockerClient.containers")
@mock.patch("docker.DockerClient.images")
@mock.patch("docker.DockerClient.login")
@mock.patch("shutil.rmtree", return_value=True)
@mock.patch("gwvolman.tasks._get_container_config", return_value=CONTAINER_CONFIG)
def test_build_tale_image(gcc, sh, dcl, dci, containers, tf, op, ac, time, b64):

    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get = mock_gc_get
    mock_gc.downloadFolderRecursive.return_value = True

    build_tale_image.girder_client = mock_gc
    build_tale_image.cli = mock.MagicMock(spec=DockerClient)
    build_tale_image.cli.images.pull.return_value = True
    build_tale_image.apicli = mock.MagicMock(spec=APIClient)
    build_tale_image.job_manager = mock.MagicMock()
    girder_worker.task.Task.canceled = mock.PropertyMock(return_value=False)

    containers.run.return_value.wait.return_value = {"StatusCode": 0}

    try:
        with mock.patch("gwvolman.utils.Deployment.registry_url",
                        new_callable=mock.PropertyMock) as mock_dep:

            mock_dep.return_value = "https://registry.test.wholetale.org"

            build_tale_image("tale1", force=False)
            build_tale_image("tale2", force=False)
            build_tale_image("tale3", force=False)

    except ValueError:
        assert False

    containers.run.assert_has_calls(
        [JUPYTER_R2D_CALL, mock.call().logs(stream=True),
         mock.call().logs().__iter__(), mock.call().wait()])

    containers.run.assert_has_calls(
        [STATA_R2D_CALL, mock.call().logs(stream=True),
         mock.call().logs().__iter__(), mock.call().wait()])

    containers.run.assert_has_calls(
        [MATLAB_R2D_CALL, mock.call().logs(stream=True),
         mock.call().logs().__iter__(), mock.call().wait()])
