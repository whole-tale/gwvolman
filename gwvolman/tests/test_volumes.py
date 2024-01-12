from girder_client import GirderClient
import mock
import os

os.environ["GIRDER_API_URL"] = "https://girder.dev.wholetale.org/api/v1"

from gwvolman.tasks import create_volume  # noqa: E402


def mock_gc_post(path, parameters=None):
    if path in ("/dm/session"):
        if "taleId" in parameters:
            return {"_id": "session1"}
        elif "dataSet" in parameters:
            return {"_id": "session2"}


def mock_gc_get(path, parameters=None):
    if path in ("/instance/instance1"):
        return {"_id": "instance1", "taleId": "tale1"}
    elif path in ("/tale/tale1"):
        return {"_id": "tale1"}
    elif path in ("/user/me"):
        return {"_id": "ghi567", "login": "user1"}
    elif path in ("/version/version1/dataSet"):
        return {}


@mock.patch("gwvolman.tasks_docker.new_user", return_value="123456")
@mock.patch("os.mkdir", return_value=None)
@mock.patch(
    "docker.client.DockerClient.info", return_value={"Swarm": {"NodeID": "node1"}}
)
@mock.patch("gwvolman.tasks_docker._get_api_key", return_value="apikey1")
def test_create_volume(gak, info, osmk, nu):
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get = mock_gc_get
    mock_gc.loadOrCreateFolder = mock.Mock(return_value={"_id": "folder1"})

    create_volume.girder_client = mock_gc
    create_volume.job_manager = mock.MagicMock()

    with mock.patch("docker.from_env") as mock_docker, mock.patch(
        "requests.Session.post"
    ) as mock_post:
        mock_docker.return_value = mock.MagicMock()
        # mock docker info
        mock_docker.return_value.info.return_value = {"Swarm": {"NodeID": "node1"}}
        # define magick mock for the container
        mock_status = mock.PropertyMock(side_effect=["starting", "running", "running"])
        fscontainer = mock.MagicMock(
            id="container1",
            name="tale1_user1_123456",
        )
        type(fscontainer).status = mock_status
        mock_docker.return_value.containers.run.return_value = fscontainer

        ret = create_volume("instance1")

        data = {
            "mounts": [
                {"type": "data", "protocol": "girderfs", "location": "data"},
                {"type": "home", "protocol": "bind", "location": "home"},
                {"type": "workspace", "protocol": "bind", "location": "workspace"},
                {"type": "versions", "protocol": "girderfs", "location": "versions"},
                {"type": "runs", "protocol": "girderfs", "location": "runs"},
            ],
            "taleId": "tale1",
            "userId": "ghi567",
            "girderApiUrl": "https://girder.dev.wholetale.org/api/v1",
            "girderApiKey": "apikey1",
            "root": "tale1_user1_123456",
        }
        headers = {"Content-Type": "application/json"}
        mock_post.assert_called_with(
            f"http://{fscontainer.name}:8888/", json=data, headers=headers
        )

    expected = {
        "nodeId": "node1",
        "fscontainerId": "container1",
        "instanceId": "instance1",
        "taleId": "tale1",
        "volumeName": "tale1_user1_123456",
    }

    assert ret == expected
