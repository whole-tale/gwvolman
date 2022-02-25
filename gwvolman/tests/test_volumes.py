from girder_client import GirderClient
import mock
import os

os.environ['GIRDER_API_URL'] = 'https://girder.dev.wholetale.org/api/v1'

from gwvolman.tasks import create_volume, _mount_girderfs, \
    _make_fuse_dirs, _create_docker_volume, _get_session # noqa


class MockVolume:
    @property
    def id(self):
        return "abc"

    @property
    def name(self):
        return "vol1"

    def remove(self):
        return True

    @property
    def attrs(self):
        return {
            'Mountpoint': '/var/lib/docker/volumes/vol1'
        }


def mock_gc_post(path, parameters=None):
    if path in ("/dm/session"):
        if 'taleId' in parameters:
            return {"_id": "session1"}
        elif 'dataSet' in parameters:
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


@mock.patch("gwvolman.tasks.new_user", return_value="123456")
@mock.patch("docker.client.DockerClient.info", return_value={'Swarm': {'NodeID': 'node1'}})
@mock.patch("docker.client.DockerClient.volumes")
@mock.patch("gwvolman.tasks._get_api_key", return_value="apikey1")
@mock.patch("gwvolman.tasks._get_session", return_value={"_id": "session1"})
@mock.patch("gwvolman.tasks._create_docker_volume", return_value="/path/to/mountpoint/")
@mock.patch("gwvolman.tasks._make_fuse_dirs", return_value=True)
@mock.patch("gwvolman.tasks._mount_girderfs", return_value=True)
def test_create_volume(mgfs, mfd, cdv, gs, gak, volumes, info, nu):

    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get = mock_gc_get
    mock_gc.loadOrCreateFolder = mock.Mock(return_value={'_id': 'folder1'})

    create_volume.girder_client = mock_gc
    create_volume.job_manager = mock.MagicMock()

    volumes.get.return_value = MockVolume()

    try:
        ret = create_volume('instance1')

        expected = {
            'nodeId': 'node1',
            'mountPoint': '/path/to/mountpoint/',
            'volumeName': 'tale1_user1_123456',
            'sessionId': 'session1',
            'instanceId': 'instance1',
            'taleId': "tale1",
        }

        assert ret == expected
    except ValueError:
        assert False

    mgfs.assert_has_calls([
        mock.call('/path/to/mountpoint/', 'data', 'wt_dms', 'session1',
                  'apikey1', hostns=True),
        mock.call('/path/to/mountpoint/', 'home', 'wt_home', 'folder1', 'apikey1'),
        mock.call('/path/to/mountpoint/', 'workspace', 'wt_work', 'tale1', 'apikey1'),
        mock.call('/path/to/mountpoint/', 'versions', 'wt_versions', 'tale1',
                  'apikey1', hostns=True),
        mock.call('/path/to/mountpoint/', 'runs', 'wt_runs', 'tale1', 'apikey1',
                  hostns=True)
        ], any_order=False)


@mock.patch("subprocess.check_call", return_value=True)
def test_mount_girderfs(spcc):

    _mount_girderfs('/path/to/mountpoint', 'home', 'wt_home', 'folder1', 'apikey1')

    _mount_girderfs('/path/to/mountpoint', 'data', 'wt_dms', 'session1', 'apikey1', True)

    spcc.assert_has_calls([
        mock.call('girderfs  -c wt_home --api-url https://girder.dev.wholetale.org/api/v1'
                  ' --api-key apikey1 /path/to/mountpoint/home folder1', shell=True),
        mock.call('girderfs --hostns -c wt_dms --api-url https://girder.dev.wholetale.org/api/v1'
                  ' --api-key apikey1 /path/to/mountpoint/data session1', shell=True)
        ], any_order=False)


@mock.patch("os.makedirs", return_value=True)
@mock.patch("os.mkdir", return_value=True)
def test_make_fuse_dirs(mkdir, makedirs):

    with mock.patch('os.path.isdir', return_value=True):
        _make_fuse_dirs('/path/to/mountpoint', ['dir1'])

    mkdir.assert_has_calls([mock.call('/host/path/to/mountpoint/dir1')], any_order=False)

    with mock.patch('os.path.isdir', return_value=False):
        _make_fuse_dirs('/path/to/mountpoint', ['dir2'])

    makedirs.assert_has_calls([mock.call('/path/to/mountpoint/dir2')], any_order=False)


@mock.patch("builtins.open", mock.mock_open(read_data="overlay"))
@mock.patch("os.chown", return_value=True)
@mock.patch("docker.client.DockerClient")
def test_create_docker_volume(cli, chown):

    cli.volumes.create.return_value = MockVolume()

    with mock.patch('os.walk') as mock_walk:
        mock_walk.return_value = [('/var/lib/docker/volumes', ('vol1',), ('',))]

        mountpoint = _create_docker_volume(cli, 'vol1')
        assert mountpoint == '/var/lib/docker/volumes/vol1'
    chown.assert_has_calls([
        mock.call('/host/var/lib/docker/volumes/vol1', 1000, 100),
        mock.call('/var/lib/docker/volumes/vol1', 1000, 100),
        mock.call('/var/lib/docker/volumes/', 1000, 100)
    ])


def test_get_session():

    mock_tale = {
        '_id': 'tale1',
        'dataSet': {
            '_modelType': 'item',
            'itemId': '60d4897ce13fb71b1c179fe4',
            'mountPath': 'usco2000.xls'
        }
    }

    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.post = mock_gc_post
    mock_gc.get = mock_gc_get

    session1 = _get_session(mock_gc, tale=mock_tale)
    assert session1['_id'] == 'session1'

    session2 = _get_session(mock_gc, tale=None, version_id='version1')
    assert session2['_id'] == 'session2'
