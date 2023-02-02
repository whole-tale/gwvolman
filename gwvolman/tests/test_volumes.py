from girder_client import GirderClient
import mock
import os

os.environ['GIRDER_API_URL'] = 'https://girder.dev.wholetale.org/api/v1'

from gwvolman.tasks import create_volume, _mount_girderfs, \
    _make_fuse_dirs, _get_session # noqa


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
@mock.patch("os.mkdir", return_value=None)
@mock.patch("docker.client.DockerClient.info", return_value={'Swarm': {'NodeID': 'node1'}})
@mock.patch("gwvolman.tasks._get_api_key", return_value="apikey1")
@mock.patch("gwvolman.tasks._get_session", return_value={"_id": "session1"})
@mock.patch("gwvolman.tasks._make_fuse_dirs", return_value=True)
@mock.patch("gwvolman.tasks._mount_girderfs", return_value=True)
@mock.patch("gwvolman.tasks._mount_bind", return_value=True)
def test_create_volume(mbind, mgfs, mfd, gs, gak, info, osmk, nu):

    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get = mock_gc_get
    mock_gc.loadOrCreateFolder = mock.Mock(return_value={'_id': 'folder1'})

    create_volume.girder_client = mock_gc
    create_volume.job_manager = mock.MagicMock()

    mount_point = "/mnt/homes/mountpoints/tale1_user1_123456"

    try:
        ret = create_volume('instance1')

        expected = {
            'nodeId': 'node1',
            'mountPoint': mount_point,
            'sessionId': 'session1',
            'instanceId': 'instance1',
            'taleId': "tale1",
            'volumeName': 'tale1_user1_123456',
        }

        assert ret == expected
    except ValueError:
        assert False

    osmk.assert_has_calls([
        mock.call("/mnt/homes/mountpoints/tale1_user1_123456")
    ])

    mbind.assert_has_calls([
        mock.call(mount_point, 'home', {'_id': 'ghi567', 'login': 'user1'}),
        mock.call(mount_point, 'workspace', {'_id': 'tale1'}),
    ])
    mgfs.assert_has_calls([
        mock.call(mount_point, 'data', 'wt_dms', 'session1',
                  'apikey1', hostns=False),
        mock.call(mount_point, 'versions', 'wt_versions', 'tale1',
                  'apikey1', hostns=False),
        mock.call(mount_point, 'runs', 'wt_runs', 'tale1', 'apikey1',
                  hostns=False)
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


@mock.patch("os.mkdir", return_value=True)
def test_make_fuse_dirs(mkdir):

    with mock.patch('os.path.isdir', return_value=True):
        _make_fuse_dirs('/path/to/mountpoint', ['dir1'])

    mkdir.assert_has_calls([mock.call('/path/to/mountpoint/dir1')], any_order=False)


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
