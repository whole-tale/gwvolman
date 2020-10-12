import copy
import mock
import pytest
from girder_client import GirderClient

from gwvolman.lib.publish_provider import PublishProvider, NullManager
from gwvolman.tests import TALE, PUBLISHED_TALE, ZENODO_TOKEN, MANIFEST, TALE_NO_DESC


def mock_gc_get(path):
    if path in "/tale/5cfd57fca18691e5d1feeda6":
        return copy.deepcopy(TALE)
    elif path in "/tale/4cfd57fca18691e5d1feeda6":
        return copy.deepcopy(TALE_NO_DESC)
    elif path.startswith("/tale") and path.endswith("/manifest"):
        return copy.deepcopy(MANIFEST)
    elif path in "/tale/1cfd57fca18691e5d1feeda6":
        return copy.deepcopy(PUBLISHED_TALE)
    else:
        raise RuntimeError


def test_ctor():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req

    provider = PublishProvider(mock_gc, TALE['_id'], ZENODO_TOKEN)
    assert provider.tale == TALE
    assert isinstance(provider.job_manager, NullManager)
    assert provider.manifest == MANIFEST

    # Test without a Tale description
    with pytest.raises(AssertionError):
        PublishProvider(mock_gc, TALE_NO_DESC['_id'], ZENODO_TOKEN)


def test_published():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req

    provider = PublishProvider(mock_gc, TALE['_id'], ZENODO_TOKEN)
    assert provider.published is False

    provider = PublishProvider(mock_gc, PUBLISHED_TALE['_id'], ZENODO_TOKEN)
    assert provider.published is True


def test_publication_info():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req

    provider = PublishProvider(mock_gc, PUBLISHED_TALE['_id'], ZENODO_TOKEN)
    assert len(provider.publication_info) > 0


def test_access_token():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req

    provider = PublishProvider(mock_gc, PUBLISHED_TALE['_id'], ZENODO_TOKEN)
    assert provider.access_token == ZENODO_TOKEN['access_token']


def test_resource_server_token():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req

    provider = PublishProvider(mock_gc, PUBLISHED_TALE['_id'], ZENODO_TOKEN)
    assert provider.resource_server == ZENODO_TOKEN['resource_server']


def test_publish():
    with pytest.raises(NotImplementedError):
        mock_gc = mock.MagicMock(spec=GirderClient)
        mock_req = mock.MagicMock()
        mock_gc.get = mock_gc_get
        mock_gc.sendRestRequest.return_value = mock_req
        provider = PublishProvider(mock_gc, PUBLISHED_TALE['_id'], ZENODO_TOKEN)
        provider.publish()