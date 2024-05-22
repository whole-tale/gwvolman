import mock
import pytest
from girder_client import GirderClient

from gwvolman.lib.publish_provider import PublishProvider, NullManager
from . import (
    TALE,
    PUBLISHED_TALE,
    ZENODO_TOKEN,
    MANIFEST,
    TALE_NO_DESC,
    mock_gc_get,
)


def test_ctor():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req
    version_id = TALE["dct:hasVersion"]["@id"].rsplit("/", 1)[-1]
    provider = PublishProvider(mock_gc, TALE["_id"], ZENODO_TOKEN, version_id)
    assert provider.tale == TALE
    assert isinstance(provider.job_manager, NullManager)
    assert provider.manifest == MANIFEST

    # Test without a Tale description
    with pytest.raises(AssertionError):
        PublishProvider(mock_gc, TALE_NO_DESC["_id"], ZENODO_TOKEN, version_id)


def test_published():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req
    version_id = TALE["dct:hasVersion"]["@id"].rsplit("/", 1)[-1]
    provider = PublishProvider(mock_gc, TALE["_id"], ZENODO_TOKEN, version_id)
    assert provider.published is False

    provider = PublishProvider(mock_gc, PUBLISHED_TALE["_id"], ZENODO_TOKEN, version_id)
    assert provider.published is True


def test_publication_info():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req
    version_id = TALE["dct:hasVersion"]["@id"].rsplit("/", 1)[-1]
    provider = PublishProvider(mock_gc, PUBLISHED_TALE["_id"], ZENODO_TOKEN, version_id)
    assert len(provider.publication_info) > 0


def test_access_token():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req
    version_id = TALE["dct:hasVersion"]["@id"].rsplit("/", 1)[-1]
    provider = PublishProvider(mock_gc, PUBLISHED_TALE["_id"], ZENODO_TOKEN, version_id)
    assert provider.access_token == ZENODO_TOKEN["access_token"]


def test_resource_server_token():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_gc.get = mock_gc_get
    mock_gc.sendRestRequest.return_value = mock_req
    version_id = TALE["dct:hasVersion"]["@id"].rsplit("/", 1)[-1]
    provider = PublishProvider(mock_gc, PUBLISHED_TALE["_id"], ZENODO_TOKEN, version_id)
    assert provider.resource_server == ZENODO_TOKEN["resource_server"]


def test_publish():
    with pytest.raises(NotImplementedError):
        mock_gc = mock.MagicMock(spec=GirderClient)
        mock_req = mock.MagicMock()
        mock_gc.get = mock_gc_get
        mock_gc.sendRestRequest.return_value = mock_req
        version_id = TALE["dct:hasVersion"]["@id"].rsplit("/", 1)[-1]
        provider = PublishProvider(
            mock_gc, PUBLISHED_TALE["_id"], ZENODO_TOKEN, version_id
        )
        provider.publish()
