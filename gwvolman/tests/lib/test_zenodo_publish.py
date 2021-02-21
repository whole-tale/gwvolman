from girder_client import GirderClient
import girder_worker
import httmock
import json
import mock
import os
import pytest

from gwvolman.lib.publish_provider import NullManager
from gwvolman.tasks import publish
from gwvolman.tests import TALE, ZENODO_TOKEN, mock_gc_get


@httmock.all_requests
def mock_other_request(url, request):
    if request.url.startswith("http+docker://"):
        return httmock.response(status_code=403)
    raise Exception("Unexpected url %s" % str(request.url))


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions$",
    method="POST",
)
def mock_create_deposit_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    return httmock.response(
        status_code=200,
        content={
            "id": 123,
            "links": {
                "self": "https://sandbox.zenodo.org/api/records/123",
                "record_html": "https://sandbox.zenodo.org/record/123",
            },
            "metadata": {},
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/123$",
    method="PUT",
)
def mock_update_deposit_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    req_data = json.loads(request.body)
    assert req_data["metadata"]["notes"].startswith("Run this ")
    return httmock.response(
        status_code=200,
        content={
            "id": 123,
            "links": {
                "self": "https://sandbox.zenodo.org/api/records/123",
                "record_html": "https://sandbox.zenodo.org/record/123",
            },
            "metadata": req_data["metadata"],
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/123$",
    method="PUT",
)
def mock_update_deposit_fail(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    req_data = json.loads(request.body)
    assert req_data["metadata"]["notes"].startswith("Run this ")
    return httmock.response(
        status_code=400,
        content={
            "message": "Validation error",
            "status": 400,
            "errors": [{"code": 10, "message": "Not a valid choice", "field": "notes"}],
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions$",
    method="POST",
)
def mock_create_deposit_fail(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    return httmock.response(
        status_code=400,
        content={
            "message": "Validation error",
            "status": 400,
            "errors": [
                {"code": 10, "message": "Not a valid choice", "field": "access_right"}
            ],
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/123/files$",
    method="POST",
)
def mock_deposit_files_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    assert request.original.data == {"name": "{}.zip".format(TALE["_id"])}
    return httmock.response(
        status_code=201,
        content={
            "id": 1,
            "name": "{}.zip".format(TALE["_id"]),
            "filesize": 300,
            "checksum": "abcd",
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/123/files$",
    method="POST",
)
def mock_deposit_files_fail(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    assert request.original.data == {"name": "{}.zip".format(TALE["_id"])}
    return httmock.response(
        status_code=404,
        content={"message": "Deposition not found", "status": 404},
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/123/actions/publish$",
    method="POST",
)
def mock_publish_deposit_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    return httmock.response(
        status_code=202,
        content={
            "id": 123,
            "doi": "10.123/123",
            "links": {
                "doi": "http://dx.doi.org/10.123/123",
                "record_html": "https://sandbox.zenodo.org/record/457",
            },
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/123$",
    method="DELETE",
)
def mock_delete_deposition(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    return httmock.response(
        status_code=204,
        content=None,
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/456$",
    method="GET",
)
def mock_get_original_deposit_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    return httmock.response(
        status_code=200,
        content={
            "id": 456,
            "doi": "10.345/6789",
            "links": {
                "doi": "http://dx.doi.org/10.345/6789",
                "record_html": "https://sandbox.zenodo.org/record/457",
            },
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/457$",
    method="GET",
)
def mock_get_new_deposit_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    return httmock.response(
        status_code=200,
        content={
            "id": 457,
            "doi": "10.345/6780",
            "links": {"doi": "http://dx.doi.org/10.345/6780"},
            "files": [
                {
                    "id": 1,
                    "name": "already_published.zip",
                    "filesize": 300,
                    "checksum": "abcd",
                }
            ],
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/457/files/1$",
    method="DELETE",
)
def mock_delete_files_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    return httmock.response(
        status_code=204,
        content=None,
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/456/actions/newversion$",
    method="POST",
)
def mock_new_version_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    return httmock.response(
        status_code=201,
        content={
            "id": 456,
            "doi": "10.345/6789",
            "links": {
                "doi": "http://dx.doi.org/10.345/6789",
                "record_html": "https://sandbox.zenodo.org/record/457",
                "latest_draft": "https://sandbox.zenodo.org/api/deposit/depositions/457",
            },
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^sandbox.zenodo.org$",
    path="^/api/deposit/depositions/457$",
    method="PUT",
)
def mock_update_newver_deposit_ok(url, request):
    assert request.headers["Authorization"] == "Bearer zenodo_api_key"
    req_data = json.loads(request.body)
    return httmock.response(
        status_code=200,
        content={
            "id": 457,
            "links": {"self": "https://sandbox.zenodo.org/api/records/457"},
            "metadata": req_data["metadata"],
        },
        headers={},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


def mock_tale_update(path, json=None):
    assert path == "tale/" + TALE["_id"]
    assert len(json["publishInfo"]) == 1
    publish_info = json["publishInfo"][0]
    assert publish_info["pid"] == "doi:10.123/123"
    assert publish_info["uri"] == "https://sandbox.zenodo.org/record/457"
    assert publish_info["repository"] == "sandbox.zenodo.org"
    assert publish_info["repository_id"] == "123"


def mock_tale_update_draft(path, json=None):
    assert path == "tale/" + TALE["_id"]
    assert len(json["publishInfo"]) == 1
    publish_info = json["publishInfo"][0]
    assert publish_info["pid"] is None
    assert publish_info["uri"] == "https://sandbox.zenodo.org/api/records/123"


def stream_response(chunk_size=65536):
    test_path = os.path.dirname(__file__)
    version_id = TALE['dct:hasVersion']['@id'].rsplit('/', 1)[-1]
    with open("{}/../data/{}.zip".format(test_path, version_id), "rb") as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            yield data


@pytest.mark.celery(result_backend="rpc")
def test_zenodo_publish():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_req.iter_content = stream_response
    mock_gc.get = mock_gc_get
    mock_gc.put = mock_tale_update
    mock_gc.sendRestRequest.return_value = mock_req
    publish.girder_client = mock_gc
    publish.job_manager = NullManager()
    girder_worker.task.Task.canceled = mock.PropertyMock(return_value=False)

    with httmock.HTTMock(
        mock_create_deposit_fail,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_delete_deposition,
        mock_other_request,
    ):
        version_id = TALE['dct:hasVersion']['@id'].rsplit('/', 1)[-1]
        with pytest.raises(ValueError) as error:
            publish("123", ZENODO_TOKEN, version_id, repository="sandbox.zenodo.org")

        assert error.match("Failed to create a deposition.")

    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_fail,
        mock_delete_deposition,
        mock_other_request,
    ):
        with pytest.raises(ValueError) as error:
            publish("123", ZENODO_TOKEN, version_id, repository="sandbox.zenodo.org")
        assert error.match("Failed to update the deposition")

    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_ok,
        mock_deposit_files_fail,
        mock_publish_deposit_ok,
        mock_delete_deposition,
        mock_other_request,
    ):
        with pytest.raises(ValueError) as error:
            publish("123", ZENODO_TOKEN, version_id, repository="sandbox.zenodo.org")
        assert error.match("Failed to upload to a deposition")

    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        publish("123", ZENODO_TOKEN, version_id, repository="sandbox.zenodo.org")

    mock_gc.put = mock_tale_update_draft
    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        publish("123", ZENODO_TOKEN, version_id, repository="sandbox.zenodo.org", draft=True)

    mock_gc.put = lambda: (_ for _ in ()).throw(Exception("Girder Died"))
    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        with pytest.raises(ValueError) as error:
            publish("123", ZENODO_TOKEN, version_id, repository="sandbox.zenodo.org")
        assert error.match("Error updating Tale")

    mock_gc.put = mock_tale_update
    with httmock.HTTMock(
        mock_get_original_deposit_ok,
        mock_new_version_ok,
        mock_get_new_deposit_ok,
        mock_delete_files_ok,
        mock_update_newver_deposit_ok,
        mock_other_request,
    ):
        with mock.patch(
            "gwvolman.tasks.ZenodoPublishProvider.publish_version", lambda x, y: None
        ):
            publish("already_published", ZENODO_TOKEN, version_id, repository="sandbox.zenodo.org")
