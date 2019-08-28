import copy
import pytest
import mock
import httmock

import girder_worker
from gwvolman.lib.publish_provider import NullManager
from gwvolman.tasks import publish
from girder_client import GirderClient


TALE = {
    "_accessLevel": 2,
    "_id": "5cfd57fca18691e5d1feeda6",
    "_modelType": "tale",
    "authors": [
        {
            "firstName": "Kacper",
            "lastName": "Kowalik",
            "orcid": "https://orcid.org/0000-0003-1709-3744",
        },
        {
            "firstName": "Craig",
            "lastName": "Willis",
            "orcid": "https://orcid.org/0000-0002-6148-7196",
        },
    ],
    "category": "Examples",
    "config": {},
    "created": "2019-06-09T19:03:24.116000+00:00",
    "creatorId": "59fb6165f7e8790001da4e8b",
    "dataSet": [
        {
            "_modelType": "item",
            "itemId": "598c693e4264d20001cd8494",
            "mountPath": "usco2005.xls",
        }
    ],
    "dataSetCitation": [
        (
            "White, D. and Alessa, L. (2010) “Humans and Hydrology at High Latitudes: "
            "Water Use Information, Version 1.0.” UCAR/NCAR - Earth Observing Laboratory. "
            "doi: 10.5065/D6862DM8."
        )
    ],
    "description": (
        "Demonstration of how to use Whole Tale to develop custom analysis and visualization for "
        "data published externally via DataONE.  See https://wholetale.readthedocs.io/en/stable/u"
        "sers_guide/quickstart.html for more information."
    ),
    "folderId": "5cfd57fca18691e5d1feeda8",
    "format": 7,
    "icon": (
        "https://raw.githubusercontent.com/whole-tale/jupyter-base/master"
        "/squarelogo-greytext-orangebody-greymoons.png"
    ),
    "illustration": "http://use.yt/upload/dc1da723",
    "imageId": "5c8bba172744a50001c5e548",
    "licenseSPDX": "CC-BY-4.0",
    "public": True,
    "publishInfo": [],
    "title": "Example Tale: Mapping Estimated Water Usage",
    "updated": "2019-10-08T17:44:29.523000+00:00",
}

MANIFEST = {
    "@context": [
        "https://w3id.org/bundle/context",
        {"schema": "http://schema.org/"},
        {"Datasets": {"@type": "@id"}},
    ],
    "@id": "https://data.wholetale.org/api/v1/tale/5cfd57fca18691e5d1feeda6",
    "Datasets": [
        {
            "@id": "doi:10.5065/D6862DM8",
            "@type": "Dataset",
            "identifier": "doi:10.5065/D6862DM8",
            "name": "Humans and Hydrology at High Latitudes: Water Use Information",
        }
    ],
    "aggregates": [
        {"uri": "../workspace/.ipynb_checkpoints/wt_quickstart-checkpoint.ipynb"},
        {"uri": "../workspace/.ipynb_checkpoints/README-checkpoint.md"},
        {"uri": "../workspace/.ipynb_checkpoints/apt-checkpoint.txt"},
        {"uri": "../workspace/.ipynb_checkpoints/requirements-checkpoint.txt"},
        {"uri": "../workspace/.ipynb_checkpoints/postBuild-checkpoint"},
        {"uri": "../workspace/postBuild"},
        {"uri": "../workspace/requirements.txt"},
        {"uri": "../workspace/wt_quickstart.ipynb"},
        {"uri": "../workspace/apt.txt"},
        {"uri": "../workspace/README.md"},
        {
            "bundledAs": {"filename": "usco2005.xls", "folder": "../data/"},
            "schema:isPartOf": "doi:10.5065/D6862DM8",
            "size": 6427136,
            "uri": (
                "https://cn.dataone.org/cn/v2/resolve/"
                "urn:uuid:01a53103-8db1-46b3-967c-b42acf69ae08"
            ),
        },
        {"schema:license": "CC-BY-4.0", "uri": "../LICENSE"},
        {"@type": "schema:HowTo", "uri": "../README.md"},
    ],
    "createdBy": {
        "@id": "willis8@illinois.edu",
        "@type": "schema:Person",
        "schema:email": "willis8@illinois.edu",
        "schema:familyName": "Willis",
        "schema:givenName": "Craig",
    },
    "createdOn": "2019-06-09 19:03:24.116000",
    "schema:author": [
        {
            "@id": "https://orcid.org/0000-0003-1709-3744",
            "@type": "schema:Person",
            "schema:familyName": "Kowalik",
            "schema:givenName": "Kacper",
        },
        {
            "@id": "https://orcid.org/0000-0002-6148-7196",
            "@type": "schema:Person",
            "schema:familyName": "Willis",
            "schema:givenName": "Craig",
        },
    ],
    "schema:category": "Examples",
    "schema:description": (
        "Demonstration of how to use Whole Tale to develop custom analysis and visualization for "
        "data published externally via DataONE.  See https://wholetale.readthedocs.io/en/stable/u"
        "sers_guide/quickstart.html for more information."
    ),
    "schema:identifier": "5cfd57fca18691e5d1feeda6",
    "schema:image": "http://use.yt/upload/dc1da723",
    "schema:name": "Example Tale: Mapping Estimated Water Usage",
    "schema:version": 7,
}


@httmock.all_requests
def mock_other_request(url, request):
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
            "links": {"self": "https://sandbox.zenodo.org/api/records/123"},
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
        content=None,
        headers={},
        reason={
            "message": "Validation error",
            "status": 400,
            "errors": [
                {"code": 10, "message": "Not a valid choice", "field": "access_right"}
            ],
        },
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
    assert request.original.data == {"filename": "{}.zip".format(TALE["_id"])}
    return httmock.response(
        status_code=201,
        content={
            "id": 1,
            "filename": "{}.zip".format(TALE["_id"]),
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
    assert request.original.data == {"filename": "{}.zip".format(TALE["_id"])}
    return httmock.response(
        status_code=404,
        content=None,
        headers={},
        reason={"message": "Deposition not found", "status": 404},
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
            "links": {"doi": "http://dx.doi.org/10.123/123"},
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


def mock_tale_update(path, json=None):
    assert path == "tale/" + TALE["_id"]
    assert len(json["publishInfo"]) == 1
    publish_info = json["publishInfo"][0]
    assert publish_info["pid"] == "10.123/123"
    assert publish_info["uri"] == "http://dx.doi.org/10.123/123"


def mock_tale_update_draft(path, json=None):
    assert path == "tale/" + TALE["_id"]
    assert len(json["publishInfo"]) == 1
    publish_info = json["publishInfo"][0]
    assert publish_info["pid"] is None
    assert publish_info["uri"] == "https://sandbox.zenodo.org/api/records/123"


def mock_gc_get(path):
    if path == "/tale/123":
        return copy.deepcopy(TALE)
    elif path == "/tale/123/manifest":
        return copy.deepcopy(MANIFEST)
    else:
        raise RuntimeError


@pytest.mark.celery(result_backend="rpc")
def test_zenodo_publish():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_gc.get = mock_gc_get
    mock_gc.put = mock_tale_update
    publish.girder_client = mock_gc
    publish.job_manager = NullManager()
    girder_worker.task.Task.canceled = mock.PropertyMock(return_value=False)

    token = {
        "provider": "zenodo",
        "access_token": "zenodo_api_key",
        "resource_server": "sandbox.zenodo.org",
    }

    with httmock.HTTMock(
        mock_create_deposit_fail,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_delete_deposition,
        mock_other_request,
    ):
        with pytest.raises(ValueError) as error:
            publish("123", token, repository="sandbox.zenodo.org")
            assert error.message.startswith("Failed to create a deposition.")

    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_deposit_files_fail,
        mock_publish_deposit_ok,
        mock_delete_deposition,
        mock_other_request,
    ):
        with pytest.raises(ValueError) as error:
            publish("123", token, repository="sandbox.zenodo.org")
            assert error.message.startswith("Failed to upload to a deposition")

    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        publish("123", token, repository="sandbox.zenodo.org")

    mock_gc.put = mock_tale_update_draft
    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        publish("123", token, repository="sandbox.zenodo.org", draft=True)

    mock_gc.put = lambda: (_ for _ in ()).throw(Exception("Girder Died"))
    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        with pytest.raises(ValueError) as error:
            publish("123", token, repository="sandbox.zenodo.org")
            assert error.message.startswith("Error updating Tale")
