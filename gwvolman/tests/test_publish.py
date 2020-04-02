import copy
from girder_client import GirderClient
import girder_worker
import httmock
import io
import json
import jwt
import mock
import os
import uuid
import pytest

from gwvolman.lib.publish_provider import NullManager
from gwvolman.tasks import publish


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
        {"uri": "../workspace/postBuild"},
        {"uri": "../workspace/requirements.txt"},
        {"uri": "../workspace/wt_quickstart.ipynb"},
        {"uri": "../workspace/apt.txt"},
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
    netloc="^dev.nceas.ucsb.edu$",
    path="^/knb/d1/mn/v2/generate$",
    method="POST",
)
def mock_generate_dataone_ok(url, request):
    response = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<identifier '
        'xmlns="http://ns.dataone.org/service/types/v1">{}</identifier>\n'
    )
    if request.body.fields["scheme"] == b"DOI":
        content = response.format("doi:10.5072/FK26T0RF9D")
    elif request.body.fields["scheme"] == b"UUID":
        content = response.format("urn:uuid:{}".format(uuid.uuid1()))
    return httmock.response(
        status_code=200,
        content=content.encode(),
        headers={"Connection": "Close", "Content-Type": "text/xml"},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


@httmock.urlmatch(
    scheme="https",
    netloc="^dev.nceas.ucsb.edu$",
    path="^/knb/d1/mn/v2/object$",
    method="POST",
)
def mock_object_dataone_ok(url, request):
    response = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n<identifier '
        'xmlns="http://ns.dataone.org/service/types/v1">{}</identifier>\n'
    )

    try:
        pid = request.body.fields["pid"].decode()
        content = response.format(pid)
    except KeyError:
        raise

    return httmock.response(
        status_code=200,
        content=content.encode(),
        headers={"Connection": "Close", "Content-Type": "text/xml"},
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


def mock_tale_update_dataone(path, json=None):
    assert path == "tale/" + TALE["_id"]
    assert len(json["publishInfo"]) == 1
    # TODO Check something


def mock_tale_update_draft(path, json=None):
    assert path == "tale/" + TALE["_id"]
    assert len(json["publishInfo"]) == 1
    publish_info = json["publishInfo"][0]
    assert publish_info["pid"] is None
    assert publish_info["uri"] == "https://sandbox.zenodo.org/api/records/123"


def mock_gc_get(path):
    if path in ("/tale/123", "tale/5cfd57fca18691e5d1feeda6"):
        return copy.deepcopy(TALE)
    elif path.startswith("/tale") and path.endswith("/manifest"):
        return copy.deepcopy(MANIFEST)
    elif path == "/tale/already_published":
        tale = copy.deepcopy(TALE)
        tale["_id"] = "already_published"
        tale["publishInfo"] = [
            {
                "pid": "10.345/6789",
                "uri": "http://dx.doi.org/10.345/6789",
                "repository": "sandbox.zenodo.org",
                "repository_id": "456",
            }
        ]
        return tale
    else:
        raise RuntimeError


def stream_response(chunk_size=65536):
    test_path = os.path.dirname(__file__)
    with open("{}/data/{}.zip".format(test_path, TALE["_id"]), "rb") as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            yield data


@httmock.urlmatch(
    scheme="https",
    netloc="^cn-stage-2.test.dataone.org$",
    path="^/cn/v2/formats$",
    method="GET",
)
def mock_dataone_formats(url, request):
    response = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><?xml-stylesheet type="text/xsl" href="/cn/xslt/dataone.types.v2.xsl" ?>
<ns3:objectFormatList xmlns:ns2="http://ns.dataone.org/service/types/v1" xmlns:ns3="http://ns.dataone.org/service/types/v2.0" count="134" start="0" total="134">
    <objectFormat>
        <formatId>eml://ecoinformatics.org/eml-2.0.0</formatId>
        <formatName>Ecological Metadata Language, version 2.0.0</formatName>
        <formatType>METADATA</formatType>
        <mediaType name="text/xml"/>
        <extension>xml</extension>
    </objectFormat>
    <objectFormat>
        <formatId>text/plain</formatId>
        <formatName>Plain Text</formatName>
        <formatType>DATA</formatType>
        <mediaType name="text/plain"/>
        <extension>txt</extension>
    </objectFormat>
    <objectFormat>
        <formatId>image/png</formatId>
        <formatName>Portable Network Graphics</formatName>
        <formatType>DATA</formatType>
        <mediaType name="image/png"/>
        <extension>png</extension>
    </objectFormat>
    <objectFormat>
        <formatId>application/octet-stream</formatId>
        <formatName>Octet Stream</formatName>
        <formatType>DATA</formatType>
        <mediaType name="application/octet-stream"/>
        <extension>data</extension>
    </objectFormat>
</ns3:objectFormatList>
"""
    return httmock.response(
        status_code=200,
        content=response,
        headers={"Connection": "Close", "Content-Type": "text/xml"},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )


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

        assert error.match("Failed to create a deposition.")

    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_fail,
        mock_delete_deposition,
        mock_other_request,
    ):
        with pytest.raises(ValueError) as error:
            publish("123", token, repository="sandbox.zenodo.org")
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
            publish("123", token, repository="sandbox.zenodo.org")
        assert error.match("Failed to upload to a deposition")

    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        publish("123", token, repository="sandbox.zenodo.org")

    mock_gc.put = mock_tale_update_draft
    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        publish("123", token, repository="sandbox.zenodo.org", draft=True)

    mock_gc.put = lambda: (_ for _ in ()).throw(Exception("Girder Died"))
    with httmock.HTTMock(
        mock_create_deposit_ok,
        mock_update_deposit_ok,
        mock_deposit_files_ok,
        mock_publish_deposit_ok,
        mock_other_request,
    ):
        with pytest.raises(ValueError) as error:
            publish("123", token, repository="sandbox.zenodo.org")
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
            publish("already_published", token, repository="sandbox.zenodo.org")


@pytest.mark.celery(result_backend="rpc")
def test_dataone_publish():
    mock_gc = mock.MagicMock(spec=GirderClient)
    mock_req = mock.MagicMock()
    mock_req.iter_content = stream_response
    mock_gc.get = mock_gc_get
    mock_gc.put = mock_tale_update_dataone
    mock_gc.sendRestRequest.return_value = mock_req
    publish.girder_client = mock_gc
    publish.job_manager = NullManager()
    girder_worker.task.Task.canceled = mock.PropertyMock(return_value=False)

    token = {
        "provider": "dataonestage2",
        "access_token": "jwt_token",
        "resource_server": "cn-stage-2.test.dataone.org",
    }

    with httmock.HTTMock(
        mock_generate_dataone_ok,
        mock_object_dataone_ok,
        mock_dataone_formats,
        mock_other_request,
    ):
        with pytest.raises(jwt.exceptions.JWTDecodeError) as error:
            publish("123", token, repository="https://dev.nceas.ucsb.edu/knb/d1/mn")
            assert error.message.startswith("Not enough segments")

        token["access_token"] = (
            "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJodHRwOlwvXC9vcmNpZC5vcmdcLzAwMDAtMDAwMy0xNzA"
            "5LTM3NDQiLCJmdWxsTmFtZSI6IkthY3BlciBLb3dhbGlrIiwiaXNzdWVkQXQiOiIyMDE5LTExLTA"
            "0VDE4OjM5OjQwLjQxNCswMDowMCIsImNvbnN1bWVyS2V5IjoidGhlY29uc3VtZXJrZXkiLCJleHA"
            "iOjE1NzI5NTc1ODAsInVzZXJJZCI6Imh0dHA6XC9cL29yY2lkLm9yZ1wvMDAwMC0wMDAzLTE3MDk"
            "tMzc0NCIsInR0bCI6NjQ4MDAsImlhdCI6MTU3Mjg5Mjc4MH0.oNGDWmdePMYPUzt1Inhu1r1p95w"
            "0kld6C24nohtgOyRROYtihdnIE0OcoxXd7KXdiVRdXLL34-qmiQTeRMPJEgMDtPNj6JUrP6yXP8Y"
            "LG77iOGrSnKFRK8vJenc7-d8vJCqzebD8Xu6_pslw0GGiRMxfISa_UdGEYp0xyRgAIQmMr7q3H-T"
            "K1P2KHb3M4RCWb5Ubv1XsTRJ5gXsLLu0WvBfXFu-EKAka7IO6uTAK1RZLnJqrotvCCT4lL6GyPPY"
            "YOCJ7pEWDqYsNcu6UC3NiY8u-2qAe-xbBMCP8XtX-u9FOX9QjsxRy4WClPIK9I8bxUj_ehI3m0jG"
            "3gJtWNeGCDw"
        )

        publish("123", token, repository="https://dev.nceas.ucsb.edu/knb/d1/mn")
