import girder_worker
from girder_client import GirderClient
from hashlib import md5
import httmock
from io import BytesIO
import mock
import os
import jwt
import pytest
import uuid
from d1_common.system_metadata import generate_system_metadata_pyxb

from gwvolman.lib.publish_provider import NullManager
from gwvolman.tasks import publish
from gwvolman.lib.dataone.publish import DataONEPublishProvider
from gwvolman.tests import TALE, mock_gc_get, mock_dataone_formats


def stream_response(chunk_size=65536):
    test_path = os.path.dirname(__file__)
    version_id = TALE['dct:hasVersion']['@id'].rsplit('/', 1)[-1]
    with open("{}/../../data/{}.zip".format(test_path, version_id), "rb") as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            yield data


def mock_tale_update_dataone(path, json=None):
    assert path == "tale/" + TALE["_id"]
    assert len(json["publishInfo"]) == 1
    # TODO Check something


@httmock.all_requests
def mock_other_request(url, request):
    if request.url.startswith("http+docker://"):
        return httmock.response(status_code=403)
    raise Exception("Unexpected url %s" % str(request.url))


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


def test_get_http_orcid():
    https_orcid = "https://orcid.org/0000-0002-1756-2128"
    http_orcid = DataONEPublishProvider._get_http_orcid(https_orcid)
    assert http_orcid.startswith("http://")


def test_get_dataone_package_url():
    pid = "urn:uuid:test"
    production_url = "https://cn.dataone.org/cn"
    non_production_url = "https://cn-stage.test.dataone.org/cn"
    res = DataONEPublishProvider._get_dataone_package_url(production_url, pid)
    assert res == '{}/{}'.format("https://search.dataone.org/view", pid)

    res = DataONEPublishProvider._get_dataone_package_url(non_production_url, pid)
    assert res == '{}/{}'.format("https://dev.nceas.ucsb.edu/view", pid)


def test_get_manifest_file_info():

    # A PoC manifest that has proper form
    good_manifest = {
        "aggregates": [
            {
                "bundledAs": {"filename": "usco2005.xls", "folder": "./data/"},
                "schema:isPartOf": "doi:10.5065/D6862DM8",
                "wt:size": 6427136,
                "wt:md5": "4071ccff46472c9c87af5827d46f4837",
                "uri": (
                    "https://cn.dataone.org/cn/v2/resolve/"
                    "urn:uuid:01a53103-8db1-46b3-967c-b42acf69ae08"
                ),
            },
        ]
    }

    relpath = "https://cn.dataone.org/cn/v2/resolve/urn:uuid:01a53103-8db1-46b3-967c-b42acf69ae08"
    size, md5 = DataONEPublishProvider._get_manifest_file_info(good_manifest, relpath)
    assert size == 6427136
    assert md5 == "4071ccff46472c9c87af5827d46f4837"

    # A malformed manifest missing the md5
    bad_manifest = {
        "aggregates": [
            {
                "bundledAs": {"filename": "usco2005.xls", "folder": "./data/"},
                "schema:isPartOf": "doi:10.5065/D6862DM8",
                "wt:size": 6427136,
                "uri": (
                    "https://cn.dataone.org/cn/v2/resolve/"
                    "urn:uuid:01a53103-8db1-46b3-967c-b42acf69ae08"
                ),
            },
        ]
    }

    with pytest.raises(KeyError):
        DataONEPublishProvider._get_manifest_file_info(bad_manifest, relpath)


def test_update_sysmeta():
    original_sysmeta = generate_system_metadata_pyxb("pid",
                                                     "format_id",
                                                     BytesIO(b"body"),
                                                     "submitter",
                                                     "rights_holder",
                                                     "urn:mn:urn",)
    new_data = "98765"
    new_pid = "1234"
    new_checksum = md5(new_data.encode("utf-8")).hexdigest()
    new_sysmeta = DataONEPublishProvider.update_sysmeta(original_sysmeta, new_data, new_pid)
    assert new_sysmeta.size == len(new_data)
    assert new_sysmeta.identifier.value() == new_pid
    assert new_sysmeta.obsoletes is None
    assert new_sysmeta.checksum.value() == new_checksum


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

    with httmock.HTTMock(
        mock_generate_dataone_ok,
        mock_object_dataone_ok,
        mock_dataone_formats,
        mock_other_request,
    ):
        token = {
            "provider": "dataonestage2",
            "access_token": "jwt_token",
            "resource_server": "cn-stage-2.test.dataone.org",
        }

        with pytest.raises(jwt.exceptions.DecodeError) as error:
            version_id = TALE['dct:hasVersion']['@id'].rsplit('/', 1)[-1]
            publish("123", token, version_id, repository="https://dev.nceas.ucsb.edu/knb/d1/mn")
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
        publish("123", token, version_id, repository="https://dev.nceas.ucsb.edu/knb/d1/mn")
