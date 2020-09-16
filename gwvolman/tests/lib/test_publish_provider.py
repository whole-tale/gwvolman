import copy
import mock
import pytest
from girder_client import GirderClient

from gwvolman.lib.publish_provider import PublishProvider, NullManager


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

TALE_NO_DESC = {
    "_accessLevel": 2,
    "_id": "4cfd57fca18691e5d1feeda6",
    "_modelType": "tale",
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
    "description": '',
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

PUBLISHED_TALE = {
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
    "publishInfo": [
    {
        "date": "2020-09-15T22:25:10.844945",
        "pid": "doi:10.5072/FK2JM2C37M",
        "repository": "https://dev.nceas.ucsb.edu/knb/d1/mn",
        "repository_id": "urn:uuid:e98b1ca9-178a-4ed7-ba4b-54e13cd6c7c3",
        "uri": "https://dev.nceas.ucsb.edu/view/urn:uuid:e98b1ca9-178a-4ed7-ba4b-54e13cd6c7c3"
    }],
    "title": "Example Tale: Mapping Estimated Water Usage",
    "updated": "2019-10-08T17:44:29.523000+00:00",
}

TOKEN = {
    "provider": "zenodo",
    "access_token": "zenodo_api_key",
    "resource_server": "sandbox.zenodo.org",
}


def mock_gc_get(path):
    if path in ("/tale/123", "tale/5cfd57fca18691e5d1feeda6"):
        return copy.deepcopy(TALE)
    elif path in ("tale/4cfd57fca18691e5d1feeda6"):
        return copy.deepcopy(TALE_NO_DESC)
    else:
        raise RuntimeError


def test_ctor():
    mock_gc = mock.MagicMock(spec=GirderClient)
    provider = PublishProvider(mock_gc, TALE['_id'], TOKEN)
    assert provider.tale == TALE
    assert provider.job_manager == NullManager()
    assert provider.manifest == MANIFEST

    # Test without a Tale description
    with pytest.raises(ValueError):
        provider = PublishProvider(mock_gc, TALE_NO_DESC['_id'], TOKEN)


def test_published():
    mock_gc = mock.MagicMock(spec=GirderClient)
    provider = PublishProvider(mock_gc, TALE['_id'], TOKEN)
    assert provider.published is False

    provider = PublishProvider(mock_gc, PUBLISHED_TALE['_id'], TOKEN)
    assert provider.published is True


def test_publication_info():
    mock_gc = mock.MagicMock(spec=GirderClient)
    provider = PublishProvider(mock_gc, PUBLISHED_TALE['_id'], TOKEN)
    assert provider.publication_info is True