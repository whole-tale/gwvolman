# A number of common/shared structures for testing
import copy
import httmock
import os


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
            "firstName": "Craigg",
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
    "_id": "1cfd57fca18691e5d1feeda6",
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
            "date": "2020-09-16T20:24:52.578794",
            "pid": "doi:10.5072/zenodo.670350",
            "repository": "sandbox.zenodo.org",
            "repository_id": "670350",
            "uri": "https://sandbox.zenodo.org/record/670350"
        }],
    "title": "Example Tale: Mapping Estimated Water Usage",
    "updated": "2019-10-08T17:44:29.523000+00:00",
}


PARENT_TALE = {
    "_accessLevel": 2,
    "_id": "123456789",
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
    "created": "2020-06-09T19:03:24.116000+00:00",
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

COPIED_TALE = {
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
    "copyOfTale": "123456789",
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


ZENODO_TOKEN = {
    "provider": "zenodo",
    "access_token": "zenodo_api_key",
    "resource_server": "sandbox.zenodo.org",
}

DATAONE_TEST_TOKEN = {
    "provider": "dataonestage2",
    "access_token": "jwt_token",
    "resource_server": "cn-stage-2.test.dataone.org",
}


def mock_gc_get(path, parameters=None):
    if path in ("/tale/123", "/tale/5cfd57fca18691e5d1feeda6"):
        return copy.deepcopy(TALE)
    elif path in "/tale/4cfd57fca18691e5d1feeda6":
        return copy.deepcopy(TALE_NO_DESC)
    elif path.startswith("/tale") and path.endswith("/manifest"):
        assert "expandFolders" in parameters
        assert parameters["expandFolders"] is True
        return copy.deepcopy(MANIFEST)
    elif path in "/tale/1cfd57fca18691e5d1feeda6":
        return copy.deepcopy(PUBLISHED_TALE)
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


@httmock.urlmatch(
    scheme="https",
    netloc="^cn-stage-2.test.dataone.org$",
    path="^/cn/v2/formats$",
    method="GET",
)
def mock_dataone_formats(url, request):
    resp_fname = os.path.join(os.path.dirname(__file__), "d1formats_resp.xml")
    with open(resp_fname, "r") as fp:
        response = fp.read()
    return httmock.response(
        status_code=200,
        content=response,
        headers={"Connection": "Close", "Content-Type": "text/xml"},
        reason=None,
        elapsed=5,
        request=request,
        stream=False,
    )
