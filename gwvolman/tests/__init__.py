# A number of common/shared structures for testing
import copy
import json
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
    "dct:hasVersion": {
        "@id": "https://data.wholetale.org/api/v1/folder/603152b34ac3a96578dda45d",
        "@type": "wt:TaleVersion",
        "schema:creator": {
            "@id": "mailto:thelen@nceas.ucsb.edu",
            "@type": "schema:Person",
            "schema:email": "thelen@nceas.ucsb.edu",
            "schema:familyName": "T",
            "schema:givenName": "Thomas",
        },
        "schema:dateModified": "2021-02-20T18:19:31.012000+00:00",
        "schema:name": "empty",
    },
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
    "description": "",
    "dct:hasVersion": {
        "@id": "https://data.wholetale.org/api/v1/folder/603152b34ac3a96578dda45d",
        "@type": "wt:TaleVersion",
        "schema:creator": {
            "@id": "mailto:thelen@nceas.ucsb.edu",
            "@type": "schema:Person",
            "schema:email": "thelen@nceas.ucsb.edu",
            "schema:familyName": "T",
            "schema:givenName": "Thomas",
        },
        "schema:dateModified": "2021-02-20T18:19:31.012000+00:00",
        "schema:name": "empty",
    },
}

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
MANIFEST = json.load(open(os.path.join(DATA_DIR, "manifest.json"), "r"))

EML = ""
with open(os.path.join(DATA_DIR, "eml_doc.xml"), "r") as f:
    EML = f.read()

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
    "dct:hasVersion": {
        "@id": "https://data.wholetale.org/api/v1/folder/603152b34ac3a96578dda45d",
        "@type": "wt:TaleVersion",
        "schema:creator": {
            "@id": "mailto:thelen@nceas.ucsb.edu",
            "@type": "schema:Person",
            "schema:email": "thelen@nceas.ucsb.edu",
            "schema:familyName": "T",
            "schema:givenName": "Thomas",
        },
        "schema:dateModified": "2021-02-20T18:19:31.012000+00:00",
        "schema:name": "empty",
    },
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
            "uri": "https://sandbox.zenodo.org/record/670350",
        }
    ],
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
    "dct:hasVersion": {
        "@id": "https://data.wholetale.org/api/v1/folder/603152b34ac3a96578dda45d",
        "@type": "wt:TaleVersion",
        "schema:creator": {
            "@id": "mailto:thelen@nceas.ucsb.edu",
            "@type": "schema:Person",
            "schema:email": "thelen@nceas.ucsb.edu",
            "schema:familyName": "T",
            "schema:givenName": "Thomas",
        },
        "schema:dateModified": "2021-02-20T18:19:31.012000+00:00",
        "schema:name": "empty",
    },
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
    "dct:hasVersion": {
        "@id": "https://data.wholetale.org/api/v1/folder/603152b34ac3a96578dda45d",
        "@type": "wt:TaleVersion",
        "schema:creator": {
            "@id": "mailto:thelen@nceas.ucsb.edu",
            "@type": "schema:Person",
            "schema:email": "thelen@nceas.ucsb.edu",
            "schema:familyName": "T",
            "schema:givenName": "Thomas",
        },
        "schema:dateModified": "2021-02-20T18:19:31.012000+00:00",
        "schema:name": "empty",
    },
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
