"""Whole Tale publisher provider for Zenodo."""

import datetime
import json
import logging
import requests
import tempfile
from urllib.parse import urlencode, urlparse

from markdown import Markdown
from lxml.html.clean import Cleaner

from .publish_provider import PublishProvider
from ..utils import DEPLOYMENT


_ZENODO_ALLOWED_TAGS = {
    "a",
    "p",
    "br",
    "blockquote",
    "strong",
    "b",
    "u",
    "i",
    "em",
    "ul",
    "ol",
    "li",
    "sub",
    "sup",
    "div",
    "strike",
}
_ZENODO_ACCEPTED_RELATIONS = {
    "isCitedBy",
    "cites",
    "isSupplementTo",
    "isSupplementedBy",
    "isContinuedBy",
    "continues",
    "hasMetadata",
    "isMetadataFor",
    "isNewVersionOf",
    "isPreviousVersionOf",
    "isPartOf",
    "hasPart",
    "isReferencedBy",
    "references",
    "isDocumentedBy",
    "documents",
    "isCompiledBy",
    "compiles",
    "isVariantFormOf",
    "isOrignialFormOf",
    "isIdenticalTo",
    "isReviewedBy",
    "reviews",
    "isDerivedFrom",
    "isSourceOf"
}
_ACTION_CODES = {"publish": 202, "newversion": 201}


class ZenodoPublishProvider(PublishProvider):
    """Whole Tale publisher provider for Zenodo."""

    total_updates = 3
    current_update = 0

    def update_progress(self, message=None):
        """Send update and increment update counter."""
        self.current_update += 1
        self.job_manager.updateProgress(
            total=self.total_updates, current=self.current_update, message=message
        )

    @staticmethod
    def _render_description(description):
        """Convert Markdown to HTML accepted by Zenodo."""
        parser = Markdown()
        html = parser.convert(description)
        html_cleaner = Cleaner(
            allow_tags=_ZENODO_ALLOWED_TAGS, remove_unknown_tags=False
        )
        return html_cleaner.clean_html(html)

    def deposit_tale(self, deposition):
        """Given a Zenodo deposition, upload tarball with a Tale."""

        self.update_progress(message="Uploading data to " + self.resource_server)

        stream = self.gc.sendRestRequest(
            "get",
            "tale/{}/export".format(self.tale["_id"]),
            parameters={"taleFormat": "bagit", "versionId": self.version_id},
            stream=True,
            jsonResp=False,
        )
        with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:

            # Write the zip file
            for chunk in stream.iter_content(chunk_size=65536):
                tmp.write(chunk)
            tmp.seek(0)

            r = self.request(
                "/api/deposit/depositions/{}/files".format(deposition["id"]),
                method="POST",
                data={"name": "{}.zip".format(self.version_id)},
                files={"file": tmp},
            )

            try:
                r.raise_for_status()
            except requests.HTTPError as exc:
                msg = "Failed to upload to a deposition (id:{}).".format(
                    deposition["id"]
                )
                msg += " Server returned: " + str(exc)
                if r.status_code > 399 and r.status_code < 500:
                    msg += "\n" + json.dumps(r.json(), sort_keys=True, indent=4) + "\n"
                logging.warning(msg)
                raise ValueError(msg)
            logging.debug(
                "[zenodo:deposit_tale] POST depositions/{}/files = {}".format(
                    deposition["id"], r.json()
                )
            )

    def get_tale_metadata(self):
        """Convert the Tale metadata to a Zenodo metadata."""

        keywords = {"Tale"}
        if self.manifest.get("schema:keywords"):
            keywords.add(self.manifest["schema:keywords"].title())

        def first_letter_lower(s):
            return s[:1].lower() + s[1:] if s else ""

        related_identifiers = []
        for related_id in self.tale.get("datacite:relatedIdentifiers", []):
            relation = related_id["datacite:relatedIdentifier"]
            relation_type = first_letter_lower(relation["datacite:relationType"][3:])
            if relation_type not in _ZENODO_ACCEPTED_RELATIONS:
                continue
            related_identifiers.append(
                {"relation": relation_type, "identifier": relation["@id"]}
            )
        related_identifiers += [
            {"relation": "cites", "identifier": ds["schema:identifier"]}
            for ds in self.manifest.get("wt:usesDataset", [])
        ]
        # Remove duplicates
        related_identifiers = [
            json.loads(rel_id)
            for rel_id in {json.dumps(_, sort_keys=True) for _ in related_identifiers}
        ]

        return {
            "metadata": {
                "upload_type": "publication",
                "publication_type": "other",
                "title": self.manifest["schema:name"],
                "creators": [
                    {
                        "name": "{}, {}".format(
                            author["schema:familyName"], author["schema:givenName"]
                        ),
                        "orcid": author["@id"].split("/")[-1],
                    }
                    for author in self.manifest["schema:author"]
                ],
                "description": self._render_description(
                    self.manifest["schema:description"]
                ),
                "keywords": list(keywords),
                "access_right": "open",
                "license": self.tale["licenseSPDX"].lower(),
                "prereserve_doi": True,
                "related_identifiers": related_identifiers,
                "references": [
                    citation for citation in self.tale.get("dataSetCitation", [])
                ],
            }
        }

    def create_deposition(self):
        """Create a deposition @ Zenodo."""
        zenodo_metadata = self.get_tale_metadata()
        logging.debug(
            "[zenodo:create_deposition] metadata = {}".format(zenodo_metadata)
        )

        self.update_progress(message="Creating a deposition at " + self.resource_server)

        r = self.request(
            "/api/deposit/depositions",
            method="POST",
            data=json.dumps(zenodo_metadata),
            headers={"Content-Type": "application/json"},
        )

        try:
            r.raise_for_status()
        except requests.HTTPError as exc:
            msg = "Failed to create a deposition. Server returned: " + str(exc)
            if r.status_code > 399 and r.status_code < 500:
                msg += "\n" + json.dumps(r.json(), sort_keys=True, indent=4) + "\n"
            logging.warning(msg)
            raise ValueError(msg)
        logging.debug("[zenodo:create_deposition] deposition = {}".format(r.json()))
        return r.json()

    def _zenodo_action(self, deposition, action):
        r = self.request(
            "/api/deposit/depositions/{dep_id}/actions/{action}".format(
                dep_id=deposition["id"], action=action
            ),
            method="POST",
        )
        assert r.status_code == _ACTION_CODES[action]
        logging.debug("[zenodo:_action_{}] deposition = {}".format(action, r.json()))
        return r.json()

    def publish_deposition(self, deposition):
        """Publish publish, I mean REALLY publish."""
        return self._zenodo_action(deposition, "publish")

    def create_new_version(self, deposition):
        """Create a new version of an exisiting deposition."""
        current_deposition = self._zenodo_action(deposition, "newversion")
        # From zenodo docs:
        # NOTE: The response body of this action is NOT the new version deposit,
        # but the original resource. The new version deposition can be accessed
        # through the "latest_draft" under "links" in the response body.
        return self.retrieve_deposition(current_deposition["links"]["latest_draft"])

    def update_deposition(self, deposition):
        """Update deposition, assumes that metadata is changed."""
        r = self.request(
            "/api/deposit/depositions/{}".format(deposition["id"]),
            method="PUT",
            data=json.dumps({"metadata": deposition["metadata"]}),
            headers={"Content-Type": "application/json"},
        )

        try:
            r.raise_for_status()
        except requests.HTTPError as exc:
            msg = "Failed to update the deposition (id={}).".format(deposition["id"])
            msg += " Server returned: " + str(exc)
            if r.status_code > 399 and r.status_code < 500:
                msg += "\n" + json.dumps(r.json(), sort_keys=True, indent=4) + "\n"
            logging.warning(msg)
            raise ValueError(msg)
        logging.debug("[zenodo:update_deposition] deposition = {}".format(r.json()))
        return r.json()

    def retrieve_deposition(self, deposition_id):
        """Get an existing deposition provided id or url."""
        if deposition_id.startswith("http"):
            url = urlparse(deposition_id).path
        else:
            url = "/api/deposit/depositions/{}".format(deposition_id)

        r = self.request(
            url, method="GET", headers={"Content-Type": "application/json"}
        )

        try:
            r.raise_for_status()
        except requests.HTTPError as exc:
            msg = "Failed to get the deposition (id={}).".format(deposition_id)
            msg += " Server returned: " + str(exc)
            if r.status_code > 399 and r.status_code < 500:
                msg += "\n" + json.dumps(r.json(), sort_keys=True, indent=4) + "\n"
            logging.warning(msg)
            raise ValueError(msg)
        logging.debug("[zenodo:retrieve_deposition] deposition = {}".format(r.json()))
        return r.json()

    def remove_files(self, deposition):
        for fileobj in deposition["files"]:
            r = self.request(
                "/api/deposit/depositions/{dep_id}/files/{file_id}".format(
                    dep_id=deposition["id"], file_id=fileobj["id"]
                ),
                method="DELETE",
            )

            try:
                r.raise_for_status()
            except requests.HTTPError as exc:
                msg = "Failed to remove file (id={}) from the deposition (id={}).".format(
                    fileobj["id"], deposition["id"]
                )
                msg += " Server returned: " + str(exc)
                if r.status_code > 399 and r.status_code < 500:
                    msg += "\n" + json.dumps(r.json(), sort_keys=True, indent=4) + "\n"
                logging.warning(msg)
                raise ValueError(msg)

    def publish(self):
        """Publish the specified Tale to Zenodo."""
        if self.published:
            old_deposition = self.retrieve_deposition(
                self.publication_info["repository_id"]
            )
            deposition = self.create_new_version(old_deposition)

            # NOTE: If Tale was already published the only way to "update" the
            # payload is to delete the file and upload a new file.
            self.remove_files(deposition)

            deposition.update(self.get_tale_metadata())
            deposition = self.update_deposition(deposition)

            # TODO: Discard version if something goes wrong...
        else:
            deposition = self.create_deposition()
        self.publish_version(deposition)

    def publish_version(self, deposition):
        # Zenodo is nice and gives up preserved DOI at this point.
        # We should update the Tale before publishing so that
        # the manifest reflects that, shouldn't we?
        # TODO: update Tale's manifest
        try:
            # TODO: not sure why, but prereserve_doi doesn't work in sandbox occasionally
            doi = deposition["metadata"]["prereserve_doi"]["doi"]
        except KeyError:
            doi = None

        # Add a self reference pointing to WT
        msg = (
            "Run this Tale on Whole Tale by clicking "
            '<a href="{girder_url}/api/v1/integration/zenodo?{query}">here</a>.'
        ).format(
            girder_url=DEPLOYMENT.girder_url,
            query=urlencode({"doi": doi, "resource_server": self.resource_server}),
        )
        deposition["metadata"]["notes"] = msg
        try:
            deposition = self.update_deposition(deposition)
        except Exception as exc:
            # We should delete deposition if something goes wrong
            self.rollback(deposition)
            raise exc

        try:
            self.deposit_tale(deposition)
        except Exception as exc:
            # We should delete deposition if something goes wrong
            self.rollback(deposition)
            raise exc

        # TODO: since it's a two step process and here's the moment when we have a
        # "camera-ready" Tale deposited and ready for final click, it'd be stupendous
        # if we had a UI component...
        if self.draft:
            published_url = deposition["links"]["self"]
        else:
            # Rollback is not possible afterwards...
            deposition = self.publish_deposition(deposition)
            doi = deposition["doi"]
            published_url = deposition["links"]["record_html"]

        self.update_progress(
            message="Your Tale has successfully been published to " + published_url
        )

        if doi and not doi.startswith("doi:"):
            doi = "doi:{}".format(doi)

        publish_info = {
            "pid": doi,
            "uri": published_url,
            "date": datetime.datetime.utcnow().isoformat(),
            "repository_id": str(deposition["id"]),
            "repository": self.resource_server,
            "versionId": self.version_id,
        }
        if self.published:
            self.tale["publishInfo"][self._published_info_index].update(publish_info)
        else:
            self.tale["publishInfo"].append(publish_info)

        # Update the Tale with a reference to the published resource
        try:
            self.gc.put("tale/{}".format(self.tale["_id"]), json=self.tale)
        except Exception as e:
            msg = "Error updating Tale {}".format(str(e))
            logging.warning(msg)
            raise ValueError(msg)

    def rollback(self, deposition):
        """Delete an unpublished deposition."""
        r = self.request(
            "/api/deposit/depositions/{}".format(deposition["id"]), method="DELETE"
        )
        logging.warning("Deleting returned : {}".format(r.status_code))
        assert r.status_code == 204  # Docs say it should return 201...

    def request(self, path, method="GET", data=None, files=None, headers=None):
        """Wrap commonly used requests to pass header and create url."""
        headers = headers or {}
        headers.update({"Authorization": "Bearer {}".format(self.access_token)})
        url = "https://{}{}".format(self.resource_server, path)

        if method.upper() == "GET":
            return requests.get(url, headers=headers)
        elif method.upper() == "PUT":
            return requests.put(url, data=data, headers=headers)
        elif method.upper() == "POST":
            return requests.post(url, data=data, files=files, headers=headers)
        elif method.upper() == "DELETE":
            return requests.delete(url, headers=headers)
