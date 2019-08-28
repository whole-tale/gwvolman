"""Whole Tale publisher provider for Zenodo."""

import datetime
import json
import logging
import requests
import tempfile

from markdown import Markdown
from lxml.html.clean import Cleaner

from .publish_provider import PublishProvider


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
            parameters={"taleFormat": "bagit"},
            stream=True,
            jsonResp=False,
        )
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:

            # Write the zip file
            for chunk in stream.iter_content(chunk_size=65536):
                tmp.write(chunk)
            tmp.seek(0)

            r = self.request(
                "/api/deposit/depositions/{}/files".format(deposition["id"]),
                method="POST",
                data={"filename": "{}.zip".format(self.tale["_id"])},
                files={"file": tmp},
            )

            try:
                r.raise_for_status()
            except requests.HTTPError as exc:
                msg = "Failed to upload to a deposition (id:{}).".format(
                    deposition["id"]
                )
                msg += " Server returned: " + str(exc)
                logging.warning(msg)
                raise ValueError(msg)
            logging.debug(
                "[zenodo:deposit_tale] POST depositions/{}/files = {}".format(
                    deposition["id"], r.json()
                )
            )

    def get_tale_metadata(self):
        """Convert the Tale metadata to a Zenodo metadata."""
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
                "access_right": "open",
                "license": self.tale["licenseSPDX"].lower(),
                "prereserve_doi": True,
                "related_identifiers": [
                    {"relation": "cites", "identifier": ds["identifier"]}
                    for ds in self.manifest.get("Datasets", [])
                ],
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
            logging.warning(msg)
            raise ValueError(msg)
        logging.debug("[zenodo:create_deposition] deposition = {}".format(r.json()))
        return r.json()

    def publish_deposition(self, deposition):
        """Publish publish, I mean REALLY publish."""
        r = self.request(
            "/api/deposit/depositions/{}/actions/publish".format(deposition["id"]),
            method="POST",
        )
        assert r.status_code == 202
        logging.debug("[zenodo:publish_deposition] deposition = {}".format(r.json()))
        return r.json()

    def publish(self):
        """Publish the specified Tale to Zenodo."""
        deposition = self.create_deposition()

        # Zenodo is nice and gives up preserved DOI at this point.
        # We should update the Tale before publishing so that
        # the manifest reflects that, shouldn't we?
        # TODO: update Tale's manifest

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
            try:
                # TODO: not sure why, but prereserve_doi doesn't work in sandbox
                doi = deposition["metadata"]["prereserve_doi"]["doi"]
            except KeyError:
                doi = None
            published_url = deposition["links"]["self"]
        else:
            # Rollback is not possible afterwards...
            deposition = self.publish_deposition(deposition)
            doi = deposition["doi"]
            published_url = deposition["links"]["doi"]

        logging.warning(deposition)
        self.update_progress(
            message="Your Tale has successfully been published to " + published_url
        )

        publish_info = {
            "pid": doi,
            "uri": published_url,
            "date": datetime.datetime.utcnow().isoformat(),
        }
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
