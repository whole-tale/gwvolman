import datetime
from hashlib import md5
import io
import json
import jwt
import logging
import mimetypes
import os
import tempfile
from typing import Tuple, Union
import zipfile

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from d1_client.mnclient_2_0 import MemberNodeClient_2_0
from d1_common.types.exceptions import DataONEException, InvalidToken
from d1_common.types.generated.dataoneTypes_v2_0 import SystemMetadata
from d1_common.env import D1_ENV_DICT

from .metadata import DataONEMetadata
from gwvolman.lib.publish_provider import PublishProvider


class DataONEPublishProvider(PublishProvider):
    def __init__(
        self, gc, tale_id, token, draft=False, job_manager=None, dataone_node=None
    ):
        """
        Initiliaze DataONE Publish Provider.

        :param gc:  Authenticated Girder client
        :param job_manager:  Optional job manager
        :param dataone_node: The DataONE member node endpoint
        :param dataone_auth_token: The user's DataONE JWT
        :param coordinating_node: URL to the coordinating node
        :type dataone_node: str
        :type dataone_auth_token: str
        :type coordinating_node: str
        """
        super().__init__(gc, tale_id, token, draft=draft, job_manager=job_manager)
        self.dataone_node = dataone_node
        self.dataone_auth_token = token["access_token"]
        self.coordinating_node = "https://{}/cn/".format(token["resource_server"])
        self.client: MemberNodeClient_2_0 = None

    def _create_client(self):
        """
        Create a client object that is used to interface with a DataONE
        member node.  The auth_token is the jwt token from DataONE.
        Close the connection between uploads otherwise some uploads will fail.
        """
        try:
            return MemberNodeClient_2_0(
                self.dataone_node,
                **{
                    "headers": {
                        "Authorization": "Bearer " + self.dataone_auth_token,
                        "Connection": "close",
                    },
                    "user_agent": "safari",
                }
            )
        except InvalidToken as e:
            logging.warning(e)
            raise ValueError("Invalid DataONE JWT token. Please re-authenticate with DataONE.")
        except DataONEException as e:
            logging.warning(e)
            raise ValueError("Failed to establish a connection with the DataONE node.")

    def publish(self):
        """
        Workhorse method that downloads a zip file for a tale then
          * Uploads individual files to DataONE
          * Generates and uploads EML metadata
          * Generates and uploads resource map
          * If provided, updates job manager progress
        """

        # Progress indicators
        step = 1
        steps = 100

        # Files to ignore when uploading
        ignore_files = [
            "tagmanifest-sha256.txt",
            "tagmanifest-md5.txt",
            "manifest-sha256.txt",
            "manifest-md5.txt",
            "bag-info.txt",
            "bagit.txt",
        ]
        self.job_manager.updateProgress(
            message="Connecting to {}".format(self.dataone_node),
            total=100,
            current=int(step / steps * 100),
        )
        step += 1

        # Throw a ValueError if a connection can't be made
        self._create_client()

        user_id, full_orcid_name = self._extract_user_info()
        if not all([user_id, full_orcid_name]):
            raise ValueError(
                "Failed to process your DataONE credentials. "
                "Please ensure you are logged into DataONE."
            )

        self.job_manager.updateProgress(
            message="Exporting Tale", total=100, current=int(step / steps * 100)
        )
        step += 1

        # Export the tale to a temp directory
        url = "tale/{}/export?taleFormat=bagit".format(self.tale["_id"])
        stream = self.gc.sendRestRequest("get", url, stream=True, jsonResp=False)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:

            # Write the zip file
            for chunk in stream.iter_content(chunk_size=65536):
                tmp.write(chunk)
            tmp.seek(0)

            # Read the zipfile
            zip_file = zipfile.ZipFile(tmp, "r")
            files = zip_file.namelist()

            # Now we know the number of steps for progress
            steps = len(files) + 5

            # Get the manifest
            manifest_path = "{}/metadata/manifest.json".format(self.tale["_id"])
            manifest_size = zip_file.getinfo(manifest_path).file_size
            with zip_file.open(manifest_path) as f:
                data = f.read()
                manifest_md5 = md5(data).hexdigest()
                manifest = json.loads(data.decode("utf-8"))

            # Read the license text
            license_path = "{}/data/LICENSE".format(self.tale["_id"])
            with zip_file.open(license_path) as f:
                license_text = str(f.read().decode("utf-8"))

            # Get the environment
            environment_path = "{}/metadata/environment.json".format(self.tale["_id"])
            environment_size = zip_file.getinfo(environment_path).file_size
            with zip_file.open(environment_path) as f:
                data = f.read()
                environment_md5 = md5(data).hexdigest()

            # Get the run-local.sh
            run_local_path = "{}/run-local.sh".format(self.tale["_id"])
            run_local_size = zip_file.getinfo(run_local_path).file_size
            with zip_file.open(run_local_path) as f:
                data = f.read()
                run_local_md5 = md5(data).hexdigest()

            # Get the fetch.txt
            fetch_path = "{}/fetch.txt".format(self.tale["_id"])
            fetch_size = zip_file.getinfo(fetch_path).file_size
            with zip_file.open(fetch_path) as f:
                data = f.read()
                fetch_md5 = md5(data).hexdigest()

            # Get the README.md
            readme_path = "{}/README.md".format(self.tale["_id"])
            readme_size = zip_file.getinfo(readme_path).file_size
            with zip_file.open(readme_path) as f:
                data = f.read()
                readme_md5 = md5(data).hexdigest()

            self.job_manager.updateProgress(
                message="Creating EML document from manifest",
                total=100,
                current=int(step / steps * 100),
            )
            step += 1
            metadata = DataONEMetadata(self.coordinating_node)
            # Create an EML document based on the manifest
            eml_pid = self._generate_pid()
            eml_doc = metadata.create_eml_doc(
                eml_pid,
                manifest,
                user_id,
                manifest_size,
                environment_size,
                run_local_size,
                fetch_size,
                license_text,
            )

            # Keep track of uploaded objects in case we need to rollback
            uploaded_pids = []
            try:
                for fpath in files:
                    with zip_file.open(fpath) as f:
                        relpath = fpath.replace(self.tale["_id"], "..")
                        fname = os.path.basename(fpath)

                        # Skip over the files we want to ignore
                        if fname in ignore_files:
                            continue
                        self.job_manager.updateProgress(
                            message="Uploading file {}".format(fname),
                            total=100,
                            current=int(step / steps * 100),
                        )
                        step += 1

                        file_pid = self._generate_pid(scheme="UUID")

                        mimeType = metadata.check_dataone_mimetype(
                            mimetypes.guess_type(fpath)[0]
                        )

                        if fname == "manifest.json":
                            size, md5_hash = manifest_size, manifest_md5
                        elif fname == "environment.json":
                            size, md5_hash = environment_size, environment_md5
                        elif fname == "run-local.sh":
                            size, md5_hash = run_local_size, run_local_md5
                        elif fname == "fetch.txt":
                            size, md5_hash = fetch_size, fetch_md5
                        elif fname == 'README.md':
                            size, md5_hash = readme_size, readme_md5
                        else:
                            size, md5_hash = self._get_manifest_file_info(manifest, relpath)

                        file_meta = metadata.generate_system_metadata(
                            file_pid, fname, mimeType, size, md5_hash, user_id
                        )

                        self._upload_file(
                            pid=file_pid,
                            file_object=f.read(),
                            system_metadata=file_meta,
                        )
                        uploaded_pids.append(file_pid)

                self.job_manager.updateProgress(
                    message="Uploading EML metadata record",
                    total=100,
                    current=int(step / steps * 100),
                )
                step += 1

                # Upload the EML document and system metadata
                eml_meta = metadata.generate_system_metadata(
                    pid=eml_pid,
                    name="metadata.xml",
                    format_id="eml://ecoinformatics.org/eml-2.1.1",
                    size=len(eml_doc),
                    md5=md5(eml_doc).hexdigest(),
                    rights_holder=user_id,
                )

                # This fails with:
                #   The supplied system metadata is invalid. The obsoletes
                #   field cannot have a value when creating entries.
                # if tale['publishInfo']:
                #    old_pid = tale['publishInfo'][-1]['pid']
                #    eml_meta.obsoletes = old_pid

                self._upload_file(
                    pid=eml_pid,
                    file_object=io.BytesIO(eml_doc),
                    system_metadata=eml_meta,
                )

                uploaded_pids.append(eml_pid)

                # Update the tale now that it has been published
                if "publishInfo" not in self.tale:
                    self.tale["publishInfo"] = []

                self.job_manager.updateProgress(
                    message="Uploading resource map",
                    total=100,
                    current=int(step / steps * 100),
                )
                step += 1

                # Create ORE
                res_pid = self._generate_pid(scheme="UUID")
                metadata.create_resource_map(res_pid, eml_pid, uploaded_pids)
                metadata.set_related_identifiers(manifest, eml_pid)
                res_map = metadata.resource_map.serialize()
                # Update the resource map with citations
                # Turn the resource map into readable bytes
                res_map = res_map.serialize()
                res_meta = metadata.generate_system_metadata(
                    pid=res_pid,
                    name=str(),
                    format_id="http://www.openarchives.org/ore/terms",
                    size=len(res_map),
                    md5=md5(res_map).hexdigest(),
                    rights_holder=self._get_resource_map_user(user_id),
                )

                self._upload_file(
                    pid=res_pid,
                    file_object=io.BytesIO(res_map),
                    system_metadata=res_meta,
                )
                package_url = self._get_dataone_package_url(
                    self.coordinating_node, res_pid
                )

                self.job_manager.updateProgress(
                    message="Your Tale has successfully been published to DataONE.",
                    total=100,
                    current=100,
                )

                self.tale["publishInfo"].append(
                    {
                        "pid": eml_pid,
                        "repository": self.dataone_node,
                        "repository_id": res_pid,
                        "uri": package_url,
                        "date": datetime.datetime.utcnow().isoformat(),
                    }
                )
                try:
                    self.gc.put("tale/{}".format(self.tale["_id"]), json=self.tale)
                except Exception as e:
                    logging.warning("Error updating Tale {}".format(str(e)))
                    raise ValueError("Error updating Tale {}".format(str(e)))

            except Exception as e:
                logging.warning("Error. Should rollback... {}".format(str(e)))
                # Getting permission denied on delete
                # for pid in uploaded_pids:
                #    try:
                #        logging.info("Deleting pid {} if I could...".format(
                #            pid))
                #        client.delete(pid)
                #    except Exception as e:
                #        logging.warning('Error deleting pid {}: {}'.format(
                #            pid, str(e)))
                raise

    @staticmethod
    def _get_manifest_file_info(manifest, relpath):
        for file in manifest["aggregates"]:
            if file["uri"] == relpath:
                md5_checksum = file["md5"]
                # mimeType = file['mimeType']
                size = file["size"]
                return size, md5_checksum
        return None, None

    def _upload_file(self, pid: str, file_object: Union[str, io.BytesIO],
                     system_metadata: SystemMetadata):
        """
        Uploads two files to a DataONE member node. The first is an object,
        which is just a data file.  The second is a metadata file describing
        the file object.

        :param pid: The pid of the data object
        :param file_object: The file object that will be uploaded
        :param system_metadata: The metadata object describing the file object
        """

        try:
            self.client.create(pid, file_object, system_metadata)
        except DataONEException as e:
            logging.warning("Error uploading file to DataONE {} {}".format(pid, str(e)))
            raise

    @staticmethod
    def _get_dataone_package_url(member_node: str, pid: str):
        """
        Given a repository url and a pid, construct a url that should
         be the package's landing page.

        :param member_node: The member node that the package is on
        :param pid: The package pid
        :return: The package landing page
        """
        if member_node in D1_ENV_DICT["prod"]:
            return str("https://search.dataone.org/view/" + pid)
        else:
            return str("https://dev.nceas.ucsb.edu/view/" + pid)

    def _get_resource_map_user(self, user_id: str) -> str:
        """
        HTTPS links will break the resource map. Use this function
        to get a properly constructed username from a user's ID.
        :param user_id: The user ORCID
        :return: An http version of the user
        """
        if bool(user_id.find("orcid.org")):
            return self._make_url_http(user_id)
        return user_id

    def _extract_user_info(self) -> Tuple[str, str]:
        """
        Takes a JWT and extracts the `userId` and `fullName` fields.
        This is used as the package's owner and contact.
        :return: The ORCID ID, and the user's full name
        """
        jwt_token = jwt.PyJWT().decode(self.dataone_auth_token, options={"verify_signature": False})
        user_id = jwt_token.get("userId")
        name = jwt_token.get("fullName")
        return user_id, name

    @staticmethod
    def _is_orcid_id(user_id: str) -> bool:
        """
        Checks whether a string is a link to an ORCID account
        :param user_id: The string that may contain the ORCID account
        :return: True/False if it is or isn't
        """
        return bool(user_id.find("orcid.org"))

    @staticmethod
    def _make_url_https(url: str) -> str:
        """
        Given an http url, return it as https

        :param url: The http url
        :return: The url as https
        """
        parsed = urlparse(url)
        return parsed._replace(scheme="https").geturl()

    @staticmethod
    def _make_url_http(url: str) -> str:
        """
        Given an https url, make it http
        :param url: The http url
        :return: The url as https
        """
        parsed = urlparse(url)
        return parsed._replace(scheme="http").geturl()

    def _generate_pid(self, scheme: str="DOI"):
        """
        Generates a DataONE identifier.
        :return: A valid DataONE identifier
        """
        try:
            return self.client.generateIdentifier(scheme=scheme).value()
        except InvalidToken as e:
            logging.warning(e)
            raise ValueError("Invalid DataONE JWT. Please refresh the token.")
        except DataONEException as e:
            logging.warning(e)
            raise ValueError("Failed to generate identifier.")
