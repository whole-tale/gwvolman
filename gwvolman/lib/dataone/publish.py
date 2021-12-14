import datetime
from hashlib import md5
import io
import json
import jwt
import logging
import mimetypes
import os
from pathlib import Path
import sys
import tempfile
from typing import Tuple, Union
import zipfile

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from d1_client.mnclient_2_0 import MemberNodeClient_2_0
from d1_common.types.exceptions import DataONEException, InvalidToken, NotFound
from d1_common.types.generated.dataoneTypes_v2_0 import SystemMetadata
from d1_common.types import dataoneTypes
from d1_common.env import D1_ENV_DICT

from .metadata import DataONEMetadata
from gwvolman.lib.publish_provider import PublishProvider


_JWT_OPTS = {"verify_signature": False, "verify_exp": not hasattr(sys, '_called_from_test')}


class DataONEPublishProvider(PublishProvider):
    def __init__(
        self, gc, tale_id: str, token: dict, version_id,
            job_manager=None, dataone_node: str=None
    ):
        """
        Initialize a DataONE Publish Provider. This object holds information about the
        repository that it's publishing to. For example, it holds the DataONE client
        which is used to interact with DataONE.

        :param gc:  Authenticated Girder client
        :param tale_id: The ID of the Tale being published
        :param token: The user's JWT token
        :param version_id: The Tale version being published
        :param job_manager:  Optional job manager
        :param dataone_node: The DataONE member node endpoint
        """
        super().__init__(gc, tale_id, token, version_id, job_manager=job_manager)
        self.dataone_node: str = dataone_node
        self.dataone_auth_token = token["access_token"]
        self.coordinating_node:str = "https://{}/cn/".format(token["resource_server"])
        self.client: MemberNodeClient_2_0 = None

    def _create_client(self):
        """
        Create a client object that is used to interface with a DataONE
        member node.  The auth_token is the jwt token from DataONE.
        Close the connection between uploads otherwise some uploads will fail.
        """
        try:
            self.client = MemberNodeClient_2_0(
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

        # Files to ignore when uploading. These come from the bagged Tale that's used
        # to upload the files.
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

        # Throw a ValueError if a client can't be created
        self._create_client()

        # Make sure that the JWT is in good form and has the right fields
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
        stream = self.gc.sendRestRequest(
            "get",
            "tale/{}/export".format(self.tale["_id"]),
            parameters={"taleFormat": "bagit", "versionId": self.version_id},
            stream=True,
            jsonResp=False,
        )
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:

            # Write the zip file
            for chunk in stream.iter_content(chunk_size=65536):
                tmp.write(chunk)
            tmp.seek(0)

            # Read the zipfile
            zip_file = zipfile.ZipFile(tmp, "r")
            files = [fname for fname in zip_file.namelist() if not fname.endswith("/")]

            # Now we know the number of steps for progress
            steps = len(files) + 5

            version_path = str(self.version_id)
            # Get the manifest
            manifest_path = f"{version_path}/metadata/manifest.json"
            manifest_size = zip_file.getinfo(manifest_path).file_size
            with zip_file.open(manifest_path) as f:
                data = f.read()
                manifest_md5 = md5(data).hexdigest()
                manifest = json.loads(data.decode("utf-8"))

            # Read the license text
            license_path = f"{version_path}/data/LICENSE"
            with zip_file.open(license_path) as f:
                license_text = str(f.read().decode("utf-8"))

            # Get the environment
            environment_path = f"{version_path}/metadata/environment.json"
            environment_size = zip_file.getinfo(environment_path).file_size
            with zip_file.open(environment_path) as f:
                data = f.read()
                environment_md5 = md5(data).hexdigest()

            # Get the run-local.sh
            run_local_path = f"{version_path}/run-local.sh"
            run_local_size = zip_file.getinfo(run_local_path).file_size
            with zip_file.open(run_local_path) as f:
                data = f.read()
                run_local_md5 = md5(data).hexdigest()

            # Get the fetch.txt
            fetch_path = f"{version_path}/fetch.txt"
            fetch_size = zip_file.getinfo(fetch_path).file_size
            with zip_file.open(fetch_path) as f:
                data = f.read()
                fetch_md5 = md5(data).hexdigest()

            # Get the README.md
            readme_path = f"{version_path}/README.md"
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

            # Keep track of uploaded objects because the resource needs them
            uploaded_pids = []
            try:
                for fpath in files:
                    with zip_file.open(fpath) as f:
                        relpath = f"./{os.sep.join(Path(fpath).parts[2:])}"
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
            except Exception as e:
                logging.error("There was an error while uploading %s\n%s", fname, str(e))
                raise ValueError(f'There was a fatal error while uploading {fname}. Please '\
                                 'contact the support team.')

            self.job_manager.updateProgress(
                message="Uploading EML metadata record",
                total=100,
                current=int(step / steps * 100),
            )
            step += 1

            if self.published:
                last_eml_pid = self.tale["publishInfo"][self._published_info_index]['pid']
                try:
                    previous_sysmeta = self.client.getSystemMetadata(last_eml_pid)
                    # Use the system metadata from the previous EML document
                    new_sysmeta = self.update_sysmeta(previous_sysmeta, eml_doc, eml_pid)
                    self._obsolete_object(last_eml_pid, eml_pid, eml_doc, new_sysmeta)

                except (IndexError, NotFound, Exception) as e:
                    # The blanket Exception is to catch any sort of potential crashing from
                    # a failed dataone request
                    # Publish an initial version
                    raise ValueError(f'Failed to update the previous version of the Tale {e}')
            else:
                eml_meta = metadata.generate_system_metadata(
                    pid=eml_pid, name='metadata.xml',
                    format_id='eml://ecoinformatics.org/eml-2.1.1',
                    size=len(eml_doc),
                    md5=md5(eml_doc).hexdigest(),
                    rights_holder=user_id)
                try:
                    self._upload_file(
                        pid=eml_pid,
                        file_object=io.BytesIO(eml_doc),
                        system_metadata=eml_meta,
                    )
                except Exception as e:
                    logging.error(f'Failed to upload the EML document {e}')
                    raise ValueError(f'Failed to upload the EML document. Please ensure '
                                     f'the files in the Tale are valid.')
            uploaded_pids.append(eml_pid)

            self.job_manager.updateProgress(
                message="Uploading resource map",
                total=100,
                current=int(step / steps * 100),
            )
            step += 1

            # Create ORE
            res_pid = self._generate_pid(scheme="UUID")
            metadata.create_resource_map(res_pid, eml_pid, uploaded_pids)
            metadata.set_related_identifiers(manifest, eml_pid, self.tale,
                                                self.dataone_node,  self.gc)
            res_map = metadata.resource_map.serialize()
            # Update the resource map with citations
            # Turn the resource map into readable bytes
            try:
                res_map = res_map.encode()
            except AttributeError:
                pass

            res_meta = metadata.generate_system_metadata(
                pid=res_pid,
                name=str(),
                format_id="http://www.openarchives.org/ore/terms",
                size=len(res_map),
                md5=md5(res_map).hexdigest(),
                rights_holder=self._get_http_orcid(user_id),
            )

            try:
                self._upload_file(
                    pid=res_pid,
                    file_object=io.BytesIO(res_map),
                    system_metadata=res_meta,
                )
            except Exception as e:
                logging.error(f'Failed to upload the resource map {e}')
                raise ValueError(f'Failed to upload the resource map.')

            package_url = self._get_dataone_package_url(
                self.coordinating_node, res_pid
            )

            self.job_manager.updateProgress(
                message="Your Tale has successfully been published to DataONE.",
                total=100,
                current=100,
            )

            publish_info = {
                "pid": eml_pid,
                "repository": self.resource_server,
                "repository_id": res_pid,
                "uri": package_url,
                "date": datetime.datetime.utcnow().isoformat(),
            }

            if self.published:
                self.tale["publishInfo"][self._published_info_index].update(publish_info)
            else:
                self.tale["publishInfo"].append(publish_info)

            try:
                self.gc.put("tale/{}".format(self.tale["_id"]), json=self.tale)
            except Exception as e:
                logging.error('Error updating Tale {e}')
                raise ValueError("There was an error while updating the Tale.")

    @staticmethod
    def _get_manifest_file_info(manifest, relpath):
        for file in manifest["aggregates"]:
            if file["uri"] == relpath:
                md5_checksum = file["wt:md5"]
                # mimeType = file['mimeType']
                size = file["wt:size"]
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
        if member_node in D1_ENV_DICT["prod"]["base_url"]:
            return str("https://search.dataone.org/view/" + pid)
        else:
            return str("https://dev.nceas.ucsb.edu/view/" + pid)

    @staticmethod
    def _get_http_orcid(user_id: str)->str:
        """
        HTTPS links will break the resource map. The ORCID IDs are stored
        as HTTPS, so the https needs to be changed to http. This method is
        used to perform that conversion.
        :param user_id: The user's ORCID
        :return: A URI that's HTTP instead of HTTPS
        """
        if bool(user_id.find("orcid.org")):
            return urlparse(user_id)._replace(scheme="http").geturl()
        return user_id

    def _extract_user_info(self) -> Tuple[str, str]:
        """
        Takes a JWT and extracts the `userId` and `fullName` fields.
        This is used as the package's owner and contact.
        :return: The ORCID ID, and the user's full name
        """
        jwt_token = jwt.PyJWT().decode(self.dataone_auth_token, options=_JWT_OPTS)
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

    def _obsolete_object(self, old_pid, new_pid, new_object, sysmeta):
        """
        Obsoletes an object with a new one. The coordinating node will handle modifying the
        system metadata with the appropriate obsoletion flags. It's most likely that this should
        only be called with a new resource map and EML document.
        :param old_pid: The pid of the existing object
        :param new_pid: The new package resource map pid
        :param new_object: The new object that is replacing the existing one
        :param sysmeta: The new object's system metadata document
        :return: None
        """
        try:
            self.client.update(old_pid, io.BytesIO(new_object), new_pid, sysmeta)

        except DataONEException as e:
            logging.error('Error obsoleting package {} with {}. {}'.format(old_pid, new_pid, e))
            raise ValueError('Failed to obsolete the previous version of the Tale. {}'.format(e))

    @staticmethod
    def update_sysmeta(sysmeta: SystemMetadata, bytes_to_upload: Union[str, bytes], new_pid):
        """
        Updates a system metadata document to describe a different object. The idea is that the
        DataONE server will set various fields on the system metadata (AuthortativeMemberNode, for example)
        and when obsoleting an object-those fields are desired. Some fields like the checksum and file size will
        be different and need to be updated, which is what this method is for.
        :param sysmeta: The system metadata document
        :param bytes_to_upload: The bytes that are being uploaded to DataONE
        :param new_pid: The pid of the object representing the bytes
        """
        if not isinstance(bytes_to_upload, bytes):
            if isinstance(bytes_to_upload, str):
                bytes_to_upload = bytes_to_upload.encode("utf-8")
            else:
                raise ValueError('Unable to convert the data object with pid {} to bytes'.format(new_pid))
        size = len(bytes_to_upload)
        checksum = md5(bytes_to_upload).hexdigest()
        sysmeta.identifier = str(new_pid)
        sysmeta.size = size
        sysmeta.checksum = dataoneTypes.checksum(str(checksum))
        sysmeta.checksum.algorithm = 'MD5'
        sysmeta.obsoletes = None
        return sysmeta
