import datetime
from hashlib import md5
from typing import Union
from pyxb import binding
import io
import json
import jwt
import logging
import mimetypes
import os
import tempfile
import zipfile

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from d1_client.mnclient_2_0 import MemberNodeClient_2_0
from d1_common.types.exceptions import DataONEException, InvalidToken,\
    NotFound, ServiceFailure, InvalidRequest
from d1_common.types import dataoneTypes
from d1_common.env import D1_ENV_DICT
from d1_common.types.generated.dataoneTypes_v2_0 import SystemMetadata

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
        self.dataone_node: str = dataone_node
        self.dataone_auth_token: str = token["access_token"]
        self.coordinating_node: str = "https://{}/cn/".format(token["resource_server"])
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
            raise ValueError("Invalid JWT token. Please re-authenticate with DataONE.")
        except DataONEException as e:
            logging.warning(e)
            raise ValueError("Failed to establish connection with the DataONE node.")

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

        # Let it throw in case the connection fails
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
            zip = zipfile.ZipFile(tmp, "r")
            files = zip.namelist()

            # Now we know the number of steps for progress
            steps = len(files) + 5

            # Get the manifest
            manifest_path = "{}/metadata/manifest.json".format(self.tale["_id"])
            manifest_size = zip.getinfo(manifest_path).file_size
            with zip.open(manifest_path) as f:
                data = f.read()
                manifest_md5 = md5(data).hexdigest()
                manifest = json.loads(data.decode("utf-8"))

            # Read the license text
            license_path = "{}/data/LICENSE".format(self.tale["_id"])
            with zip.open(license_path) as f:
                license_text = str(f.read().decode("utf-8"))

            # Get the environment
            environment_path = "{}/metadata/environment.json".format(self.tale["_id"])
            environment_size = zip.getinfo(environment_path).file_size
            with zip.open(environment_path) as f:
                data = f.read()
                environment_md5 = md5(data).hexdigest()

            # Get the run-local.sh
            run_local_path = "{}/run-local.sh".format(self.tale["_id"])
            run_local_size = zip.getinfo(run_local_path).file_size
            with zip.open(run_local_path) as f:
                data = f.read()
                run_local_md5 = md5(data).hexdigest()

            # Get the fetch.txt
            fetch_path = "{}/fetch.txt".format(self.tale["_id"])
            fetch_size = zip.getinfo(fetch_path).file_size
            with zip.open(fetch_path) as f:
                data = f.read()
                fetch_md5 = md5(data).hexdigest()

            # Get the README.md
            readme_path = "{}/README.md".format(self.tale["_id"])
            readme_size = zip.getinfo(readme_path).file_size
            with zip.open(readme_path) as f:
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
                    with zip.open(fpath) as f:
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
                            size, hash = manifest_size, manifest_md5
                        elif fname == "environment.json":
                            size, hash = environment_size, environment_md5
                        elif fname == "run-local.sh":
                            size, hash = run_local_size, run_local_md5
                        elif fname == "fetch.txt":
                            size, hash = fetch_size, fetch_md5
                        elif fname == 'README.md':
                            size, hash = readme_size, readme_md5
                        else:
                            size, hash = self._get_manifest_file_info(manifest, relpath)

                        file_meta = metadata.generate_system_metadata(
                            file_pid, fname, mimeType, size, hash, user_id
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

                try:
                    last_eml_pid = self.tale['publishInfo']['DataONE']['pid']
                    previous_sysmeta = self.client.getSystemMetadata(last_eml_pid)
                    # Use the system metadata from the previous EML document
                    new_sysmeta = self.update_sysmeta(previous_sysmeta, eml_doc, eml_pid)
                    self._obsolete_object(last_eml_pid, eml_pid, eml_doc, new_sysmeta)



                except (IndexError, NotFound, Exception) as e:
                    # Then this hasn't been published before and could not be updated
                    eml_meta = metadata.generate_system_metadata(
                        pid=eml_pid, name='metadata.xml',
                        format_id='eml://ecoinformatics.org/eml-2.1.1',
                        size=len(eml_doc),
                        md5=md5(eml_doc).hexdigest(),
                        rights_holder=user_id)
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

                # Create the resource map's pid
                res_pid = self._generate_pid(scheme="UUID")
                # Generate the resource map
                metadata.create_resource_map(res_pid, eml_pid, uploaded_pids)
                # Update the resource map with any datacite properties
                metadata.set_related_identifiers(manifest, eml_pid, self.tale,
                                                 self.dataone_node, self.gc)
                # Turn the resource map into readable bytes
                res_map = metadata.resource_map.serialize()
                res_meta = metadata.generate_system_metadata(
                    pid=res_pid,
                    name=str(),
                    format_id="http://www.openarchives.org/ore/terms",
                    size=len(res_map),
                    md5=md5(res_map).hexdigest(),
                    rights_holder=self._get_http_orcid(user_id),
                )

                self._upload_file(
                    pid=res_pid,
                    file_object=io.BytesIO(res_map),
                    system_metadata=res_meta,
                )
                package_url = self._get_dataone_package_url(
                    self.coordinating_node, res_pid)

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
                    raise ValueError("There was an errror while updating the Tale's "
                                     "published location {}".format(str(e)))

            except Exception as e:
                raise

    @staticmethod
    def _get_manifest_file_info(manifest, relpath):
            for file in manifest["aggregates"]:
                try:
                    if file["uri"] == relpath:
                        md5 = file["md5"]
                        size = file["size"]
                        return size, md5
                except KeyError:
                    # It should be okay if there's a key error, continue to return
                    pass
            return None, None


    def _upload_file(self, pid: str, file_object: Union[str, io.BytesIO], system_metadata: SystemMetadata):
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
    def _get_dataone_package_url(member_node, pid):
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

    def _extract_user_info(self):
        """
        Takes a JWT and extracts the `userId` and `fullName` fields.
        This is used as the package's owner and contact.
        :param jwt_token: The decoded JWT
        :type jwt_token: str
        :return: The ORCID ID
        :rtype: str, None if failure
        """
        jwt_token = jwt.PyJWT().decode(self.dataone_auth_token, verify=False)
        user_id = jwt_token.get("userId")
        name = jwt_token.get("fullName")
        return user_id, name

    def _generate_pid(self, scheme="DOI") -> binding.datatypes.string:
        """
        Generates a DataONE identifier. The identifier type is generated from
        and xml definition, hence the binding.datatypes.string return value

        :return: A valid. reserved DataONE identifier
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

        except (ServiceFailure, InvalidRequest) as e:
            logging.error('Error obsoleting package {} with {}. {}'.format(old_pid, new_pid, e))
            raise ValueError('Failed to obsolete the previous version of the Tale')

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
