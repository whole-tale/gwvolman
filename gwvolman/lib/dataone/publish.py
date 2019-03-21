from hashlib import md5
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
from d1_common.types.exceptions import DataONEException

from .metadata import DataONEMetadata

from .constants import DataONELocations

from gwvolman.lib.publish_provider import PublishProvider


class DataONEPublishProvider(PublishProvider):

    def _connect(self, dataone_node, dataone_auth_token):
        """
        Create a client object that is used to interface with a DataONE
        member node.  The auth_token is the jwt token from DataONE.
        Close the connection between uploads otherwise some uploads will fail.
        CW: What does this mean?
        """
        try:
            return MemberNodeClient_2_0(dataone_node,
               **{
                    "headers": {
                        "Authorization": "Bearer " + dataone_auth_token,
                        "Connection": "close"
                    },
                    "user_agent": "safari",
                }
            )
        except DataONEException as e:
            logging.warning(e)
            raise ValueError('Failed to establish connection with DataONE.')

    def publish(self, tale_id, gc, dataone_node, dataone_auth_token,
                job_manager=None):
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

        if job_manager:
            job_manager.updateProgress(
                message='Connecting to {}'.format(dataone_node),
                total=100, current=int(step/steps*100))
        step += 1

        try:
            client = self._connect(dataone_node, dataone_auth_token)
        except DataONEException as e:
            logging.warning(e)
            # We'll want to exit if we can't create the client
            raise ValueError('Failed to establish connection with DataONE.')

        user_id, full_orcid_name = self._extract_user_info(dataone_auth_token)
        if not all([user_id, full_orcid_name]):
            raise ValueError('Failed to process your DataONE credentials. '
                             'Please ensure you are logged into DataONE.')

        if job_manager:
            job_manager.updateProgress(
                message='Exporting Tale',
                total=100, current=int(step/steps*100))
        step += 1

        # Export the tale to a temp directory
        url = 'tale/{}/export'.format(tale_id)
        stream = gc.sendRestRequest('get', url, stream=True, jsonResp=False)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp:

            # Write the zip file
            for chunk in stream.iter_content(chunk_size=65536):
                tmp.write(chunk)
            tmp.seek(0)

            # Read the zipfile
            zip = zipfile.ZipFile(tmp, 'r')
            files = zip.namelist()

            # Now we know the number of steps for progress
            steps = len(files) + 5

            # Get the manifest
            manifest_path = '{}/metadata/manifest.json'.format(tale_id)
            manifest_size = zip.getinfo(manifest_path).file_size
            with zip.open(manifest_path) as f:
                data = f.read()
                manifest_md5 = md5(data).hexdigest()
                manifest = json.loads(data.decode('utf-8'))

            # Get the environment
            environment_path = '{}/environment.txt'.format(tale_id)
            environment_size = zip.getinfo(environment_path).file_size

            if job_manager:
                job_manager.updateProgress(
                    message='Creating EML document from manifest',
                    total=100, current=int(step/steps*100))
            step += 1

            metadata = DataONEMetadata()
            # Create an EML document based on the manifest
            eml_pid, eml_doc = metadata.create_eml_doc(
                manifest, user_id, manifest_size, environment_size)

            # Keep track of uploaded objects in case we need to rollback
            uploaded_pids = []
            try:
                for fpath in files:
                    with zip.open(fpath) as f:
                        relpath = fpath.replace(tale_id, "..")
                        fname = os.path.basename(fpath)

                        if job_manager:
                            job_manager.updateProgress(
                                message='Uploading file {}'.format(fname),
                                total=100, current=int(step/steps*100))
                        step += 1

                        # Generate uuid (TODO: Replace with D1 API call)
                        file_pid = metadata.generate_dataone_guid()

                        mimeType = metadata.get_dataone_mimetype(
                            mimetypes.guess_type(fpath))

                        if fname == 'manifest.json':
                            size, hash = manifest_size, manifest_md5
                        else:
                            size, hash = self._get_manifest_file_info(
                                manifest, relpath)

                        file_meta = metadata.generate_system_metadata(
                                file_pid, fname, mimeType, size, hash, user_id)

                        self._upload_file(client=client, pid=file_pid,
                                          file_object=f.read(),
                                          system_metadata=file_meta)
                        uploaded_pids.append(file_pid)

                # Update the tale now that it has been published
                tale = gc.get('tale/{}/'.format(tale_id))
                if 'publishInfo' not in tale:
                    tale['publishInfo'] = []

                if job_manager:
                    job_manager.updateProgress(
                        message='Uploading EML metadata record',
                        total=100, current=int(step/steps*100))
                step += 1

                # Upload the EML document and system metadata
                eml_meta = metadata.generate_system_metadata(
                               pid=eml_pid, name='metadata.xml',
                               format_id='eml://ecoinformatics.org/eml-2.1.1',
                               size=len(eml_doc),
                               md5=md5(eml_doc).hexdigest(),
                               rights_holder=user_id)

                # This fails with:
                #   The supplied system metadata is invalid. The obsoletes
                #   field cannot have a value when creating entries.
                # if tale['publishInfo']:
                #    old_pid = tale['publishInfo'][-1]['pid']
                #    eml_meta.obsoletes = old_pid

                self._upload_file(client=client, pid=eml_pid,
                                  file_object=io.BytesIO(eml_doc),
                                  system_metadata=eml_meta)

                uploaded_pids.append(eml_pid)

                if job_manager:
                    job_manager.updateProgress(
                        message='Uploading resource map',
                        total=100, current=int(step/steps*100))
                step += 1

                # Create ORE
                res_pid, res_map = metadata.create_resource_map(
                    eml_pid, uploaded_pids)
                res_meta = metadata.generate_system_metadata(
                            pid=res_pid, name=str(),
                            format_id='http://www.openarchives.org/ore/terms',
                            size=len(res_map),
                            md5=md5(res_map).hexdigest(),
                            rights_holder=self._get_resource_map_user(user_id))

                self._upload_file(client=client, pid=res_pid,
                                  file_object=io.BytesIO(res_map),
                                  system_metadata=res_meta)

                package_url = self._get_dataone_package_url(
                    dataone_node, eml_pid)

                if job_manager:
                    job_manager.updateProgress(
                               message='Your Tale has successfully been '
                                       'published to DataONE.',
                               total=100,
                               current=100)

                tale['publishInfo'].append(
                    {
                        'pid': eml_pid,
                        'uri': package_url
                    }
                )
                try:
                    gc.put('tale/{}'.format(tale['_id']), json=tale)
                except Exception as e:
                    logging.warning("Error updating Tale {}".format(str(e)))
                    raise

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

    def _get_manifest_file_info(self, manifest, relpath):
        for file in manifest['aggregates']:
            if file['uri'] == relpath:
                md5 = file['md5']
                # mimeType = file['mimeType']
                size = file['size']
                return size, md5
        return None, None

    def _upload_file(self, client, pid, file_object, system_metadata):
        """
        Uploads two files to a DataONE member node. The first is an object,
        which is just a data file.  The second is a metadata file describing
        the file object.

        :param client: A client for communicating with a member node
        :param pid: The pid of the data object
        :param file_object: The file object that will be uploaded
        :param system_metadata: The metadata object describing the file object
        :type client: MemberNodeClient_2_0
        :type pid: str
        :type file_object: str
        :type system_metadata: d1_common.types.generated.dataoneTypes_v2_0.SystemMetadata
        """
        logging.info("Upload File {} {}".format(
            pid, system_metadata.toxml('utf-8')))

        # TODO do we really need this?
        # pid = check_pid(pid)
        try:
            client.create(pid, file_object, system_metadata)
        except DataONEException as e:
            logging.warning('Error uploading file to DataONE {} {}'.format(
                pid, str(e)))
            raise

    def _get_dataone_package_url(self, member_node, pid):
        """
        Given a repository url and a pid, construct a url that should
         be the package's landing page.

        :param member_node: The member node that the package is on
        :param pid: The package pid
        :return: The package landing page
        """
        if member_node in DataONELocations.prod_mn:
            return str('https://search.dataone.org/view/'+pid)
        elif member_node in DataONELocations.dev_mn:
            return str('https://dev.nceas.ucsb.edu/view/'+pid)

    def _get_resource_map_user(self, user_id):
        """
        HTTPS links will break the resource map. Use this function
        to get a properly constructed username from a user's ID.
        :param user_id: The user ORCID
        :type user_id: str
        :return: An http version of the user
        :rtype: str
        """
        if bool(user_id.find('orcid.org')):
            return self._make_url_http(user_id)
        return user_id

    def _extract_user_info(self, jwt_token):
        """
        Takes a JWT and extracts the `userId` and `fullName` fields.
        This is used as the package's owner and contact.
        :param jwt_token: The decoded JWT
        :type jwt_token: str
        :return: The ORCID ID
        :rtype: str, None if failure
        """
        jwt_token = jwt.decode(jwt_token, verify=False)
        user_id = jwt_token.get('userId')
        name = jwt_token.get('fullName')
        return user_id, name

    def _is_orcid_id(self, user_id):
        """
        Checks whether a string is a link to an ORCID account
        :param user_id: The string that may contain the ORCID account
        :type user_id: str
        :return: True/False if it is or isn't
        :rtype: bool
        """
        return bool(user_id.find('orcid.org'))

    def _make_url_https(self, url):
        """
        Given an http url, return it as https

        :param url: The http url
        :type url: str
        :return: The url as https
        :rtype: str
        """
        parsed = urlparse(url)
        return parsed._replace(scheme="https").geturl()

    def _make_url_http(self, url):
        """
        Given an https url, make it http
        :param url: The http url
        :type url: str
        :return: The url as https
        :rtype: str
        """
        parsed = urlparse(url)
        return parsed._replace(scheme="http").geturl()
