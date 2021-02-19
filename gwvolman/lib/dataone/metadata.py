import io
import logging
import os
from rdflib import Namespace
from rdflib.term import URIRef
import re
from typing import List
import xml.etree.cElementTree as ET

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

from .constants import \
    ExtraFileNames, \
    file_descriptions

from d1_client.cnclient_2_0 import CoordinatingNodeClient_2_0
from d1_common.types import dataoneTypes
from d1_common.types.exceptions import DataONEException
from d1_common.types.generated.dataoneTypes_v2_0 import SystemMetadata
from d1_common.types.generated.dataoneTypes_v1 import AccessPolicy
from d1_common import const as d1_const
from d1_common.resource_map import \
    ResourceMap, DCTERMS


"""
Methods that are responsible for handling metadata generation and parsing
belong here. Many of these methods are helper functions for generating the
EML document.
"""


class DataONEMetadata(object):

    def __init__(self, coordinating_node: str):
        self.coordinating_node: str = coordinating_node
        self.mimetypes: set = self.get_dataone_mimetypes()
        self.resource_map: ResourceMap = None
        self.access_policy: AccessPolicy = None

    def get_dataone_mimetypes(self) -> set:
        """
        Uses a coordinating node client to retrieve a list of supported
        formats

        :return: A set of mimetypes that DataONE supports
        """
        try:
            cn_client: CoordinatingNodeClient_2_0 = CoordinatingNodeClient_2_0(self.coordinating_node)
            formats_response = cn_client.listFormats()
        except DataONEException as e:
            logging.error("Failed to connect to the DataONE coordinating node. {}".format(e))
            return set()

        if not formats_response:
            # If the response is empty, we should exit before trying to parse it
            return set()

        # Use fromString to avoid length restrictions, and wrap fromString in ElementTree
        # to get a full xml representation, rather than an ET.Element
        element_tree_response = ET.ElementTree(ET.fromstring(formats_response.toxml("utf-8")))
        root = element_tree_response.getroot()
        mime_types = set()
        for element in root.iter('objectFormat'):
            try:
                mime_types.add(element.find("mediaType").attrib['name'])
            except (KeyError, AttributeError):
                # In case mediaType isn't found, or if it doesn't have 'name'
                continue
        return mime_types

    def check_dataone_mimetype(self, mimetype: str) -> str:
        """
        If a mimeType isn't found in DataONE's supported list,
        default to application/octet-stream.

        :param mimetype: The mimetype in question
        :return: A mimetype that is supported by DataONE
        """
        if mimetype not in self.mimetypes:
            return 'application/octet-stream'
        return mimetype

    def set_related_identifiers(self, manifest: dict, eml_pid: str,
                                tale: dict, member_node: str, gc):
        """
        This method adds fields to the DataONE resource map if there are
        1. Any DataCite:RelatedIdentifiers
        2. Any DataCiteIsDerivedFrom relations
        3. Any publishings of a potential parent Tale to the same member node

        :param manifest: The Tale's manifest
        :param eml_pid: The pid of the EML document
        :param tale: The Tale being published
        :param member_node: The member node that the Tale is being published to
        :param gc: The Gider client
        """
        eml_element = None
        try:
            eml_element = self.resource_map.getObjectByPid(eml_pid)
        except IndexError:
            logging.warning("Failed to find the pid {} in the resource map.".format(eml_pid))
            return

        if eml_element:
            added_record = False
            datacite_namespace = Namespace("http://purl.org/spar/datacite/")
            try:
                for relation in manifest["DataCite:relatedIdentifiers"]:
                    related_object = relation["DataCite:relatedIdentifier"]
                    if related_object["DataCite:relationType"] == "DataCite:Cites":
                        self.resource_map.add((eml_element, DCTERMS.references, URIRef(related_object["@id"])))
                    elif related_object["DataCite:relationType"] == "DataCite:IsDerivedFrom":
                        self.resource_map.add((eml_element,
                                               datacite_namespace.IsDerivedFrom, URIRef(related_object["@id"])))
                if tale['copyOfTale']:
                    # If this Tale is a copy of another Tale, we need to check if its predecessor was published
                    # If it was, then add a DataCite relation to the resource map
                    try:
                        parent_tale = gc.get("tale/{}".format(tale['copyOfTale']))
                        old_publish = next(
                            (item for item in parent_tale['publishInfo'] if item['repository'] == member_node),
                            None
                        )
                        if old_publish:
                            self.resource_map.add((eml_element,
                                                   datacite_namespace.IsDerivedFrom, URIRef(old_publish['pid'])))
                            added_record = True
                    except (KeyError, TypeError):
                        # If there was an error, then silently pass
                        pass
            except KeyError:
                pass
            if added_record:
                # Then add DataCite to the resource map namespace
                self.resource_map.namespace_manager.bind('datacite', datacite_namespace)

    def get_access_policy(self) -> AccessPolicy:
        """
        Returns or creates the access policy for the system metadata.
        :return: The access policy
        """

        if not self.access_policy:
            self.access_policy = dataoneTypes.accessPolicy()

            public_access_rule = dataoneTypes.AccessRule()
            public_access_rule.subject.append(d1_const.SUBJECT_PUBLIC)
            permission = dataoneTypes.Permission(
                dataoneTypes.Permission('read'))
            public_access_rule.permission.append(permission)
            self.access_policy.append(public_access_rule)

            admin_access_rule = dataoneTypes.AccessRule()
            admin_access_rule.subject.append(
                "CN=knb-data-admins,DC=dataone,DC=org")
            admin_access_rule.permission.append(
                dataoneTypes.Permission('write'))
            admin_access_rule.permission.append(permission)
            self.access_policy.append(admin_access_rule)

        return self.access_policy

    def create_resource_map(self, pid: str, scimeta_pid: str, sciobj_pid_list: List):
        """
        Create a simple resource map with one science metadata document and any
        number of science data objects.
        This method differs from d1_common.resource_map.createSimpleResourceMap
        by allowing you to specify the coordinating node that the objects
        can be found on.
        :param pid: The resource map's pid
        :param scimeta_pid: PID of the metadata document
        :param sciobj_pid_list: List of pids of data objects being uploaded
        """
        ore: ResourceMap = ResourceMap(base_url=self.coordinating_node)
        ore.initialize(pid)
        ore.addMetadataDocument(scimeta_pid)
        ore.addDataDocuments(sciobj_pid_list, scimeta_pid)
        self.resource_map = ore

    def create_entity(self, root, name, description):
        """
        Create an otherEntity section
        :param root: The parent element
        :param name: The name of the object
        :param description: The description of the object
        :type root: xml.etree.ElementTree.Element
        :type name: str
        :type description: str
        :return: An entity section
        :rtype: xml.etree.ElementTree.Element
        """
        entity = ET.SubElement(root, 'otherEntity')
        ET.SubElement(entity, 'entityName').text = name
        if description:
            ET.SubElement(entity, 'entityDescription').text = description
        return entity

    def create_physical(self, other_entity_section, name, size):
        """
        Creates a `physical` section.
        :param other_entity_section: The super-section
        :param name: The name of the object
        :param size: The size in bytes of the object
        :type other_entity_section: xml.etree.ElementTree.Element
        :type name: str
        :type size: str
        :return: The physical section
        :rtype: xml.etree.ElementTree.Element
        """
        physical = ET.SubElement(other_entity_section, 'physical')
        ET.SubElement(physical, 'objectName').text = name
        size_element = ET.SubElement(physical, 'size')
        size_element.text = str(size)
        size_element.set('unit', 'bytes')
        return physical

    def create_format(self, object_format, physical_section):
        """
        Creates a `dataFormat` field in the EML to describe the format
         of the object
        :param object_format: The format of the object
        :param physical_section: The element defining a `physical` EML section
        :type object_format: str
        :type physical_section: xml.etree.ElementTree.Element
        :return: None
        """
        data_format_elem = ET.SubElement(physical_section, 'dataFormat')
        externally_defined = ET.SubElement(
            data_format_elem, 'externallyDefinedFormat')
        ET.SubElement(externally_defined, 'formatName').text = object_format

    def create_intellectual_rights(self, dataset_element, tale_license):
        """
        :param dataset_element: The xml element that defines the `dataset`
        :param tale_license: The Tale's license
        :type dataset_element: xml.etree.ElementTree.Element
        :type tale_license: dict
        :return: None
        """
        intellectual_rights_elem = ET.SubElement(
            dataset_element, 'intellectualRights')
        section_elem = ET.SubElement(intellectual_rights_elem, 'section')
        para_elem = ET.SubElement(section_elem, 'para')
        ET.SubElement(para_elem, 'literalLayout').text = tale_license

    def add_object_record(self, root, name, description, size, object_format):
        """
        Add a section to the EML that describes an object.
        :param root: The root entity
        :param name: The name of the object
        :param description: The object's description
        :param size: The size of the object
        :param object_format: The format type
        :type root: xml.etree.ElementTree.Element
        :type name: str
        :type description: str
        :type size: str
        :type object_format: str
        :return: None
        """
        entity_section = self.create_entity(
            root, name, self._strip_html_tags(description))
        physical_section = self.create_physical(
            entity_section, name, size)
        self.create_format(object_format, physical_section)
        ET.SubElement(entity_section, 'entityType').text = 'dataTable'

    def set_user_name(self, root, first_name, last_name, user_id=None):
        """
        Creates a section in the EML that describes a user's name.
        :param root: The parent XML element
        :param first_name: The user's first name
        :param last_name: The user's last name
        :param user_id: The user's ORCID
        :type root: xml.etree.ElementTree.Element
        :type first_name: str
        :type last_name: str
        :return: None
        """
        individual_name_elem = ET.SubElement(root, 'individualName')
        ET.SubElement(individual_name_elem, 'givenName').text = first_name
        ET.SubElement(individual_name_elem, 'surName').text = last_name
        if user_id is not None:
            userid_elem = ET.SubElement(root, 'userId')
            userid_elem.text = user_id
            userid_elem.set('directory', self._get_directory(user_id))

    def set_user_contact(self, root, user_id, email):
        """
        Creates a section that describes the contact and owner
        :param root: The parent XML element
        :param user_id: The user's ID
        :param email: The user's email
        :type root: xml.etree.ElementTree.Element
        :type user_id: str
        :type email: str
        :return: None
        """
        ET.SubElement(root, 'electronicMailAddress').text = email
        userid_elem = ET.SubElement(root, 'userId')
        userid_elem.text = user_id
        userid_elem.set('directory', self._get_directory(user_id))

    def create_eml_doc(self, eml_pid, manifest, user_id, manifest_size,
                       environment_size, run_local_size, fetch_size,
                       license_text):
        """
                Creates an initial EML record for the package based on a manifest.
        Individual objects will be added after-the-fact.
        :param eml_pid: The pid of the EML document
        :param manifest: The manifest document
        :param user_id: The ORCID of the publisher
        :param manifest_size: The size of the manifest
        :param environment_size: The size of the environment
        :param run_local_size: The size of the run-local script
        :param fetch_size: The size of the fetch file
        :param license_text: The text of the license file
        :return: ETree
        """

        # Create the namespace
        ns = ET.Element('eml:eml')
        ns.set('xmlns:eml', 'eml://ecoinformatics.org/eml-2.1.1')
        ns.set('xsi:schemaLocation',
               'eml://ecoinformatics.org/eml-2.1.1 eml.xsd')
        ns.set('xmlns:stmml', 'http://www.xml-cml.org/schema/stmml-1.1')
        ns.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
        ns.set('scope', 'system')
        ns.set('system', 'knb')
        ns.set('packageId', eml_pid)

        """
        Create a `dataset` field, and assign the title to
        the name of the Tale. The DataONE Quality Engine
        prefers to have titles with at least 7 words.
        """
        dataset_elem = ET.SubElement(ns, 'dataset')
        ET.SubElement(dataset_elem, 'title').text = manifest['schema:name']

        """
        Create a `creator` section for each Tale author.
        """

        for author in manifest['schema:author']:
            creator_elem = ET.SubElement(dataset_elem, 'creator')
            first_name = author['schema:givenName']
            last_name = author['schema:familyName']
            user_id = author['@id']
            self.set_user_name(creator_elem, first_name, last_name, user_id)

        # If the Tale doesn't have an author, use the Tale creator
        if not len(manifest['schema:author']):
            creator_elem = ET.SubElement(dataset_elem, 'creator')
            first_name = manifest['createdBy']['schema:givenName']
            last_name = manifest['createdBy']['schema:familyName']
            contact_email = manifest['createdBy']['schema:email']
            self.set_user_name(creator_elem, first_name, last_name, contact_email)

        # Create a `description` field, but only if the Tale has a description.
        description = manifest['schema:description']
        if description is not str():
            abstract_elem = ET.SubElement(dataset_elem, 'abstract')
            ET.SubElement(abstract_elem, 'para').text = \
                self._strip_html_tags(str(description))

        # Add a section for the license file
        self.create_intellectual_rights(dataset_elem, license_text)

        """
        Add a dataset contact. This is set to the person publishing
        the Tale.
        """
        first_name = manifest['createdBy']['schema:givenName']
        last_name = manifest['createdBy']['schema:familyName']
        contact_email = manifest['createdBy']['schema:email']
        contact_elem = ET.SubElement(dataset_elem, 'contact')
        self.set_user_name(contact_elem, first_name, last_name)
        self.set_user_contact(contact_elem, user_id, contact_email)

        for item in manifest['aggregates']:
            if 'bundledAs' not in item:
                name = os.path.basename(item['uri'])
                size = item['size']
                mime_type = self.check_dataone_mimetype(item['mimeType'])
                self.add_object_record(dataset_elem, name, '', size, mime_type)

        # Add the manifest itself
        name = ExtraFileNames.manifest_file
        description = file_descriptions[ExtraFileNames.manifest_file]
        self.add_object_record(dataset_elem, name, description,
                               manifest_size, 'application/json')

        # Add the environment json
        name = ExtraFileNames.environment_file
        description = file_descriptions[ExtraFileNames.environment_file]
        self.add_object_record(dataset_elem, name, description,
                               environment_size, 'application/json')

        # Add the run-local.sh file
        description = file_descriptions[ExtraFileNames.run_local_file]
        self.add_object_record(dataset_elem, ExtraFileNames.run_local_file, description,
                               run_local_size, 'application/octet-stream')
        # Add the fetch.txt file
        description = file_descriptions[ExtraFileNames.fetch_file]
        self.add_object_record(dataset_elem, ExtraFileNames.fetch_file, description,
                               fetch_size, 'text/plain')

        # Add README.md file
        description = file_descriptions[ExtraFileNames.readme_file]
        self.add_object_record(dataset_elem, ExtraFileNames.readme_file, description,
                               fetch_size, 'text/plain')

        """
        Emulate the behavior of ElementTree.tostring in Python 3.6.0
        Write the contents to a stream and then return its content.
        The Python 3.4 version of ElementTree.tostring doesn't allow for
        `xml_declaration` to be set, so make a direct call to
        ElementTree.write, passing xml_declaration in.
        """
        stream = io.BytesIO()
        ET.ElementTree(ns).write(file_or_filename=stream,
                                 encoding='UTF-8',
                                 xml_declaration=True,
                                 method='xml',
                                 short_empty_elements=True)
        return stream.getvalue()

    def generate_system_metadata(self, pid: str, name: str, format_id: str,
                                 size: int, md5: str, rights_holder: str) -> SystemMetadata:
        """
        Generates a metadata document describing the file_object.

        :param pid: The pid that the object will have
        :param name: The name of the object being described
        :param format_id: The format of the object (e.g text/csv)
        :param size: The size of the file
        :param md5: The md5 of the file
        :param rights_holder: The owner of this object
        :return: The metadata describing file_object
        """

        sys_meta = dataoneTypes.systemMetadata()
        sys_meta.identifier = pid
        sys_meta.formatId = format_id
        sys_meta.size = size
        sys_meta.submitter = rights_holder
        sys_meta.rightsHolder = rights_holder
        sys_meta.checksum = dataoneTypes.checksum(str(md5))
        sys_meta.checksum.algorithm = 'MD5'
        sys_meta.accessPolicy = self.get_access_policy()
        sys_meta.fileName = name
        return sys_meta

    @staticmethod
    def _get_directory(user_id: str) -> str:
        """
        Returns the directory that should be used in the EML

        :param user_id: The user ID
        :return: The directory name
        """
        if bool(user_id.find('orcid.org')):
            return 'https://orcid.org'
        return 'https://cilogon.org'

    @staticmethod
    def _strip_html_tags(html_string: str) -> str:
        """
        Removes HTML tags from a string
        :param html_string: The string with HTML
        :return: The string without HTML
        """
        return re.sub('<[^<]+?>', '', html_string)
