import io
import os
import re
import uuid
import xml.etree.cElementTree as ET

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen


from .constants import \
    ExtraFileNames, \
    file_descriptions, \
    DATAONE_URL

from d1_common.types import dataoneTypes
from d1_common import const as d1_const
from d1_common.resource_map import \
    ResourceMap

"""
Methods that are responsible for handling metadata generation and parsing
belong here. Many of these methods are helper functions for generating the
EML document.
"""


class DataONEMetadata(object):
    mimetypes = set()
    access_policy = None

    def get_dataone_mimetypes(self):
        """
        Returns a list of DataONE supported mimetypes. The endpoint returns
        XML, which is parsed with ElementTree.
        :return: A list of mimetypes
        :rtype: list
        """
        response = urlopen(DATAONE_URL+'/v2/formats')
        e = ET.ElementTree(ET.fromstring(response.read()))
        root = e.getroot()
        mime_types = set()

        for element in root.iter('mediaType'):
            mime_types.add(element.attrib['name'])

        return mime_types

    def get_dataone_mimetype(self, mimetype):
        """
        If a mimeType isn't found in DataONE's supported list,
        default to application/octet-stream.

        :param supported_types:
        :param mimetype:
        :return:
        """

        if not self.mimetypes:
            self.mimetypes = self.get_dataone_mimetypes()

        if mimetype not in self.mimetypes:
            return 'application/octet-stream'
        return mimetype

    def get_access_policy(self):
        """
        Returns or creates the access policy for the system metadata.
        :return: The access policy
        :rtype: d1_common.types.generated.dataoneTypes_v1.AccessPolicy
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

    def create_resource_map(self, pid, scimeta_pid, sciobj_pid_list):
        """
        Create a simple resource map with one science metadata document and any
        number of science data objects.
        This method differs from d1_common.resource_map.createSimpleResourceMap
        by allowing you to specify the coordinating node that the objects
        can be found on.
        :param scimeta_pid: PID of the metadata document
        :param sciobj_pid_list: PID of the upload object
        :type scimeta_pid: str
        :type sciobj_pid_list: list
        :return: The ORE object
        :rtype: d1_common.resource_map.ResourceMap
        """

        ore = ResourceMap(base_url=DATAONE_URL+'/cn')
        ore.initialize(pid)
        ore.addMetadataDocument(scimeta_pid)
        ore.addDataDocuments(sciobj_pid_list, scimeta_pid)

        return ore.serialize()

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
        data_format = ET.SubElement(physical_section, 'dataFormat')
        externally_defined = ET.SubElement(
            data_format, 'externallyDefinedFormat')
        ET.SubElement(externally_defined, 'formatName').text = object_format

    def create_intellectual_rights(self, dataset_element, tale_license):
        """
        :param dataset_element: The xml element that defines the `dataset`
        :param tale_license: The Tale's license
        :type dataset_element: xml.etree.ElementTree.Element
        :type tale_license: dict
        :return: None
        """
        intellectual_rights = ET.SubElement(
            dataset_element, 'intellectualRights')
        section = ET.SubElement(intellectual_rights, 'section')
        para = ET.SubElement(section, 'para')
        ET.SubElement(para, 'literalLayout').text = tale_license

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

    def set_user_name(self, root, first_name, last_name):
        """
        Creates a section in the EML that describes a user's name.
        :param root: The parent XML element
        :param first_name: The user's first name
        :param last_name: The user's last name
        :type root: xml.etree.ElementTree.Element
        :type first_name: str
        :type last_name: str
        :return: None
        """
        individual_name = ET.SubElement(root, 'individualName')
        ET.SubElement(individual_name, 'givenName').text = first_name
        ET.SubElement(individual_name, 'surName').text = last_name

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
        users_id = ET.SubElement(root, 'userId')
        users_id.text = user_id
        users_id.set('directory', self._get_directory(user_id))

    def create_eml_doc(self, eml_pid, manifest, user_id, manifest_size,
                       environment_size, license_text):
        """
        Creates an initial EML record for the package based on a manifest.
        Individual objects will be added after-the-fact.

        :param manifest: Tale manifest
        :type manifest: dict
        :return: etree object
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
        dataset = ET.SubElement(ns, 'dataset')
        ET.SubElement(dataset, 'title').text = manifest['schema:name']

        first_name = manifest['createdBy']['schema:givenName']
        last_name = manifest['createdBy']['schema:familyName']
        email = manifest['createdBy']['schema:email']

        """
        Create a `creator` section, using the information in the
         `model.user` object to provide values.
        """
        creator = ET.SubElement(dataset, 'creator')
        self.set_user_name(creator, first_name, last_name)
        self.set_user_contact(creator, user_id, email)

        # Create a `description` field, but only if the Tale has a description.
        description = manifest['schema:description']
        if description is not str():
            abstract = ET.SubElement(dataset, 'abstract')
            ET.SubElement(abstract, 'para').text = \
                self._strip_html_tags(str(description))

        # Add a section for the license file
        self.create_intellectual_rights(dataset, license_text)

        # Add a section for the contact
        contact = ET.SubElement(dataset, 'contact')
        self.set_user_name(contact, first_name, last_name)
        self.set_user_contact(contact, user_id, email)

        for item in manifest['aggregates']:
            if 'bundledAs' not in item:
                name = os.path.basename(item['uri'])
                size = item['size']
                mimeType = self.get_dataone_mimetype(item['mimeType'])
                self.add_object_record(dataset, name, '', size, mimeType)

        # Add the manifest itself
        name = ExtraFileNames.tale_config
        description = file_descriptions[ExtraFileNames.tale_config]
        self.add_object_record(dataset, name, description,
                               manifest_size, 'application/json')

        # Add the environment json
        name = ExtraFileNames.environment_file
        description = file_descriptions[ExtraFileNames.environment_file]
        self.add_object_record(dataset, name, description,
                               environment_size, 'application/json')

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

    def generate_system_metadata(self, pid, name, format_id, size, md5,
                                 rights_holder):
        """
        Generates a metadata document describing the file_object.

        :param pid: The pid that the object will have
        :param name: The name of the object being described
        :param format_id: The format of the object (e.g text/csv)
        :param size: The size of the file
        :param md5: The md5 of the file
        :param rights_holder: The owner of this object
        :type pid: str
        :type name: str
        :type format_id: str
        :type size: int
        :type md5: int
        :type rights_holder: str
        :return: The metadata describing file_object
        :rtype: d1_common.types.generated.dataoneTypes_v2_0.SystemMetadata
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

    def _get_directory(self, user_id):
        """
        Returns the directory that should be used in the EML

        :param user_id: The user ID
        :type user_id: str
        :return: The directory name
        :rtype: str
        """
        if bool(user_id.find('orcid.org')):
            return 'https://orcid.org'
        return 'https://cilogon.org'

    def _strip_html_tags(self, html_string):
        """
        Removes HTML tags from a string
        :param html_string: The string with HTML
        :type html_string: str
        :return: The string without HTML
        :rtype: str
        """
        return re.sub('<[^<]+?>', '', html_string)
