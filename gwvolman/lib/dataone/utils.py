# -*- coding: utf-8 -*-
# Copyright (c) 2016, Data Exploration Lab
# Distributed under the terms of the Modified BSD License.

"""A set of helper routines for DataONE publishing related tasks."""

import os
import re
import uuid
import logging
import jwt
import hashlib
import math
import xml.etree.cElementTree as eTree
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
import docker

from .constants import DataONELocations

DATAONE_URL=os.environ.get('DATAONE_URL', 'https://cn-stage-2.test.dataone.org/cn')

def get_file_item(item_id, gc):
    """
    Gets the file out of an item.

    :param item_id: The item that has the file inside
    :param gc: The girder client
    :type: item_id: str
    :return: The file object or None
    :rtype: girder.models.file
    """
    file_generator = gc.listFile(item_id)
    try:
        return next(file_generator)
    except StopIteration as e:
        return None


def from_dataone(gc, item_id):
    """
    Checks if an item came from DataONE
    :param gc: The Girder client
    :param item_id: The item in question
    :return: True if it does, False otherwise
    """
    item = gc.getItem(item_id)
    folder = gc.getFolder(item['folderId'])
    if folder:
        try:
            return folder['meta']['provider'] == 'DataONE'
        except KeyError:
            return False
    return False


def from_http(gc, item_id):
    """
    Check if an item came from Dataverse
    :param gc: The Girder client
    :param item_id: The item in question
    :return: True if it does, False otherwise
    """
    item = gc.getItem(item_id)
    if item:
        try:
            return item['meta']['provider'] == 'HTTP'
        except KeyError:
            return False
    return False


def check_pid(pid):
    """
    Check that a pid is of type str. Pids are generated as uuid4, and this
    check is done to make sure the programmer has converted it to a str before
    attempting to use it with the DataONE client.

    :param pid: The pid that is being checked
    :type pid: str, int
    :return: Returns the pid as a str, or just the pid if it was already a str
    :rtype: str
    """

    if not isinstance(pid, str):
        return str(pid)
    else:
        return pid


def get_remote_url(item_id, gc):
    """
    Checks if a file has a link url and returns the url if it does. This is less
     restrictive than thecget_dataone_url in that we aren't restricting the link
      to a particular domain.

    :param item_id: The id of the item
    :param gc: The girder client
    :return: The url that points to the object
    :rtype: str or None
    """

    file = get_file_item(item_id, gc)
    if file is None:
        file_error = 'Failed to find the file with ID {}'.format(item_id)
        logging.warning(file_error)
        raise ValueError(file_error)
    url = file.get('linkUrl')
    if url is not None:
        return url


def get_dataone_package_url(member_node, pid):
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


def extract_user_id(jwt_token):
    """
    Takes a JWT and extracts the 'userId` field. This is used
    as the package's owner and contact.
    :param jwt_token: The decoded JWT
    :type jwt_token: str
    :return: The ORCID ID
    :rtype: str, None if failure
    """
    jwt_token = jwt.decode(jwt_token, verify=False)
    user_id = jwt_token.get('userId')
    return user_id


def extract_user_name(jwt_token):
    """
    Takes a JWT and extracts the 'userId` field. This is used
    as the package's owner and contact.
    :param jwt_token: The decoded JWT
    :type jwt_token: str
    :return: The ORCID ID
    :rtype: str, None if failure
    """
    jwt_token = jwt.decode(jwt_token, verify=False)
    user_id = jwt_token.get('fullName')
    return user_id


def is_orcid_id(user_id):
    """
    Checks whether a string is a link to an ORCID account
    :param user_id: The string that may contain the ORCID account
    :type user_id: str
    :return: True/False if it is or isn't
    :rtype: bool
    """
    return bool(user_id.find('orcid.org'))


def esc(value):
    """
    Escape a string so it can be used in a Solr query string
    :param value: The string that will be escaped
    :type value: str
    :return: The escaped string
    :rtype: str
    """
    return urlparse.quote_plus(value)


def strip_html_tags(html_string):
    """
    Removes HTML tags from a string
    :param html_string: The string with HTML
    :type html_string: str
    :return: The string without HTML
    :rtype: str
    """
    return re.sub('<[^<]+?>', '', html_string)


def get_directory(user_id):
    """
    Returns the directory that should be used in the EML

    :param user_id: The user ID
    :type user_id: str
    :return: The directory name
    :rtype: str
    """
    if is_orcid_id(user_id):
        return "https://orcid.org"
    return "https://cilogon.org"


def make_url_https(url):
    """
    Given an http url, return it as https

    :param url: The http url
    :type url: str
    :return: The url as https
    :rtype: str
    """
    parsed = urlparse(url)
    return parsed._replace(scheme="https").geturl()


def make_url_http(url):
    """
    Given an https url, make it http
     :param url: The http url
    :type url: str
    :return: The url as https
    :rtype: str
    """
    parsed = urlparse(url)
    return parsed._replace(scheme="http").geturl()


def get_resource_map_user(user_id):
    """
    HTTPS links will break the resource map. Use this function
    to get a properly constructed username from a user's ID.
    :param user_id: The user ORCID
    :type user_id: str
    :return: An http version of the user
    :rtype: str
    """
    if is_orcid_id(user_id):
        return make_url_http(user_id)
    return user_id


def get_file_md5(file_object, gc):
    """
    Computes the md5 of a file on the Girder filesystem.

    :param file_object: The file object that will be hashed
    :param gc: The girder client
    :type file_object: girder.models.file
    :return: Returns an updated md5 object. Returns None if it fails
    :rtype: md5
    """

    file = gc.downloadFileAsIterator(file_object['_id'])
    try:
        md5 = compute_md5(file)
    except Exception as e:
        logging.warning('Error: {}'.format(e))
        raise ValueError('Failed to download and md5 a remote file. {}'.format(e))
    return md5


def compute_md5(file):
    """
    Takes an file handle and computes the md5 of it. This uses duck typing
    to allow for any file handle that supports .read. Note that it is left to the
    caller to close the file handle and to handle any exceptions

    :param file: An open file handle that can be read
    :return: Returns an updated md5 object. Returns None if it fails
    :rtype: md5
    """
    md5 = hashlib.md5()
    while True:
        buf = file.read(8192)
        if not buf:
            break
        md5.update(buf)
    return md5


def get_item_identifier(item_id, gc):
    """
    Returns the identifier field in an item's meta field
    :param item_id: The item's ID
    :param gc: The Girder Client
    :type item_id: str
    :return: The item's identifier
    """
    item = gc.getItem(item_id)
    config = item.get('meta')
    if config:
        return config.get('identifier')


def filter_workspace(root_folder, gc, workspace_items=None):
    """
    Given a workspace folder, create a record about the items inside.
    :param root_folder: The folder whose contents are being stored
    :param gc: The girder client
    :param workspace_items: Items that were found in the workspace
    :return: The items in the Tale's workspace
    """
    if workspace_items is None:
        workspace_items = set()
    for obj in gc.listItem(root_folder):
        workspace_items.add(obj['_id'])
    for obj in gc.listFolder(root_folder):
        temp_object = filter_workspace(obj['_id'], gc)
        if len(temp_object):
            workspace_items.update(temp_object)
    return workspace_items


def filter_dataset(dataset_obj,
                   gc,
                   dataone_pids=None,
                   dataone_items=None,
                   http_items=None):
    """
    Given an item/folder in a Tale's dataSet, store the items that are inside
    :param dataset_obj: Either a folder or item
    :param gc: The Girder Client
    :param dataone_pids: List of DataONE pids from Tale items
    :param dataone_items: List of items that are referenced via DataONE
    :param http_items: Items that point to http resources
    :return: The dataone items, their pids, and any http items
    """
    if dataone_pids is None:
        dataone_pids = set()
    if dataone_items is None:
        dataone_items = set()
    if http_items is None:
        http_items = set()

    if dataset_obj['_modelType'] == 'item':
        url = get_remote_url(dataset_obj['_id'], gc)
        if url:
            if from_dataone(gc, dataset_obj['_id']):
                dataone_items.add(dataset_obj['_id'])
                dataone_pids.add(get_item_identifier(dataset_obj['_id'], gc))
            elif from_http(gc, dataset_obj['_id']):
                http_items.add(dataset_obj['_id'])

    elif dataset_obj['_modelType'] == 'folder':
        for sub_item in gc.listItem(dataset_obj['_id']):
            temp_http, temp_dataone_items, temp_pids = filter_dataset(sub_item,
                                                                      gc,
                                                                      dataone_pids,
                                                                      dataone_items,
                                                                      http_items)
            if len(temp_http):
                http_items.update(temp_http)
            if len(temp_dataone_items):
                dataone_items.update(temp_dataone_items)
            if len(temp_pids):
                dataone_pids.update(temp_pids)
    return http_items, dataone_items, dataone_pids


def filter_items(tale, gc):
    """
    Take the dataSet and workspace and sort the items by
    location (HTTP, dataone, local).
    """
    # Holds item_ids for DataONE objects
    dataone_items = set()
    # Hold the DataONE pids
    dataone_pids = set()
    # Holds item_ids for files not in DataONE
    http_items = set()

    # Handle the workspace
    local_items = filter_workspace(tale['workspaceId'], gc)
    # Handle the dataSet
    for obj in tale['dataSet']:
        if obj['_modelType'] == 'item':
            temp_http, temp_dataone_items, temp_pids =\
                filter_dataset(gc.getItem(obj['itemId']),
                               gc,
                               dataone_pids,
                               dataone_items,
                               http_items)
            if len(temp_http):
                http_items.update(temp_http)
            if len(temp_dataone_items):
                dataone_items.update(temp_dataone_items)
            if len(temp_pids):
                dataone_pids.update(temp_pids)
        elif obj['_modelType'] == 'folder':
            temp_http, temp_dataone_items, temp_pids =\
                filter_dataset(gc.getFolder(obj['itemId']),
                               gc,
                               dataone_pids,
                               dataone_items,
                               http_items)
            if len(temp_http):
                http_items.update(temp_http)
            if len(temp_dataone_items):
                dataone_items.update(temp_dataone_items)
            if len(temp_pids):
                dataone_pids.update(temp_pids)
    return {'dataone': list(dataone_items),
            'dataone_pids': list(dataone_pids),
            'remote': list(http_items),
            'local_items': list(local_items)}


def generate_dataone_guid():
    """
    DataONE requires that UUIDs are prepended with `urn:uuid:`. This method
    returns a DataONE compliant guid.
    :return: A DataONE compliant guid
    :rtype: str
    """
    return 'urn:uuid:'+str(uuid.uuid4())


def generate_size_progress_message(name, size_bytes):
    """
    Generates a message for the user about which file is being uploaded to a
    remote repository during publishing. For UX reasons, we convert Bytes
    to an appropriate derivative type.
    This was adapted from the following post at Stack Overflow
    https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python

    :param name: Name of the file
    :param size_bytes: Size of the file in Bytes
    :return: The message that the user will see
    :rtype: str
    """

    size_name = ("Bytes", "KB", "MB", "GB", "TB", "PB")
    if size_bytes > 0:
        i = int(math.floor(math.log(size_bytes, 1024)))
    else:
        i = 0
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    progress_message = "Uploading {}  Size: {} {}".format(name,
                                                          s,
                                                          size_name[i])
    return progress_message


def retrieve_supported_mimetypes():
    """
    Returns a list of DataONE supported mimetypes. The endpoint returns
    XML, which is parsed with ElementTree.
    :return: A list of mimetypes
    :rtype: list
    """
    response = urlopen(DATAONE_URL+'/v2/formats')
    e = eTree.ElementTree(eTree.fromstring(response.read()))
    root = e.getroot()
    mime_types = set()

    for element in root.iter('mediaType'):
        mime_types.add(element.attrib['name'])

    return mime_types


def get_dataone_mimetype(supported_types, mimetype):
    """

    :param supported_types:
    :param mimetype:
    :return:
    """
    if mimetype not in supported_types:
        return 'application/octet-stream'
    return mimetype
