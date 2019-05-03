#!/usr/bin/env python
# -*- coding: utf-8 -*-


class DataONELocations:
    """
    An enumeration that describes the different DataONE
    endpoints.
    """
    # Production coordinating node
    prod_mn = 'https://knb.ecoinformatics.org/knb/d1/mn'
    # Development member node
    dev_mn = 'https://dev.nceas.ucsb.edu/knb/d1/mn'


class ExtraFileNames:
    """
    When creating data packages we'll have to create additional files, such as
    the zipped recipe, the tale.yml file, the metadata document, and possibly
    more. Keep their names store here so that they can easily be referenced and
    changed in a single place.
    """
    # Name for the tale config file
    manifest_file = 'manifest.json'
    license_filename = 'LICENSE'
    environment_file = 'environment.json'


"""
A dictionary for the descriptions of the manually added package files.
"""
file_descriptions = {
    ExtraFileNames.environment_file:
        'Contains configuration information about the underlying compute '
        'environment required to run the Tale.',
    ExtraFileNames.manifest_file:
        'A configuration file, holding information that is needed to '
        'reproduce the compute environment.',
    ExtraFileNames.license_filename:
        'The package\'s licensing information.'
}
