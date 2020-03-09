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
    There are a couple of extra files that we generate when creating
    the bag that gets uploaded to DataONE. Keep track of their filenames
    here.
    """
    # Name for the tale config file
    manifest_file = 'manifest.json'
    license_filename = 'LICENSE'
    environment_file = 'environment.json'
    fetch_file = 'fetch.txt'
    run_local_file = 'run-local.sh'
    readme_file = 'README.md'


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
        'The package\'s licensing information.',
    ExtraFileNames.fetch_file:
        'Contains references to external data that needs to be downloaded '
        'before running the Tale',
    ExtraFileNames.run_local_file:
        'A bash script that downloads the neccessary external data and then runs '
        'the Tale.',
    ExtraFileNames.readme_file:
        'A readme file that describes how to interact with this Tale.'
}
