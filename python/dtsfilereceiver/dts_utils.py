#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

""" Miscellaneous functions for the DTS file accept and receiver codes """

import re

######################################################################
def read_config(cfgfile):
    """ Read the configuration file into a dictionary """
    config = {}
    with open(cfgfile, "r") as cfgfh:
        for line in cfgfh:
            line = line.strip()
            if len(line) > 0 and not line.startswith('#'):
                lmatch = re.match(r"([^=]+)\s*=\s*(.*)$", line)
                config[lmatch.group(1).strip()] = lmatch.group(2).strip()

    return config

######################################################################
def determine_filetype(filename):
    """ Returns the filetype of the given file or None if cannot determine filetype """
    filetype = None

    if filename.endswith('.fits'):
        filetype = 'raw'
    elif filename.startswith('manifest_SN') and filename.endswith('.json'):
        filetype = 'snmanifest'

    return filetype


######################################################################
def check_already_registered(filename, filemgmt):
    """ Throws exception if file is already registered """

    has_meta = filemgmt.has_metadata_ingested([filename])
    return len(has_meta) == 1

if __name__ == '__main__':
    pass
