#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Class for DTS file receiver tasks specific to filetype raw
"""

__version__ = "$Rev$"

import pyfits

import despymisc.miscutils as miscutils
import despyfits.fits_special_metadata as spmeta

class DTSraw(object):
    """
    Class for DTS file receiver tasks specific to filetype raw
    """

    @staticmethod
    def requested_config_vals():
        """ Return dictionary with REQ/OPT config values for this class """
        return {'raw_project':'REQ'}

    ######################################################################
    def __init__(self, dbh, config):
        """ Initialize object data """
        self.config = config
        self.dbh = dbh
        self.verbose = 0

    ######################################################################
    def get_archive_path(self, fullname):
        """ Return the relative path inside the archive for the given file """

        # get nite
        primary_hdr = pyfits.getheader(fullname, 0)
        prihdu = pyfits.PrimaryHDU(header=primary_hdr)
        hdulist = pyfits.HDUList([prihdu])
        nite = spmeta.func_nite(fullname, hdulist, 0)

        # create path
        filepath = '%s/raw/%s' % (self.config['raw_project'], nite)
        return filepath

if __name__ == '__main__':
    pass
