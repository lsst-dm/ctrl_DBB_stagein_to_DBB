# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Class for DTS file receiver tasks specific to filetype snmanifest
"""

__version__ = "$Rev$"

import dtsfilereceiver.dts_utils as dtsutils
import despymisc.misctime as misctime
import json

class DTSsnmanifest(object):
    """
    Class for DTS file receiver tasks specific to filetype snmanifest
    """
    @staticmethod
    def requested_config_vals():
        """ Return dictionary with REQ/OPT config values for this class """
        return {'filetype_metadata':'OPT', 'raw_project':'REQ'}

    ######################################################################
    def __init__(self, dbh, config):
        """ Initialize object data """
        self.config = config
        self.dbh = dbh
        self.verbose = 0
        self.debug = 0


    ######################################################################
    def check_valid(self, fullname): # should raise exception if not valid
        """ Check whether the given file is a valid SNe manifest file """
        pass


    ######################################################################
    def get_archive_path(self, fullname):
        """ Return the relative path inside the archive for the given file """

        nite = None
        with open(fullname, 'r') as jsonfh:
            line = jsonfh.readline()
            linedata = json.loads(line)
            datestr = linedata['exposures'][0]['date']
            nite = misctime.convert_utc_str_to_nite(datestr)

        filepath = '%s/%s/%s' % (self.config['raw_project'], 'snmanifest', nite)
        return filepath


if __name__ == '__main__':
    pass
