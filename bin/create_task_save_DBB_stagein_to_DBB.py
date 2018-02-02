#!/usr/bin/env python

""" Creates and begins task against which provenance for files inserted into
    the Data Backbone via the staging area will be tracked """

import argparse
import sys
import despydmdb.desdmdbi as desdmdbi


###########################################################################
def parse_cmdline(argv):
    """ Parse command line and return dictionary of values """

    parser = argparse.ArgumentParser(description='Create task for DBB file provenance')
    parser.add_argument('--label', action='store', required=True)
    parser.add_argument('--services', action='store',
                        help='full filename of services wallet file')
    parser.add_argument('--db_section', '--section', action='store',
                        help='section name from wallet to use to connect to db')

    args = vars(parser.parse_args(argv))   # convert to dict
    return args


###########################################################################
def main(argv):
    """ Program entry point """

    args = parse_cmdline(argv)
    dbh = desdmdbi.DesDmDbi(args['services'], args['db_section'])

    task_name = 'DBB_stagein_to_DBB'
    print("Creating task with name='%s' and label='%s'" % (task_name, args['label']))
    task_id = dbh.create_task(name=task_name, info_table=None, parent_task_id=None,
                              root_task_id=None, i_am_root=True, label=args['label'],
                              do_begin=True, do_commit=True)
    row = {'task_id': task_id, 'prov_msg': 'save files from staging area into DBB %s' % args['label']}
    dbh.basic_insert_row('FILE_REGISTRATION', row)
    dbh.commit()
    dbh.close()

    print("Update the config file:   task_id = %d" % task_id)


if __name__ == "__main__":
    main(sys.argv[1:])
