#!/usr/bin/env python

""" For each file that was accepted by accept_dts_delivery,
    copy file to archive and register file in DB """

import commands
import os
import time
import sys
import re
import argparse
import shutil # move file
import traceback
from datetime import datetime

import despymisc.miscutils as miscutils
import filemgmt.fmutils as fmutils
import filemgmt.disk_utils_local as diskutils
import intgutils.replace_funcs as replfuncs


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


################################################################
def read_notify_file(notify_file):
    """ Return contents of notify file as dictionary """
    notifydict = {}
    with open(notify_file, "r") as notifyfh:
        for line in notifyfh:
            mline = re.match(r"^\s*(\S+)\s*=(.+)\s*$", line)
            notifydict[mline.group(1).strip().lower()] = mline.group(2).strip()
    return notifydict

################################################################
def stop_if_already_running():
    """ Exits program if program is already running """

    script_name = os.path.basename(__file__)
    statout = commands.getstatusoutput("ps aux | grep -e '%s' | grep -v grep | grep -v vim | awk '{print $2}'| awk '{print $2}' " % script_name)
    if statout[1]:
        print "Already running.  Aborting"
        print statout[1]
        sys.exit(0)


################################################################
def move_file_to_archive(config, delivery_fullname, archive_rel_path, dts_md5sum):
    """ Move file to its location in the archive """

    basename = os.path.basename(delivery_fullname)
    root = config['archive'][config['archive_name']]['root']
    path = "%s/%s" % (root, archive_rel_path)
    dst = "%s/%s" % (path, basename)
    if miscutils.fwdebug_check(3, "DTSFILEHANDLER_DEBUG"):
        miscutils.fwdebug_print("%s -> %s" % (delivery_fullname, dst))

    # try a couple times to copy file to archive directory
    max_cp_tries = 5
    cp_cnt = 1
    copied = False
    fileinfo = {}
    while cp_cnt <= max_cp_tries and not copied:
        miscutils.coremakedirs(path)

        shutil.copy2(delivery_fullname, dst) # similar to cp -p
        starttime = datetime.now()
        fileinfo = diskutils.get_single_file_disk_info(dst, True, root)
        endtime = datetime.now()

        miscutils.fwdebug_print("%s: md5sum after move %s (%0.2f secs)" % \
                                (delivery_fullname, fileinfo['md5sum'],
                                 (endtime - starttime).total_seconds()))

        if dts_md5sum is None:
            copied = True
        elif dts_md5sum != fileinfo['md5sum']:
            miscutils.fwdebug_print("Warning: md5 doesn't match after cp (%s, %s)" % \
                                    (delivery_fullname, dst))
            time.sleep(5)
            os.unlink(dst)   # remove bad file from archive
            cp_cnt += 1
        else:
            copied = True

    if not copied:
        raise IOError("Cannot cp file (%s->%s)" % (delivery_fullname, dst))

    os.unlink(delivery_fullname)

    return dst

################################################################
def handle_file(notify_file, delivery_fullname, config, filemgmt, task_id):
    """ Performs steps necessary for each file """

    ftype = None
    metadata = None
    disk_info = None
    new_fullname = None

    # read values from notify file
    notifydict = read_notify_file(notify_file)

    # use dts_md5sum from notify_file
    dts_md5sum = None
    if 'md5sum' in notifydict:
        dts_md5sum = notifydict['md5sum']

    miscutils.fwdebug_print("%s: dts md5sum = %s" % (delivery_fullname, dts_md5sum))

    #print config.keys()
    try:
        filename = miscutils.parse_fullname(delivery_fullname, miscutils.CU_PARSE_FILENAME)
        miscutils.fwdebug_print("filename = %s" % filename)

        if not os.path.exists(delivery_fullname):
            miscutils.fwdebug_print("Warning:  delivered file does not exist:")
            miscutils.fwdebug_print("\tnotification file: %s" % notify_file)
            miscutils.fwdebug_print("\tdelivered file: %s" % delivery_fullname)
            miscutils.fwdebug_print("\tRemoving notification file and continuing")
            os.unlink(notify_file)
            return

        ftype = determine_filetype(filename)
        if miscutils.fwdebug_check(3, "DTSFILEHANDLER_DEBUG"):
            miscutils.fwdebug_print("filetype = %s" % ftype)

        if filemgmt.is_file_in_archive([delivery_fullname], config['archive_name']):
            handle_bad_file(config, notify_file, delivery_fullname, None, filemgmt,
                            ftype, None, None, "Duplicate file")
        elif filemgmt.check_valid(ftype, delivery_fullname):
            starttime = datetime.now()
            results = filemgmt.register_file_data(ftype, [delivery_fullname], None, task_id, False, None, None)
            endtime = datetime.now()
            miscutils.fwdebug_print("%s: gathering and registering file data (%0.2f secs)" % \
                                    (delivery_fullname, (endtime - starttime).total_seconds()))
            disk_info = results[delivery_fullname]['diskinfo']
            metadata = results[delivery_fullname]['metadata']
            md5sum_before_move = disk_info['md5sum']

            # check that dts given md5sum matches md5sum in delivery directory
            if dts_md5sum is not None:
                miscutils.fwdebug_print("%s: md5sum before move %s" % (delivery_fullname,
                                                                       md5sum_before_move))
                if md5sum_before_move != dts_md5sum:
                    miscutils.fwdebug_print("%s: dts md5sum = %s" % (delivery_fullname, dts_md5sum))
                    miscutils.fwdebug_print("%s: py  md5sum = %s" % (delivery_fullname,
                                                                     md5sum_before_move))
                    raise IOError("md5sum in delivery dir not the same as DTS-provided md5sum")

            # get path 
            patkey = 'dirpat_' + ftype
            miscutils.fwdebug_print('patkey = %s' % patkey)
            dirpat = config['directory_pattern'][config[patkey]]['ops']
            miscutils.fwdebug_print('dirpat = %s' % dirpat)
            archive_rel_path = replfuncs.replace_vars_single(dirpat, metadata)
                
            if miscutils.fwdebug_check(3, "DTSFILEHANDLER_DEBUG"):
                miscutils.fwdebug_print('archive_rel_path = %s' % archive_rel_path)

            new_fullname = move_file_to_archive(config, delivery_fullname,
                                                archive_rel_path, dts_md5sum)
            miscutils.fwdebug_print("%s: fullname in archive %s" % (delivery_fullname,
                                                                    new_fullname))
            filemgmt.register_file_in_archive(new_fullname, config['archive_name'])

            # if success
            miscutils.fwdebug_print("%s: success.  committing to db" % (delivery_fullname))
            filemgmt.commit()
            os.unlink(notify_file)
        else:
            handle_bad_file(config, notify_file, delivery_fullname, new_fullname, filemgmt,
                            ftype, metadata, disk_info, "Invalid file")

    except Exception as err:
        (extype, exvalue, trback) = sys.exc_info()
        print "******************************"
        print "Error: %s" % delivery_fullname
        traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
        print "******************************"

        handle_bad_file(config, notify_file, delivery_fullname, new_fullname, filemgmt,
                        ftype, metadata, disk_info, "Exception: %s" % err)
    except SystemExit:   # Wrappers code calls exit if cannot find header value
        handle_bad_file(config, notify_file, delivery_fullname, new_fullname, filemgmt,
                        ftype, metadata, disk_info,
                        "SystemExit: Probably missing header value.  Check log for error msg.")

    filemgmt.commit()



################################################################
def handle_bad_file(config, notify_file, delivery_fullname, archive_fullname,
                    dbh, ftype, metadata, disk_info, msg):
    """ Perform steps required by any bad file """

    dbh.rollback()  # undo any db changes for this file

    miscutils.fwdebug_print("delivery_fullname = %s" % delivery_fullname)
    miscutils.fwdebug_print("archive_fullname = %s" % archive_fullname)
    miscutils.fwdebug_print("filetype = %s" % ftype)
    miscutils.fwdebug_print("msg = %s" % msg)
    if metadata is None:
        miscutils.fwdebug_print("metadata = None")
    else:
        miscutils.fwdebug_print("# metadata = %s" % len(metadata))
    if disk_info is None:
        miscutils.fwdebug_print("disk_info = None")
    else:
        miscutils.fwdebug_print("disk_info is not None")

    if miscutils.fwdebug_check(6, "DTSFILEHANDLER_DEBUG"):
        miscutils.fwdebug_print("metadata = %s" % metadata)
        miscutils.fwdebug_print("disk_info = %s" % disk_info)

    today = datetime.now()
    datepath = "%04d/%02d" % (today.year, today.month)

    # where is file now
    if archive_fullname is None:
        orig_fullname = delivery_fullname
    else:
        orig_fullname = archive_fullname

    # create a uniq name for living in the "bad file" area
    # contains relative path for storing in DB
    uniq_fullname = "%s/%s.%s" % (datepath, os.path.basename(orig_fullname),
                                  today.strftime("%Y%m%d%H%M%S%f")[:-3])

    # absolute path
    destbad = "%s/%s" % (config['bad_file_dir'], uniq_fullname)

    if os.path.exists(destbad):
        miscutils.fwdebug_print("WARNING: bad file already exists (%s)" % destbad)
        os.remove(destbad)

    # make directory in "bad file" area and move file there
    miscutils.coremakedirs(os.path.dirname(destbad))
    shutil.move(orig_fullname, destbad)

    # save information in db about bad file
    row = {}

    # save extra metadata if it exists
    if metadata is not None:
        badcols = dbh.get_column_names('DTS_BAD_FILE')

        for bcol in badcols:
            if bcol in metadata:
                row[bcol] = metadata[bcol]

    row['task_id'] = config['dts_task_id']
    notifyts = os.path.getmtime(notify_file)
    row['delivery_date'] = datetime.fromtimestamp(notifyts)
    row['orig_filename'] = os.path.basename(orig_fullname)
    row['uniq_fullname'] = uniq_fullname
    row['rejected_date'] = today
    row['rejected_msg'] = msg
    row['filesize'] = os.path.getsize(destbad)
    if ftype is not None:
        row['filetype'] = ftype


    dbh.basic_insert_row('DTS_BAD_FILE', row)
    dbh.commit()
    os.unlink(notify_file)



###########################################################################
def parse_cmdline(argv):
    """ Parse command line and return dictionary of values """

    parser = argparse.ArgumentParser(description='Handle files delivered by DTS')
    parser.add_argument('--config', action='store', required=True)
    #parser.add_argument('fullname', action='store')
    #parser.add_argument('--classmgmt', action='store')
    #parser.add_argument('--classutils', action='store')
    #parser.add_argument('--des_services', action='store')
    #parser.add_argument('--des_db_section', action='store')
    #parser.add_argument('--archive', action='store', help='single value')
    #parser.add_argument('--verbose', action='store', default=1)
    #parser.add_argument('--version', action='store_true', default=False)

    args = vars(parser.parse_args(argv))   # convert to dict
    return args


###########################################################################
def get_list_files(notify_dir, delivery_dir):
    """ Create list of files that need to be put into archive """

    filenames = next(os.walk(notify_dir))[2]

    delivery_filenames = []

    # sort by delivery order by using time of notification file
    for filen in sorted(filenames, key=lambda name: os.path.getmtime(os.path.join(notify_dir, name))):
        #print filen
        nfile = os.path.join(notify_dir, filen)
        dfile = os.path.join(delivery_dir, re.sub('.dts$', '', filen))
        delivery_filenames.append([nfile, dfile])

    return delivery_filenames

###########################################################################
def main(argv):
    """ Program entry point """

    args = parse_cmdline(argv)
    #print args

    config = read_config(args['config'])
    config['get_db_config'] = True

    filepairs = get_list_files(config['delivery_notice_dir'], config['delivery_dir'])
    #print filepairs

    if len(filepairs) > 0:
        filemgmt = None

        #print config['classmgmt']
        filemgmt_class = miscutils.dynamically_load_class(config['classmgmt'])
        #valDict = fmutils.get_config_vals({}, config, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(initvals=config)
        config['filetype_metadata'] = filemgmt.get_all_filetype_metadata()
        config['archive'] = filemgmt.get_archive_info()
        config['directory_pattern'] = filemgmt.query_results_dict('select * from OPS_DIRECTORY_PATTERN', 'name')

        task_id = config['dts_task_id']  # get task id for dts

        for fpair in filepairs:
            handle_file(fpair[0], fpair[1], config, filemgmt, task_id)
            miscutils.fwdebug_print("====================\n\n")
    else:
        miscutils.fwdebug_print("0 files to handle")


###########################################################################
if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)  # turn off buffering of stdout

    stop_if_already_running()

    #print "sleeping"
    #time.sleep(3000)

    main(sys.argv[1:])
