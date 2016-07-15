#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

""" Create a web page used to monitor the DESDM side of DTS """

import argparse
import despydb.desdbi as desdbi
from datetime import datetime, timedelta
import sys
import os
import glob
import re
import pytz
import getpass
import socket
import urllib2
import subprocess
from dateutil.parser import parse


verbose = False
NUM_NITES = 21
#NITESUM_BASE = "http://decam03.fnal.gov:8080/nightsum/"
NITESUM_BASE = "http://des-ops.fnal.gov:8080/nightsum/"

# desar2
#DELIVERY_LOG = "/home/databot/dts.log"
#ACCEPT_LOG_DIR = "/home/databot2/logs/accept_dts_delivery_logs"
#HANDLER_LOG_DIR = "/home/databot2/logs/dts_file_handler_logs"

# desar3
DELIVERY_LOG = "/local/dts/logs/dts.log"
DELIVERY_DIR = "/local/dts/delivery"
ACCEPT_LOG_DIR = "/local/dts_desdm/logs/accept_dts_delivery_logs"
HANDLER_LOG_DIR = "/local/dts_desdm/logs/dts_file_handler_logs"


def count_num_files_in_delivery_dir():
    """ Count the number of files in the delivery directory """
    return len(os.listdir(DELIVERY_DIR))


def convert_dts_log_timestamp(dts_ts):
    """ convert dts log timestamp to local timestamp """
    local_dt = None

    tsmatch = re.match(r'(\d\d)(\d\d) (\d\d:\d\d:\d\d)', dts_ts)
    if tsmatch:
        month = int(tsmatch.group(1))
        day = tsmatch.group(2)
        time = tsmatch.group(3)

        curr_time = datetime.now()
        if month > curr_time.month:
            year = curr_time.year -1
        else:
            year = curr_time.year
        new_ts_str = "%s-%02d-%sT%s.000000+00:00" % (year, month, day, time)
        utc_dt = parse(new_ts_str)

        # convert to local time
        local_tz = pytz.timezone('America/Chicago') # use your local timezone name here
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
        local_dt = local_tz.normalize(local_dt) # .normalize might be unnecessary
    else:
        local_dt = "UNK(%s)" % dts_ts

    return local_dt

def parse_timestamp(line):
    """ parse a timestamp from a line """
    timestamp = None
    lmatch = re.match(r'(\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d)', line)
    if lmatch:
        timestamp = lmatch.group(1)
    return timestamp

def get_last_timestamp_log_file(logfile):
    """ get the last timestamp from a log file """
    with open(logfile, "rb") as logfh:
        logfh.seek(-2, 2)            # Jump to the second last byte.
        while logfh.read(1) != "\n": # Until EOL is found...
            logfh.seek(-2, 1)        # ...jump back the read byte plus one more.
        last = logfh.readline()      # Read last line.

    return parse_timestamp(last)


def get_latest_log_file(basedir):
    """ get the latest log filename """
    logfile = basedir

    # get max year
    years = os.listdir(basedir)
    logfile += '/'+max(years)

    # get max month
    months = os.listdir(logfile)
    logfile += '/'+max(months)

    # get max filename
    filenames = os.listdir(logfile)
    logfile += '/'+max(filenames)

    return logfile

def get_timestamp_last_accept():
    """ get the timestamp of the last accept """
    logdir = ACCEPT_LOG_DIR
    latest_log = get_latest_log_file(logdir)
    last_accept = get_last_timestamp_log_file(latest_log)
    return last_accept

def get_timestamp_last_processed():
    """ get the timestamp of the last file processed """
    logdir = HANDLER_LOG_DIR

    latest_log = get_latest_log_file(logdir)

    latest_log_dir = os.path.dirname(latest_log)
    filelist = glob.glob('%s/*.log' % latest_log_dir)
    #print filelist
    files = ' '.join(filelist)
    #print files

    last_line = ""
    cmd = 'grep -h handle_file %s' % files    # no filename
    if verbose:
        print cmd
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = process.communicate()[0]
    last_line = None
    if len(out) > 0:
        outlines = out.strip('\n').split('\n')
        if len(outlines) >= 1:
            last_line = outlines[-1]
    if verbose:
        print "last_line:", last_line
    last_processed = ""
    if last_line is not None:
        last_processed = parse_timestamp(last_line)
    if verbose:
        print "last_processed:", last_processed

    return last_processed

def get_last_transfer_summary():
    """ get last transfer summary line from the dts log """
    dtslog = DELIVERY_LOG

    last_line = None
    cmd = 'grep XSUM %s' % dtslog
    if verbose:
        print cmd
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = process.communicate()[0]
    if len(out) > 0:
        outlines = out.split('\n')
        if len(outlines) >= 2:
            last_line = outlines[-2]
        elif len(outlines) == 1:
            last_line = outlines[-1]
    if verbose:
        print "last_line:", last_line

    msg_str = ""
    if last_line is not None:
        tstamp = last_line[:13]
        local_ts = convert_dts_log_timestamp(tstamp)

        dts_msg = last_line[14:].strip()
        msg_str = "%s - %s" % (local_ts.strftime("%Y/%m/%d %H:%M:%S"), dts_msg)

    return msg_str

def get_last_dts_err():
    """ get the last error message from the dts log """
    dtslog = DELIVERY_LOG

    cmd = 'grep ERR %s' % dtslog
    if verbose:
        print cmd
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = process.communicate()[0]

    last_line = None
    if len(out) > 0:
        outlines = out.split('\n')
        if len(outlines) >= 2:
            last_line = outlines[-2]
        elif len(outlines) == 1:
            last_line = outlines[-1]

    if verbose:
        print "last_line:", last_line

    msg_str = ""
    if last_line is not None:
        tstamp = last_line[:13]
        local_ts = convert_dts_log_timestamp(tstamp)

        dts_msg = last_line[14:].strip()
        msg_str = "%s - %s" % (local_ts.strftime("%Y/%m/%d %H:%M:%S"), dts_msg)

    return msg_str


def get_nightly_summary_index():
    """ get the nite summary urls from the nite summary index page """
    nite_summary_urls = {}
    try:
        index_file = urllib2.urlopen(NITESUM_BASE+"index.html").read()
        if verbose:
            print 'index_file=', index_file
        for line in index_file.split('\n'):
            lmatch = re.search(r"nightsum-(\d\d\d\d)-(\d\d)-(\d\d)/nightsum.html", line)
            if lmatch:
                nite = "%s%s%s" % (lmatch.group(1), lmatch.group(2), lmatch.group(3))
                url = NITESUM_BASE+"nightsum-%s-%s-%s/nightsum.html" % (lmatch.group(1), lmatch.group(2), lmatch.group(3))
                nite_summary_urls[nite] = url
    except Exception:
        pass
    return nite_summary_urls


def print_summary_html(timestamp, nite_info, extra_info, nitelist, htmlfile, cron=0):
    """ print summary in html """
    with open(htmlfile, "w") as htmlfh:
        htmlfh.write("<html>\n")
        htmlfh.write("<body>\n")
        htmlfh.write("<p>\n")
        htmlfh.write("Report for %s nites generated at %s by user %s on %s<br>\n" % \
                 (NUM_NITES, timestamp.strftime("%Y/%m/%d %H:%M:%S"),
                  getpass.getuser(), socket.gethostname()))
        tdelta = (datetime.now()-timestamp)
        htmlfh.write("(")
        if cron > 0:
            htmlfh.write("Running in cron every %s minutes.  " % cron)
        htmlfh.write("Took %0d.%04d secs to generate)<br>" % (tdelta.seconds, tdelta.microseconds))
        htmlfh.write("</p>\n")
        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Lasts</h2>\n")
        htmlfh.write("<ul>\n")
        htmlfh.write("<li>last time file delivered (accept script called): %s</li>\n" % \
                 extra_info['last_accept'])
        htmlfh.write("<li>last time file processed (cron job ran with work to do): %s</li>\n" % \
                 extra_info['last_processed'])
        htmlfh.write("<li>number of files in DTS delivery directory (%s): %s</li>\n" % \
                 (DELIVERY_DIR, extra_info['num_delivery']))
        htmlfh.write("<li>last file processed error: %s</li>\n" % extra_info['last_processed_err'])
        htmlfh.write("<li>last DTS transfer summary (seconds): %s</li>\n" % extra_info['last_xsum'])
        htmlfh.write("<li>last DTS ERR message: %s</li>\n" % extra_info['last_dts_err'])
        htmlfh.write("</ul>\n")
        htmlfh.write("</p>\n")


        # column descriptions
        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Nitely Counts</h2>\n")
        htmlfh.write("Column descriptions\n")
        htmlfh.write("<ul>\n")
        htmlfh.write("<li>Nite Sum: link appears if page exists for nite at %s</li>\n" % NITESUM_BASE)
        htmlfh.write("<li>SISPI: number of exposures in SISPI backup DB</li>\n")
        htmlfh.write("<li>DESDM: number of exposures in desoper DB (are officially in DESDM archive)</li>\n")
        htmlfh.write("<li>Missing: number of exposures in SISPI, but not in DESDM.<br>Only includes those that should be in DESDM (calibrations, des queue, SMASH project)</li>\n")
        htmlfh.write("<li>Extra: number of exposures in DESDM, but not in SISPI.<br>Same rules as for Missing</li>\n")
        htmlfh.write("</ul>\n")


        # table of information
        htmlfh.write("<table border=1 cellpad=3>\n")
        htmlfh.write("<tr><th>nite</th><th>nite sum</th><th>SISPI</th><th>DESDM</th><th>Missing</th><th>Extra</th></tr>\n")
        for nite in sorted(nitelist, reverse=True):
            htmlfh.write("<tr>")
            htmlfh.write("<td>%s</td>" % nite)
            if nite_info[nite]['nite_sum'] is None:
                htmlfh.write("<td>&nbsp;</td>")
            else:
                htmlfh.write('<td><a href="%s"</a>link</td>' % nite_info[nite]['nite_sum'])


            htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_sispi'])
            htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_desdm'])
            if nite_info[nite]['cnt_missing'] != 0:
                htmlfh.write('<td style="background-color:orange">%s</td>' % nite_info[nite]['cnt_missing'])
            else:
                htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_missing'])
            if nite_info[nite]['cnt_extra'] != 0:
                htmlfh.write('<td style="background-color:cyan">%s</td>' % nite_info[nite]['cnt_extra'])
            else:
                htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_extra'])
            htmlfh.write("</tr>\n")
        htmlfh.write("</table>\n")
        htmlfh.write("</p>\n")
        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Last 20 failures within date range:</h2>\n")
        htmlfh.write("Data comes from table dts_bad_file\n")
        htmlfh.write("Column descriptions\n")
        htmlfh.write("<ul>\n")
        htmlfh.write("<li>rejected_date: timestamp when delivered file was rejected.  Orange if within last 3 days.</li>\n")
        htmlfh.write("<li>orig_filename: filename as delivered.  uniq_filename in table has timestamp added to allow for multiple failures for a single file.</li>\n")
        htmlfh.write("<li>rejected_msg: error message describing why the file was rejected</li>\n")
        htmlfh.write("</ul>\n")


        htmlfh.write("<table border=1 cellpad=3>\n")
        htmlfh.write("<tr><th>rejected_date</th><th>orig_filename</th><th>rejected_msg</th></tr>\n")
        sortfail = sorted(extra_info['failures'], key=lambda item: item['rejected_date'],
                          reverse=True)
        for fcnt in range(0, min(20, len(sortfail))):
            htmlfh.write("<tr>\n")
            if (timestamp.date() - timedelta(days=3)) <= sortfail[fcnt]['rejected_date'].date():
                htmlfh.write('<td style="background-color:orange">%s</td>\n' % \
                         sortfail[fcnt]['rejected_date'])
            else:
                htmlfh.write("<td>%s</td>\n" % sortfail[fcnt]['rejected_date'])
            htmlfh.write("<td>%s</td>\n" % sortfail[fcnt]['orig_filename'])
            htmlfh.write("<td>%s</td>\n" % sortfail[fcnt]['rejected_msg'])
            htmlfh.write("</tr>\n")
        htmlfh.write("</table>\n")
        htmlfh.write("</p>\n")

        htmlfh.write("</body>\n")
        htmlfh.write("</html>")


def parse_argv(argv):
    """ Return command line arguments as dict """
    parser = argparse.ArgumentParser(description='Compare exposure DB entries for DESDM vs SISPI backup')
    parser.add_argument('--cron', action='store')
    parser.add_argument('--des_services', action='store')
    parser.add_argument('--des_db_section', action='store', required=True)
    parser.add_argument('--sispi_db_section', action='store', required=True)
    parser.add_argument('--nitestr', action='store')
    parser.add_argument('--verbose', action='store_true')
    args = vars(parser.parse_args(argv))   # convert dict
    return args


def convert_sispi_date_to_nite(sispidate):
    """ Convert sispi date to nite string """
    nitesplit = sispidate.replace(hour=15)
    expdate = sispidate
    if sispidate < nitesplit:
        expdate = sispidate - timedelta(days=1)
    expnite = expdate.strftime("%Y%m%d")

    return expnite


# SISPI db
def get_sispi_info(des_services, sispi_db_section, nitelist):
    """ Returns list of expnums in sispi db exposure table for nites """

    begnite = nitelist[-1]
    nitedate = datetime.strptime(begnite, "%Y%m%d")
    #begdate = nitedate.replace(hour=9, tzinfo=pytz.timezone('UTC'))
    begdate = nitedate.replace(hour=15)
    nitedate = datetime.strptime(nitelist[0], "%Y%m%d")
    enddate = nitedate.replace(hour=15) + timedelta(days=1)    # nite ends next date noonish


    dbh = desdbi.DesDbi(des_services, sispi_db_section)
    sql = "select id,date from exposure where %s <= date and date <= %s and delivered=TRUE and (flavor != 'object' or dtsqueue='des' or propid='2013B-0440');" % (dbh.get_named_bind_string("begdate"), dbh.get_named_bind_string("enddate"))
    if verbose:
        print sql
        print begdate, enddate
    curs = dbh.cursor()
    curs.execute(sql, {'begdate':begdate, 'enddate':enddate})
    desc = [d[0].lower() for d in curs.description]

    sispi_expnums = {}
    for nite in nitelist:
        sispi_expnums[nite] = set()

    for row in curs:
        rowd = dict(zip(desc, row))
        rowd['nite'] = convert_sispi_date_to_nite(rowd['date'])
        sispi_expnums[rowd['nite']].add(rowd['id'])
    dbh.close()

    return sispi_expnums



# DESDM db
def get_desdm_info(dbh, nitelist):
    """ Returns list of expnums in DESDM db exposure table for given nites """

    begnite = nitelist[-1]
    endnite = nitelist[0]
    sql = "select expnum, nite from exposure where %s <= nite and nite <= %s order by expnum" % \
          (dbh.get_named_bind_string('begnite'), dbh.get_named_bind_string('endnite'))
    if verbose:
        print sql
        print begnite, endnite
    curs = dbh.cursor()
    curs.execute(sql, {'begnite':begnite, 'endnite':endnite})
    desc = [d[0].lower() for d in curs.description]

    desdm_expnums = {}
    for nite in nitelist:
        desdm_expnums[nite] = set()

    for row in curs:
        rowd = dict(zip(desc, row))
        desdm_expnums[rowd['nite']].add(rowd['expnum'])

    return desdm_expnums


def get_desdm_fail_cnt_by_nite(failures, sispi_info):
    """ get the number of register failures by nite """
    # first make a dictionary by expnum
    # repeated failures don't count
    by_expnum = {}
    for faild in failures:
        if 'expnum' in faild:
            if faild['expnum'] not in by_expnum:
                by_expnum[faild['expnum']] = {}
            by_expnum[faild['expnum']][faild['orig_filename']] = faild

    by_nite = {}
    for nite in sispi_info:
        by_nite[nite] = 0
        for expnum in sispi_info[nite]:
            if expnum in by_expnum:
                by_nite[nite] += len(by_expnum[expnum])

    return by_nite


def get_desdm_fail_info(dbh, begdate):
    """ Returns list of failed files and messages during the time period """

    sql = "select orig_filename, rejected_msg, rejected_date from dts_bad_file where rejected_date >= %s" % dbh.get_named_bind_string('begdate')
    curs = dbh.cursor()
    curs.execute(sql, {'begdate': begdate})
    desc = [d[0].lower() for d in curs.description]

    des_fail = []
    for row in curs:
        rowd = dict(zip(desc, row))
        fmatch = re.match(r'D(\d+)_fits.fz', rowd['orig_filename'])
        if fmatch:
            rowd['expnum'] = fmatch.group(1).lstrip('0')
        des_fail.append(rowd)
    return des_fail


def gather_lasts():
    """ Gather information about last events """

    extra_info = {}
    extra_info['last_accept'] = get_timestamp_last_accept()
    extra_info['last_processed'] = get_timestamp_last_processed()
    extra_info['last_xsum'] = get_last_transfer_summary()
    extra_info['last_dts_err'] = get_last_dts_err()
    extra_info['num_delivery'] = count_num_files_in_delivery_dir()
    return extra_info


def gather_info(args, nitelist):
    """ Workflow for gathering all the information """

    all_nitely_info = {}

    extra_info = gather_lasts()

    begnite = nitelist[-1]
    begdate = datetime.strptime(begnite, "%Y%m%d")

    nite_summary_urls = get_nightly_summary_index()

    sispi_expnums = get_sispi_info(args['des_services'], args['sispi_db_section'], nitelist)

    # open DESDM connection here as needed multiple places
    dbh = desdbi.DesDbi(args['des_services'], args['des_db_section'])

    desdm_expnums = get_desdm_info(dbh, nitelist)
    extra_info['failures'] = get_desdm_fail_info(dbh, begdate)
    if len(extra_info['failures']) > 0:
        lasterr = extra_info['failures'][-1]
        extra_info['last_processed_err'] = "%s - %s - %s" % (lasterr['rejected_date'].strftime("%Y/%m/%d %H:%M:%S"), lasterr['orig_filename'], lasterr['rejected_msg'])
    else:
        extra_info['last_processed_err'] = ""
    dbh.close()

    fail_cnt_by_nite = get_desdm_fail_cnt_by_nite(extra_info['failures'], sispi_expnums)

    for nite in nitelist:
        all_nitely_info[nite] = {}

        if nite in nite_summary_urls:
            all_nitely_info[nite]['nite_sum'] = nite_summary_urls[nite]
        else:
            all_nitely_info[nite]['nite_sum'] = None

        if nite in sispi_expnums:
            all_nitely_info[nite]['cnt_sispi'] = len(sispi_expnums[nite])
        else:
            all_nitely_info[nite]['cnt_sispi'] = 0
        if nite in desdm_expnums:
            all_nitely_info[nite]['cnt_desdm'] = len(desdm_expnums[nite])
        else:
            all_nitely_info[nite]['cnt_desdm'] = 0

        extra_expnums = desdm_expnums[nite] - sispi_expnums[nite]
        all_nitely_info[nite]['cnt_extra'] = len(extra_expnums)

        missing_expnums = sispi_expnums[nite] - desdm_expnums[nite]
        all_nitely_info[nite]['cnt_missing'] = len(missing_expnums)

        if nite in fail_cnt_by_nite:
            all_nitely_info[nite]['cnt_fail'] = fail_cnt_by_nite[nite]
        else:
            all_nitely_info[nite]['cnt_fail'] = 0

    return all_nitely_info, extra_info


def create_full_html_filename():
    """ create the full filename for the output html file """
    htmlfilename = 'dtsmonitor.html'

    if 'DTSMONITOR_OUTBASE' in os.environ:
        output_base = os.environ['DTSMONITOR_OUTBASE']
        monitor_html = "%s/%s" % (output_base, htmlfilename)
    elif 'USER' in os.environ:
        user = os.environ['USER']
        if user == 'databot2':
            output_base = "/work/QA/technical/dts"
        else:
            output_base = "/home/%s/public_html" % (user)
        monitor_html = "%s/%s" % (output_base, htmlfilename)
    else:
        print "Warning:  writing to current directory"
        monitor_html = "%s" % (htmlfilename)

    return monitor_html


def main():
    """ Program entry point """
    starttime = datetime.now()
    args = parse_argv(sys.argv[1:])

    if args['verbose'] is not None:
        verbose = args['verbose']

    endnite = args['nitestr']
    if endnite is None:
        enddate = datetime.now()
        endnite = enddate.strftime("%Y%m%d")
    else:
        enddate = datetime.strptime(endnite, "%Y%m%d")

    datelist = [enddate - timedelta(days=x) for x in range(0, NUM_NITES)]
    if verbose:
        print datelist
    nitelist = [datetime.strftime(x, "%Y%m%d") for x in datelist]

    (nite_info, extra_info) = gather_info(args, nitelist)
    htmlfile = create_full_html_filename()
    if verbose:
        print "Writing html to %s" % (htmlfile)
    print_summary_html(starttime, nite_info, extra_info, nitelist, htmlfile, args['cron'])


if __name__ == "__main__":
    main()
