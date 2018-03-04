"""
eod_datareader._archive
~~~~~~~~
This module implements metheods for csv archive management.
The CSV archive structure is as such:
ARCH_DIR/
        _ARCHIVE_INDEX                      # index file of archive
        AAPL/                               # data, organized by ticker symbol
            1998-07-07_to_2000-01-01.csv
            ...


:author: leoliu49
"""
import csv
import errno
import os

from ConfigParser import SafeConfigParser
from datetime import datetime, timedelta
from glob import glob

from . import constants

LOCAL = {}
UPDATE = {}

def _source_local():
    r''' Sources archive index file into LOCAL '''
    config = SafeConfigParser()
    config.read(constants.ARCH_DIR + constants._ARCHIVE_INDEX)
    for ts in config.sections():
        LOCAL[ts] = []

        start = None
        for key, value in config.items(ts):
            if key.startswith('s'):
                start = value
            elif key.startswith('e'):
                LOCAL[ts].append((start, value))

def _sync_local(dt_start_total, dt_end_total):
    r'''
    Saves changes made in UPDATE into LOCAL, then outputs to index file
    '''
    config = SafeConfigParser()
    config.read(constants.ARCH_DIR + constants._ARCHIVE_INDEX)

    for ts in UPDATE:
        if ts not in LOCAL:
            LOCAL[ts] = UPDATE[ts]
        else:
            pass
        # Reindex archive index with new LOCAL indices
        config.remove_section(ts)
        config.add_section(ts)
        index = 0
        for s, e in LOCAL[ts]:
            config.set(ts, 's{}'.format(index), s)
            config.set(ts, 'e{}'.format(index), e)
            index += 1

    with open(constants.ARCH_DIR + constants._ARCHIVE_INDEX, 'wb') as f:
        config.write(f)

    UPDATE.clear()

def _save_local(df, ts, dt_start, dt_end):
    r'''
    Outputs CSV file to archive, merging with connected files as necessary
    '''
    start = dt_start.strftime(constants.TIME_FORMAT)
    end = dt_end.strftime(constants.TIME_FORMAT)

    out_dir = constants.ARCH_DIR + ts + '/'

    # Try to create directory if it doesn't alread exist
    try:
        os.makedirs(out_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    csv_output = df.to_csv(header=False)
    file_name = ''

    prepend_date = (dt_start
            - timedelta(days=1)).strftime(constants.TIME_FORMAT)
    append_date = (dt_end
            + timedelta(days=1)).strftime(constants.TIME_FORMAT)

    # Append to previous store, if connected
    connected_file = glob('{}*{}.csv'.format(out_dir, prepend_date))
    if connected_file:
        with open(connected_file[0], 'a') as f:
            f.write(csv_output)
        file_name = '{}_to_{}.csv'.format(connected_file[0].split('_to_')[0],
                                          end)
        os.rename(connected_file[0], file_name)
    else:
        file_name = '{}{}_to_{}.csv'.format(out_dir, start, end)
        with open(file_name, 'wb') as f:
            f.write(csv_output)

    # Join subsequent store, if connected
    connected_file = glob('{}{}*.csv'.format(out_dir, append_date))
    if connected_file:
        reader = csv.reader(open(connected_file[0], 'rb'))
        writer = csv.writer(open(file_name, 'ab'))
        for row in reader:
            writer.writerow(row)
        new_file_name = '{}_to_{}.csv'.format(
                file_name.split('_to_')[0],
                connected_file[0].split('_to_')[1].split('.')[0])
        os.rename(file_name, new_file_name)
        os.remove(connected_file[0])

    if ts not in UPDATE:
        UPDATE[ts] = []
    UPDATE[ts].append(
        (dt_start.strftime(constants.TIME_FORMAT),
         dt_end.strftime(constants.TIME_FORMAT)))

def _compute_gaps(ts, dt_start_total, dt_end_total):
    r'''
    Compute gaps in current archive which need to be retrieved online
    '''
    # Make sure archive is indexed
    if not LOCAL:
        _source_local()

    # Return full interval if nothing in archive
    if ts not in LOCAL:
        return [(dt_start_total, dt_end_total)]

    gaps = []
    last_avail = dt_start_total - timedelta(days=1)
    # Compute gaps in current archive and save into array
    for start, end in LOCAL[ts]:
        start = datetime.strptime(start, constants.TIME_FORMAT)
        end = datetime.strptime(end, constants.TIME_FORMAT)
        if start > last_avail + timedelta(days=1):
            gaps.append(
                    (last_avail + timedelta(days=1),
                        start - timedelta(days=1)))
        if end > last_avail:
            last_avail = end
        if last_avail >= dt_end_total:
            break
    if last_avail < dt_end_total:
        gaps.append((last_avail + timedelta(days=1), dt_end_total))

    return gaps







