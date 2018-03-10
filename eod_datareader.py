"""
eod_datareader.eod_datareader
~~~~~~~~
This module implements the eod_datareader API.
EOD stock data may be retrieved using the get() function. Additionally, the
module will automatically output results as CSV into an archive for
data persistence and reusability.


:author: leoliu49
"""
import pandas

from datetime import datetime, timedelta

from . import _archive as archive
from . import _web_datareader as web_datareader

from . import constants


HEADERS = constants.HEADERS


def _merge(df_filleds, df_gaps):
    return df_filleds, df_gaps

def _get(ts, dt_start, dt_end, source):
    r'''
    Retrieved requested EOD finance data, either from current archive or
    the pandas_datareader API
    '''
    gaps = archive._compute_gaps(ts, dt_start, dt_end)
    filleds = archive._compute_filleds(gaps, dt_start, dt_end)

    df_filleds = archive._pull_local(ts, filleds)
    df_gaps = []
    for dt_s, dt_e in gaps:
        df = web_datareader.query(ts, dt_s, dt_e, source)
        df_gaps.append(df)
        archive._save_local(df, ts, dt_s, dt_e)
    archive._sync_local(dt_start.strftime(constants.TIME_FORMAT),
                        dt_end.strftime(constants.TIME_FORMAT))
    return _merge(df_filleds, df_gaps)

def set_archive_directory(dir_path):
    r''' Sets output archive directory '''
    if dir_path[-1] != '/':
        dir_path += '/'
    constants.ARCH_DIR = dir_path

def get(ts, start,
        end=datetime.now().strftime(constants.TIME_FORMAT),
        source=constants.SRC_DEFAULT):
    r''' Outward facing API endpoint '''
    return _get(ts, datetime.strptime(start, constants.TIME_FORMAT),
                datetime.strptime(end, constants.TIME_FORMAT), source)
