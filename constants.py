"""
eod_datareader.constants
~~~~~~~~
This module contains numerous constants used by eod_datareader.


:author: leoliu49
"""
_ARCHIVE_INDEX = 'archive.data'

TIME_FORMAT = '%Y-%m-%d'
SRC_DEFAULT = 'iex'
ARCH_DIR = 'csv/'

class HEADERS:
    OPEN = 'open'
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    VOLUME = 'volume'
    HEADERS = [OPEN, HIGH, LOW, CLOSE, VOLUME]
