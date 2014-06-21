from __future__ import division

import datetime
import sys
import time

from dateutil import tz
from dateutil.relativedelta import *

import logging_manager

from misc import deprecated

# Constants
TIME_OFFSET = datetime.datetime.utcnow() - datetime.datetime.now()
EPOCH = datetime.datetime(1970, 1, 1)
MIDNIGHT = datetime.time(0, 0)

FROM_ZONE = tz.tzutc()
TO_ZONE = tz.tzlocal()

PYTHON_VERSION = sys.version_info[0:2]  # This extracts the major and minor version numbers
TARGET_VERSION = (2, 7)  # Python 2.7+


class InSeconds(object):
    MINUTE = 60
    HOUR = 3600
    DAY = 86400

SECONDS_PER_DAY = InSeconds.DAY

logger = logging_manager.start_logger('python_utils.date_utils', use_root_logger=False)


def datetime_to_millisec(obj):
    try:
        return int((obj.microsecond / 1000) + (time.mktime(obj.timetuple()) * 1000))
    except AttributeError:
        if isinstance(obj, datetime.date):
            obj = datetime.datetime.combine(obj, MIDNIGHT)

        time_diff = obj - EPOCH

        # For Python 2.7+
        if PYTHON_VERSION >= TARGET_VERSION:
            return int(time_diff.total_seconds() * 1000)
        else:
            microseconds = ((time_diff.seconds + time_diff.days * InSeconds.DAY) * 10 ** 6)
            return int((time_diff.microseconds + microseconds) / 10 ** 3)


def millisec_to_datetime(milliseconds):
    """Convert milliseconds to epoch to Python datetime objects.
    There's some loss in this function, but the loss is less than 1 second."""
    milliseconds = int(milliseconds)

    try:
        return (datetime.datetime.fromtimestamp(milliseconds / 1000)
                + datetime.timedelta(milliseconds=(milliseconds % 1000)))
    except ValueError:
        return EPOCH - TIME_OFFSET + datetime.timedelta(milliseconds=milliseconds)


def millisec_to_utcdatetime(milliseconds):
    return (datetime.datetime.utcfromtimestamp(milliseconds / 1000)
            + datetime.timedelta(milliseconds=(milliseconds % 1000)))


def local_dt(dt):
    """Convert utc datetime to local string datetime"""
    local = dt.replace(tzinfo=FROM_ZONE)
    return str(local.astimezone(TO_ZONE))

########################
# Deprecated functions #
########################


@deprecated("Use the datetime_to_millisec() function instead")
def datetime_to_ms_since_epoch(obj):
    try:
        return int((time.mktime(obj.timetuple()) * 1000) + (obj.microsecond / 1000))
    except Exception:
        td = obj - EPOCH

        # For Python 2.7+
        if PYTHON_VERSION >= TARGET_VERSION:
            return int(td.total_seconds() * 1000)
        else:
            return int((td.microseconds + ((td.seconds + td.days * InSeconds.DAY) * 10 ** 6)) / 10 ** 3)

    return int((time.mktime(obj.timetuple()) * 1000) + (obj.microsecond / 1000))
