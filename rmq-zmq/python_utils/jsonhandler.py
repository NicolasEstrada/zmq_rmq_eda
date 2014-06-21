"""This is the main JSON handler. It converts objects to their correct representation.

Currently it only changes datetime objects in to milliseconds since epoch, and there's a function
to revert back to datetime objects as well."""

__author__ = "Nicolas, Matias, Gonzalo"
__version__ = "0.2"

import datetime

import date_utils

from misc import deprecated


try:
    # Import, simplejson
    import simplejson as json
except ImportError:
    # Fallback to the standard python library
    import json


def better_dumps(obj):
    """Dump a Python object to a JSON string

    """

    return json.dumps(obj, default=_handler)


def _handler(obj):
    if isinstance(obj, datetime.datetime):
        return date_utils.datetime_to_millisec(obj)
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        msg = "Object of type {0} with value of {1} is not JSON serializable"
        raise ValueError(msg.format(type(obj), repr(obj)))


#########################
#  Deprecated functions #
#########################

@deprecated("Use the better_dumps() function instead.")
def dumps(obj):
    return better_dumps(obj)


@deprecated("Import the 'json' variable from this module, and use the loads() function from there.")
def loads(obj):
    """Load a string in to a python object

    """

    return json.loads(obj)


@deprecated
def _decode_list(data):

    rv = []

    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)

        rv.append(item)

    return rv


@deprecated
def _decode_dict(data):
    rv = {}

    for key, value in data.items():
        if isinstance(key, unicode):
            key = key.encode('utf-8')

        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)

        rv[key] = value

    return rv
