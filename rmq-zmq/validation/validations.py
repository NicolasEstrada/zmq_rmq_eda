""" This file contains the definitions of what a 'clean' value is.

For example, it checks whether datetime values are indeed integers,
and whether their range makes it seem plausible that that they are
expressed in milliseconds since Epoch.

"""

from __future__ import division

import re
import string
import time

# This is January 01, 2005 @ 00:00 UTC
MIN_VALID_DATE = 1104537600000
DATE_OFFSET = 1000000

# This is January 1st 1900 @ 00:00 UTC
MIN_BIRTHDAY = -2208988800000

# Minimum date for a user to be in Skout
MIN_AGE = 13
YEAR_MILLISECONDS = 31536000000

#
# Other constants
#

VALID_DEVICES = frozenset((
    "android", "ipad", "ipod",
    "ipod touch", "iphone", "web"))

VALID_FILTER_TYPES = frozenset((
    'local', 'favorites', 'friends'))

VALID_PUSH_TYPES = frozenset(xrange(0, 100))

INVALID_LOCATIONS = frozenset(())

# 0 and Coffeyville
INVALID_LATITUDES = frozenset((
    0, ))

# 0 and Coffeyville
INVALID_LONGITUDES = frozenset((
    0, ))

ANDROID_MIN_ID_LEN = 13
ANDROID_MAX_ID_LEN = 17
IOS_ID_LEN = 40
ALTERNATIVE_USER_AGENTS = ('Freya [set]', )

PASSPORT_SOURCES = frozenset((
    "profile", "meet", "buzz",
    "other", "popular", "search"))

DEVICE_FIELDS = frozenset((
    'model', 'version', 'brand'))

# Pre compiled REGEX
IOS_REGEX = re.compile(
    "^iphone (amazon |qq )?(skout|boyahoy|flurv)\+?( )+\d+(\.\d+){1,2}",
    re.IGNORECASE)

ANDROID_REGEX = re.compile(
    "^android (amazon |qq )?(skout|boyahoy|flurv)\+?( )+\d+(\.\d+){1,2}",
    re.IGNORECASE)

FREYA_REGEX = re.compile("^freya \d+(\.\d+){0,2}", re.IGNORECASE)


def is_string(value, **kw):
    return isinstance(value, basestring)


def is_integer(integer, **kw):
    """Check whether the value you pass in is a correct integer value.
    The integer must be greater than 0.

    """

    return isinstance(integer, (int, long))


def is_dictionary(value, **kw):

    return isinstance(value, dict)


def is_boolean(value, **kw):
    """Check whether the value is a boolean.

    """

    return isinstance(value, bool)


def is_in_list(value, **kw):
    """This function can be used to test for enumerations.
    Pass in the enumerations as an array of integers in **kw['options']

    """

    iterable = kw.get('options')

    if not iterable:
        return True

    elif value in iterable:
        return True

    else:
        return False


def valid_iterable(container, validation_function):
    for element in container:
        if not validation_function(element):
            return False

    return True


def valid_userid(userid):
    """Check whether the value you pass in is a correct userid value.
    The userid is not guaranteed to exist in the db!

    """

    return (is_integer(userid) and userid > 0)


def valid_userids(container):
    return valid_iterable(container, valid_userid)


def valid_millisec_timestamp(millisec_timestamp):
    """Check whether the value you pass in is a correct datetime value.
    Correct datetimes are after the year 2005 and not in the future.

    """

    if not is_integer(millisec_timestamp):
        return False

    elif ((MIN_VALID_DATE > millisec_timestamp) or
          (millisec_timestamp > (time.time() * 1000) + DATE_OFFSET)):
        return False

    else:
        return True


def valid_device_udid(udid, **kw):
    """Check whether the value you pass in is a correct udid value

    """

    if not is_string(udid):
        return False

    elif len(udid) != 40:
        return False

    else:
        for letter in udid.lower():
            if letter not in string.hexdigits:
                return False

        return True


def valid_locationstring(value, **kw):
    """The locationstring is city, state, country

    """

    if not value:
        return False

    elif value in INVALID_LOCATIONS:
        return False

    else:
        return True


def valid_latitude(latitude, **kw):

    if not isinstance(latitude, float):
        return False

    elif latitude in INVALID_LATITUDES:
        return False

    else:
        return True


def valid_longitude(longitude, **kw):

    if not isinstance(longitude, float):
        return False

    elif longitude in INVALID_LONGITUDES:
        return False

    else:
        return True


def valid_points(points, **kw):
    """Points can be positive and negative, so the only thing we can
    check is whether they are indeed integers

    """

    return is_integer(points)


def valid_birthday(bday, **kw):

    if not is_integer(bday):
        return False

    # It's very unlikely that a Skout user is born before January 1st, 1900
    elif bday < MIN_BIRTHDAY:
        return False

    # You have to be older than 13 to join Skout
    if (((time.time() * 1000) - bday) // YEAR_MILLISECONDS) <= MIN_AGE:
        return False

    else:
        return True


def valid_ui(ui, **kw):
    """Check whether the UI string is formatted correctly. Examples:
    
    Android SKOUT 3.1.0
    Android Amazon Skout 3.1.0
    Android QQ Skout 3.1.0
    iPhone SKOUT 3.0.1
    iPhone Amazon Skout 3.0.1
    iPhone BoyAhoy 3.0.0
    iPhone SKOUT+ 2.7.0
    Freya 3.0.0

    """

    # The check is done in this way, so we avoid to apply all the regexes
    # in case one already produce a match
    if IOS_REGEX.search(ui):
        return True
    elif ANDROID_REGEX.search(ui):
        return True
    elif FREYA_REGEX.search(ui):
        return True
    else:
        return False


def valid_device_type(device_type, **kw):
    """Check wheter the device type is valid.

    """

    device_type = device_type.lower()
    return is_string(device_type) and (device_type in VALID_DEVICES)


def valid_filter_type(filter_type, **kw):
    """Check the filter type

    """

    return filter_type in VALID_FILTER_TYPES


def valid_push_type(push_type, **kw):
    return push_type in VALID_PUSH_TYPES


def valid_user_agent(user_agent, **kw):

    if is_string(user_agent):
        if user_agent in ALTERNATIVE_USER_AGENTS:
            return True

        len_ua = len(user_agent)

        is_android_id = (ANDROID_MIN_ID_LEN <= len_ua <= ANDROID_MAX_ID_LEN)
        is_ios_id = (len_ua == IOS_ID_LEN)

        if (is_ios_id or is_android_id):
            return True

    return False


def valid_bid(bid, **kw):
    if is_integer(bid):
        if 0 <= bid <= 100000:
            return True
    return False


def check_device(device, **kw):
    try:
        ##########################
        # Proposed future change #
        ##########################

        # DEVICE_FIELDS should be a subset of the keys of
        # the element we're receiving. This ensures that
        # we should have all the keys we're expecting.
        # Also, it returns True/False inmediately
        #return DEVICE_FIELDS <= frozenset(device.keys())

        # Actual code: The difference should be an empty set
        return not DEVICE_FIELDS.difference(frozenset(device.keys()))
    except AttributeError:
        return False


def valid_passport_source(source, **kw):
    """ Check for passport possible sources:
    profile, meet, buzz, other, popular, search.
    """
    if source.lower() in PASSPORT_SOURCES:
        return True
    return False


def omit(return_value):
    def dummy(*args, **kwargs):
        return return_value

    return dummy


VALIDATION_ALIASES = {
    'datetime': valid_millisec_timestamp,
    'userid': valid_userid,
    'enum': is_in_list,
    'string': is_string,
    'latitude': valid_latitude,
    'longitude': valid_longitude,
    'location_string': valid_locationstring,
    'points': valid_points,
    'birthday': valid_birthday,
    'ui': valid_ui,
    'integer': is_integer,
    'boolean': is_boolean,
    "device_type": valid_device_type,
    "userids": valid_userids,
    "buzz_filtertype": valid_filter_type,
    "pushtype": valid_push_type,
    "dictionary": is_dictionary,
    "useragent": valid_user_agent,
    "omit": omit(True),
    "bid": valid_bid,
    "device": check_device,
    "passport_source": valid_passport_source
}
