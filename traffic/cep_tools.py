#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Complex event processing for traffic control.

Receives events from controller to process them and create aggregated data.

Example:
    Execution mode:

        $ python cep.py

Schema:

|----- CEP controller -----|
|__________________________|
|                          |
|     <pattern_matching>   |
|--------------------------|
|      <moving_average>    |
|--------------------------|
|    <threshold_overpass>  |
|__________________________|

                                    PULL
                              -------------
        SUB                  |_____cep_____|
    ------------             | CEP         |
 --| controller | PUSH --> --| controller  |
    ------------              -------------|
        PUSH                    PUSH
         ||                      ||
         ||                      ||
        PULL <-------------------||
    ------------
 --|    data    |--
    ------------
     <Database>

"""

import time
import json
import numpy

# import redis

__author__ = "Nicolas Estrada"
__version__ = "1.0.0"
__email__ = "nicoestrada.i@gmail.com"
__status__ = "Development"

 
WINDOW_SIZE = 1000
MIN_THRESHOLD = 30
MAX_THESHOLD = 150

class Notification(object):
    """Class for notification levels"""

    IGNORE = dict(
        notify_str = 'OK',
        log = '[OK] Normal values, moving avg., |speed = {speed} km/h (moving avg = {avg_speed} km/h)|',
        notify_id = 0,
        event = dict(
            routing_key = 'ignore.avg',
            actions = []
            ),
        threshold = 10
        )
    WARNING = dict(
        notify_str = 'WARNING',
        log = '[WARNING] values over 10 percent, |{speed} km/h (moving avg = {avg_speed} km/h)|',
        notify_id = 1,
        event = dict(
            routing_key = 'warning.avg',
            actions = ['send_event']
            ),
        threshold = 20
        )
    CRITICAL = dict(
        notify_str = 'CRITICAL',
        log = '[CRITICAL] values over 20 percent, |{speed} km/h (moving avg = {avg_speed} km/h)|',
        notify_id = 2,
        event = dict(
            routing_key = 'critical.avg',
            actions = ['send_event', 'cep_agg']
            ),
        threshold = 50
        )
    EXCEPTION = dict(
        notify_str = 'EXCEPTION_AVG',
        log = '[EXCEPTION] values over 50 percent, |{speed} km/h (moving avg = {avg_speed} km/h)|',
        notify_id = 3,
        event = dict(
            routing_key = 'exception.avg',
            actions = ['send_event', 'cep_agg']
            ),
        threshold = 100
        )
    EXCEPTION_MIN = dict(
        notify_str = 'EXCEPTION_MIN',
        log = '[EXCEPTION] speed under minimum threshold, |{speed} km/h (moving avg = {avg_speed} km/h)|',
        notify_id = 4,
        event = dict(
            routing_key = 'exception.min',
            actions = ['send_event']
            ),
        threshold = MIN_THRESHOLD
        )
    EXCEPTION_MAX = dict(
        notify_str = 'EXCEPTION_MAX',
        log = '[EXCEPTION] speed over maximum threshold, |{speed} km/h (moving avg = {avg_speed} km/h)|',
        notify_id = 5,
        event = dict(
            routing_key = 'exception.max',
            actions = ['send_event']
            ),
        threshold = MAX_THESHOLD
        )

    RECOVERY = dict(
        notify_str = 'RECOVERY',
        log = '[RECOVERY] mv avg speed recovered, |{speed} km/h (moving avg = {avg_speed} km/h)|',
        notify_id = 6,
        event = dict(
            routing_key = 'recovery.avg',
            actions = ['send_event']
            ),
        threshold = 10
        )

    MODE = {
        1: 'percent_variation',
        2: 'speed_threshold'
    }

    DEFAULT_MODE = 1

    def __init__(self):
        super(Notification, self).__init__()
        self._avg = None
        self._last_notification = None
        self._last_speed = None
        self._last_notification_id = 0

    def get_level(self, value, mode=DEFAULT_MODE):

        if self.MODE[mode] == self.MODE[1]:
            # percent variation accoridng moving average

            if value < self.IGNORE['threshold']:
                if (self._last_notification_id
                    and self._last_notification_id
                        != self.RECOVERY['notify_id']):

                    return Notification.RECOVERY
                else:
                    return Notification.IGNORE

            elif value < self.WARNING['threshold']:
                return Notification.WARNING

            elif value < self.CRITICAL['threshold']:
                return Notification.CRITICAL

            else:
                return Notification.EXCEPTION

        elif self.MODE[mode] == self.MODE[2]:
            # speed threshold detection

            if value < MIN_THRESHOLD:
                return Notification.EXCEPTION_MIN

            elif value > MAX_THESHOLD:
                return Notification.EXCEPTION_MAX

            else:
                return Notification.IGNORE


def last_moving_average(values, window_size=WINDOW_SIZE):
    """Calculates moving average of a list of values

    Arguments:
        values,         list of values to calculate the mvg avg
        window_size,    windows size for mvg avg calculation

    Return:
        moving_average, last moving average value
    """

    if len(values) < window_size:
        # in case the list of values is shorter that window size
        return numpy.average(values)

    # calculating moving average an returning last value
    weights = numpy.repeat(1.0, window_size) / window_size
    return numpy.convolve(values, weights, 'valid')[-1]


def check(instance, speed, values):

    mv_avg = last_moving_average(values)

    percent_variation = 100 * numpy.absolute(mv_avg - speed) / mv_avg

    cep_event = instance.get_level(percent_variation)

    print '-----------------------------------------------------------------\n'
    print '| Notification Id: ', cep_event['notify_id'], ' [', cep_event['notify_str'], '], percent variation: ', percent_variation
    print cep_event['log'].format(speed=speed, avg_speed=mv_avg)
    print 'CEP event agg: ', cep_event['event']
    print '-----------------------------------------------------------------\n'

