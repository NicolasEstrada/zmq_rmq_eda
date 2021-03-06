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
from collections import deque

# import redis

__author__ = "Nicolas Estrada"
__version__ = "1.0.0"
__email__ = "nicoestrada.i@gmail.com"
__status__ = "Development"


WINDOW_SIZE = 500
WARMUP = WINDOW_SIZE * .85
WINDOW_SIZE_COMP = 10
MIN_THRESHOLD = 5
MAX_THESHOLD = 150


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


def moving_average(values, window_size=WINDOW_SIZE_COMP):
    """Calculates moving average of a list of values

    Arguments:
        values,         list of values to calculate the mvg avg
        window_size,    windows size for mvg avg calculation

    Return:
        moving_average, moving average list
    """

    if len(values) < window_size:
        # in case the list of values is shorter that window size
        # return numpy.average(values)
        window_size = 1


    # calculating moving average an returning last value
    weights = numpy.repeat(1.0, window_size) / window_size
    return numpy.convolve(values, weights, 'valid')


class Notification(object):
    """Class for notification levels"""

    IGNORE = dict(
        notify_str = 'OK',
        log = '[OK] Normal values, moving avg., |speed = {speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
        notify_id = 0,
        event = dict(
            routing_key = 'ignore.avg',
            actions = []
            ),
        threshold = 10
        )
    WARNING = dict(
        notify_str = 'WARNING',
        log = '[WARNING] values over 10 percent, |{speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
        notify_id = 1,
        event = dict(
            routing_key = 'warning.avg',
            actions = ['send_event']
            ),
        threshold = 20
        )
    CRITICAL = dict(
        notify_str = 'CRITICAL',
        log = '[CRITICAL] values over 20 percent, |{speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
        notify_id = 2,
        event = dict(
            routing_key = 'critical.avg',
            actions = ['send_event', 'cep_agg']
            ),
        threshold = 50
        )
    EXCEPTION = dict(
        notify_str = 'EXCEPTION_AVG',
        log = '[EXCEPTION] values over 50 percent, |{speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
        notify_id = 3,
        event = dict(
            routing_key = 'exception.avg',
            actions = ['send_event', 'cep_agg']
            ),
        threshold = 100
        )
    EXCEPTION_MIN = dict(
        notify_str = 'EXCEPTION_MIN',
        log = '[EXCEPTION] speed under minimum threshold, |{speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
        notify_id = 4,
        event = dict(
            routing_key = 'exception.min',
            actions = ['send_event']
            ),
        threshold = MIN_THRESHOLD
        )
    EXCEPTION_MAX = dict(
        notify_str = 'EXCEPTION_MAX',
        log = '[EXCEPTION] speed over maximum threshold, |{speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
        notify_id = 5,
        event = dict(
            routing_key = 'exception.max',
            actions = ['send_event']
            ),
        threshold = MAX_THESHOLD
        )

    RECOVERY = dict(
        notify_str = 'RECOVERY',
        log = '[RECOVERY] mv avg speed recovered, |{speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
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
        self._lasts_notifications_ids = deque([], WINDOW_SIZE)
        self._offset = 0
        self._mv_avg = 0

    def get_level(self, speed, variation):

        # if self.MODE[mode] == self.MODE[1]:
        #     # percent variation accoridng moving average

        if speed < MIN_THRESHOLD:
            return Notification.EXCEPTION_MIN

        elif speed > MAX_THESHOLD:
            return Notification.EXCEPTION_MAX

        elif variation < self.IGNORE['threshold']:
            if (not self._last_notification_id
                and self.RECOVERY['notify_id'] not in self._lasts_notifications_ids
                and self._lasts_notifications_ids.count(
                        self.IGNORE['notify_id']) >= WARMUP):

                return Notification.RECOVERY
            else:
                return Notification.IGNORE

        elif variation < self.WARNING['threshold']:
            return Notification.WARNING

        elif variation < self.CRITICAL['threshold']:
            return Notification.CRITICAL

        elif variation < self.EXCEPTION['threshold']:
            return Notification.EXCEPTION

        # elif self.MODE[mode] == self.MODE[2]:
        #     # speed threshold detection

        else:
            return Notification.IGNORE

    def check(self, speed, values):

        if not self._offset % WINDOW_SIZE:
            # use simple average when amount of values is less that winow size
            self._mv_avg = last_moving_average(values)
            self._avg = numpy.average(values)

        percent_variation = 100 * (self._mv_avg - speed) / self._mv_avg

        cep_event = self.get_level(speed, numpy.absolute(percent_variation))

        # setting snapshot variables
        self._last_notification_id = cep_event['notify_id']
        self._lasts_notifications_ids.append(cep_event['notify_id'])
        self._offset += 1
        self._last_speed = speed

        # if cep_event['notify_id'] in (6,):
        # # if cep_event['notify_id'] in (3,4,5,6):
        #     print '-----------------------------------------------------------------\n'
        #     print '| Notification Id: {0} [{1}], variation: {2:.2f}%, avg {3:.2f} km/h\n'.format(
        #         cep_event['notify_id'],
        #         cep_event['notify_str'],
        #         percent_variation,
        #         self._avg
        #         )
        #     print cep_event['log'].format(speed=speed, avg_speed=self._mv_avg)
        #     print 'CEP event agg: ', cep_event['event']
        #     print '-----------------------------------------------------------------\n'

        return cep_event

