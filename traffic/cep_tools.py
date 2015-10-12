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
import numpy
import bisect
import ujson as json
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
LOG_STR = "{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}"


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


def get_consecutive(sorted_list):

    aux = sorted_list[:]
    result = []

    local = []
    last = int(aux.pop(0))
    local.append(last)

    for i in aux:
        if int(i) - last == 1:
            local.append(i)
            last = int(i)
        else:
            result.append(local)
            local = [i]
            last = int(i)
    if local:
        result.append(local)

        pass

    return result


class Notification(object):
    """Class for notification levels"""

    RECOVERY = dict(
        notify_str = 'RECOVERY',
        log = '[RECOVERY] mv avg speed recovered, |{speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
        notify_id = -1,
        event = dict(
            routing_key = 'recovery.avg',
            actions = ['send_event']
            ),
        threshold = 10
        )

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
            actions = ['send_event' ,'cep_agg']
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

    EXCEPTION_AGG = dict(
        notify_str = 'EXCEPTION_AGG',
        log = '[EXCEPTION_AGG] speeds levels out of boundaries for multiple correlated sensors, |{speed:.2f} km/h (moving avg = {avg_speed:.2f} km/h)|',
        notify_id = 10,
        event = dict(
            routing_key = 'exception.agg',
            actions = ['send_event']
            ),
        threshold = 10
        )

    MODE = {
        1: 'percent_variation',  # avg
        2: 'speed_threshold',  # min, max
        3: 'correlated_events'  # agg
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

        # evaluating threshold
        if speed < MIN_THRESHOLD:
            yield Notification.EXCEPTION_MIN

        elif speed > MAX_THESHOLD:
            yield Notification.EXCEPTION_MAX

        # evaluating moving average variation
        if variation < self.IGNORE['threshold']:
            # if recovered or still under traffic issues
            if (not self._last_notification_id
                and self.RECOVERY['notify_id'] not in self._lasts_notifications_ids
                and self._lasts_notifications_ids.count(
                        self.IGNORE['notify_id']) >= WARMUP):

                yield Notification.RECOVERY

            else:
                yield Notification.IGNORE

        elif variation < self.WARNING['threshold']:
            yield Notification.WARNING

        elif variation < self.CRITICAL['threshold']:
            yield Notification.CRITICAL

        elif variation < self.EXCEPTION['threshold']:
            yield Notification.EXCEPTION

        else:
            yield Notification.EXCEPTION

    def check(self, speed, values):

        if self._offset < WINDOW_SIZE:
            # use simple average when amount of values is less than window size
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


class Aggregator(object):
    def __init__(self, zmq_rcv, zmq_pub, verbose=False):
        self._events = {}
        self._receiver = zmq_rcv
        self._publisher = zmq_pub
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()

        self.count_in = 0
        self.count_out = 0
        self.bytes_in = 0
        self.bytes_out = 0
        self._offset = 1
        return self

    def __exit__(self, *args):
        self.end = time.time()

        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        self.ratio_in = float(self.count_in / self.secs)
        self.bratio_in = float(self.bytes_in / self.secs)
        self.ratio_out = float(self.count_out / self.secs)
        self.bratio_out = float(self.bytes_out / self.secs)
        if self.verbose:
            msg = LOG_STR.format(
                self.msecs, self.count_in, self.count_out,
                self.bytes_in, self.bytes_out, self.ratio_in,
                self.ratio_out, self.bratio_in, self.bratio_out)
            print msg

        found_patterns = self.check_all()

        for pattern in found_patterns:
            print("[aggregator - pattern] Pattern found: %s" % (pattern,))

    def register_event(self, sid, ts):
        if ts not in self._events:
            self._events[ts] = [str(sid)]
        elif str(sid) not in self._events[ts]:
            bisect.insort(self._events[ts], str(sid))
        else:
            pass

        return

    def receive(self):
        rkey, message = self._receiver.recv_multipart()
        self.count_in += 1
        return rkey, json.loads(message)

    def publish(self, rk, message):
        self.count_out += 1
        self._publisher.send_multipart(rk, message)

    def check(self, ts):

        pattern_matched = []

        # for ts in self._events:
        delete = False
        for events in get_consecutive(self._events[ts]):
            # TODO: events processing
            if len(events) > 1:
                pattern = ' -> '.join([str(e) for e in events])
                pattern_matched.append(pattern)

                delete = True

        if delete:
            del self._events[ts][:]

        return pattern_matched

    def check_all(self):

        pattern_matched = []

        for ts in self._events:
            delete = False
            for events in get_consecutive(self._events[ts]):
                # TODO: events processing
                if len(events) > 1:
                    pattern = ' -> '.join([str(e) for e in events])
                    pattern_matched.append(pattern)

                    delete = True

            if delete:
                del self._events[ts][:]
                delete = False

        return pattern_matched

    def get_all_aggregated(self, ts, wsize):

        for ts in self._events:
            delete = False
            for events in get_consecutive(self._events[ts]):
                if len(events) > 1:

                    message = dict(
                        sensor_id = -1,
                        event_id = self._offset,
                        sensor_ids = events,
                        event_ts = ts,
                        event_ts_upper = ts + window_size,
                        notification = Notification.EXCEPTION_AGG,

                        profiler = dict(
                            created_ts = time.time())
                        )

                    print '({0:0>6})[{1} - {2}]: | {3} |'.format(
                        message['event_id'],
                        arrow.get(message['event_ts']).strftime('%Y-%m-%d %H:%M:%S'),
                        arrow.get(message['event_ts_upper']).strftime('%Y-%m-%d %H:%M:%S'),
                        ' -> '.join(events)
                        )

                    self._offset += 1
                    delete = True

                    yield message

            if delete:
                del self._events[ts][:]
                delete = False

    def print_matched(self):
        print self._events
