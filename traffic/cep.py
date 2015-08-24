#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Complex event processing for traffic control.

Receives events from controller to process them and create aggregated data.

Example:
    Execution mode:

        $ python cep.py

Schema:


        SUB                    PULL
    ------------              -------
 --| controller | PUSH --> --|  cep  |
    ------------              -------
        PUSH                    PUSH
         ||                      ||
         ||                      ||
        PULL <-------------------||
    ------------
 --|    data    |--
    ------------
     <Database>

"""

import sys
import time
import json
import numpy
from collections import deque

import zmq
from matplotlib import pyplot

import cep_tools
from config import zmq_config as conf

__author__ = "Nicolas Estrada"
__version__ = "1.0.0"
__email__ = "nicoestrada.i@gmail.com"
__status__ = "Development"

X_POINTS = 100
MAX_LENGTH = 100000
# speeds = deque([], MAX_LENGTH)
speeds = []
notification = cep_tools.Notification()


def run():

    context = zmq.Context()

    rcv = context.socket(getattr(
        zmq,
        conf.cep['incoming']['socket_type']))
    rcv.bind("tcp://{host}:{port}".format(**conf.cep['incoming']))

    pub = context.socket(getattr(
        zmq,
        conf.cep['outgoing']['socket_type'])
    )
    pub.connect("tcp://{host}:{port}".format(**conf.cep['outgoing']))

    functions = {
        'send_event': lambda rk, msg: pub.send_multipart([rk, json.dumps(msg)]),
        'cep_agg': lambda rk, msg: (rk, msg)
    }

    try:
        while True:

            rkey, message = rcv.recv_multipart()
            # print("[cep] Received message [%s] RKEY: [%s]" % (message, rkey))
            message = json.loads(message)

            message['profiler']['data_ts'] = time.time()

            # cep processing: moving avg; min/max threshold speed

            speeds.append(message['speed'])  # replace using Redis
            cep_event = notification.check(message['speed'], speeds)

            # event shift (semantic, granularity and sliding windows)
            for action in cep_event['event']['actions']:

                if cep_event['notify_id'] in conf.cep['events'][action]:
                    functions[action](
                        cep_event['event']['routing_key'],
                        message)

            # pub.send_multipart([rkey, json.dumps(message)])
            # print("[cep] Sent message [%s] RKEY: [%s]" % (message, rkey))

    except KeyboardInterrupt:
        rcv.close()
        pub.close()
        context.term()
        sys.exit(0)
        
    except:
        rcv.close()
        pub.close()
        context.term()
        raise
        # sys.exit(1)
    finally:
        # plotting results
        speeds_compressed = []

        offset = int(numpy.floor(len(speeds) / X_POINTS))

        for i in xrange(X_POINTS):
            # compressing data points
            lower = int(i * offset)
            upper = int((i * offset) + offset + 1)
            speeds_compressed.append(
                numpy.average(speeds[lower:upper])
                )

        # x = [i + 1 for i in xrange(len(speeds))]
        # mvg_avg = cep_tools.moving_average(speeds)

        # pyplot.plot(x[len(x) - len(mvg_avg):], mvg_avg)
        # pyplot.plot(x, speeds)
        # pyplot.show()

        x = [i + 1 for i in xrange(len(speeds_compressed))]
        mvg_avg = cep_tools.moving_average(speeds_compressed)

        pyplot.plot(x[len(x) - len(mvg_avg):], mvg_avg)
        pyplot.plot(x, speeds_compressed)
        pyplot.show()


if __name__ == '__main__':
    run()
