#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Aggregator handler for traffic control exception events.

Receives exceptions events from cep processors and
analyze them to find correlated events by sensor id.

This corresponds to Pattern Matching in Complex Event Processing

Example:
    Execution mode:

        $ python aggregator.py


Schema:

                                   -------
        SUB                       ------- | XPUB -->  |     ------------
    ------------                 ------- | XPUB ---> SUB - | aggregator | 
 --| controller | PUSH --> PULL |  cep  | XPUB ---->  |     ------------
    ------------                 -------                        PUSH
        PUSH                      PUSH                           ||
         ||                        ||                            ||
         ||                        ||                            ||
        PULL <---------------------||<---------------------------||
    ------------
 --|    data    |--
    ------------
     <Database>
"""

import sys
import time
import ujson as json

import zmq

from cep_tools import Aggregator
from config import zmq_config as conf

__author__ = "Nicolas Estrada"
__version__ = "1.0.0"
__email__ = "nicoestrada.i@gmail.com"
__status__ = "Development"

WINDOW_SIZE = 3600


def run():

    context = zmq.Context()

    queue = context.socket(getattr(
        zmq,
        conf.aggregator['incoming']['socket_type'])
    )
    queue.connect("tcp://{host}:{port}".format(**conf.aggregator['incoming']))
    queue.setsockopt(zmq.SUBSCRIBE, conf.aggregator['incoming']['routing_key'])

    pub = context.socket(getattr(
        zmq,
        conf.aggregator['outgoing']['socket_type'])
    )
    pub.connect("tcp://{host}:{port}".format(**conf.aggregator['outgoing']))

    functions = {
        'send_event': lambda rk, msg: pub.send_multipart([rk, json.dumps(msg)])
    }

    ts_base = None

    try:

        with Aggregator(queue, pub, True) as agg:
            while True:

                rkey, message = agg.receive()
                # print("[aggregator] Received message [%s] RKEY: [%s]" % (message, rkey))

                message = json.loads(message)
                message['profiler']['aggregator_ts'] = time.time()

                sid = message['sensor_id']
                ts_key = message['event_ts'] - message['event_ts'] % WINDOW_SIZE

                agg.register_event(sid, ts_key)

                if ts_base is None:
                    ts_base = ts_key
                else:
                    if not ts_base == ts_key and ts_key > ts_base:

                        for agg_event in agg.get_all(ts_base, WINDOW_SIZE):

                            for action in agg_event['notification']['actions']:
                                function[action](
                                    agg_event['notification']['routing_key'],
                                    message
                                    )

                        ts_base = ts_key

                    else:
                        pass

                # patterns = agg.check(ts_key)

                # if len(patterns):
                #     for pattern in patterns:
                #         print("[aggregator - pattern] Pattern found: %s" % (pattern,))
                    # print("[aggregator - db] Sent message [%s] RKEY: [%s]" % (message, rkey))
                    # agg.publish([rkey, json.dumps(message)])

    except KeyboardInterrupt:
        queue.close()
        pub.close()
        context.term()
        sys.exit(0)
        
    except:
        queue.close()
        pub.close()
        context.term()
        raise
        # sys.exit(1)


if __name__ == '__main__':
    run()
