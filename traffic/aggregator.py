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
import json

import zmq

from config import zmq_config as conf

__author__ = "Nicolas Estrada"
__version__ = "1.0.0"
__email__ = "nicoestrada.i@gmail.com"
__status__ = "Development"


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

    try:

        while True:

            rkey, message = queue.recv_multipart()
            # print("[aggregator] Received message [%s] RKEY: [%s]" % (message, rkey))

            message = json.loads(message)
            message['profiler']['aggregator_ts'] = time.time()

            if CONDITION AGG:
                pub.send_multipart([rkey, json.dumps(message)])
                # print("[aggregator - db] Sent message [%s] RKEY: [%s]" % (message, rkey))

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
