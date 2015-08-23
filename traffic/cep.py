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

import zmq

import cep_tools
from config import zmq_config as conf

__author__ = "Nicolas Estrada"
__version__ = "1.0.0"
__email__ = "nicoestrada.i@gmail.com"
__status__ = "Development"

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

    try:
        while True:

            rkey, message = rcv.recv_multipart()
            # print("[cep] Received message [%s] RKEY: [%s]" % (message, rkey))
            message = json.loads(message)

            message['profiler']['data_ts'] = time.time()

            # cep processing: moving avg; min/max threshold speed

            speeds.append(message['speed'])  # replace using Redis
            notification.check(message['speed'], speeds)

            pub.send_multipart([rkey, json.dumps(message)])
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


if __name__ == '__main__':
    run()
