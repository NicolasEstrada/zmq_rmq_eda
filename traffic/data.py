#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Data handler for traffic control.

Receives events from controller for further storage/visualization.

Example:
    Execution mode:

        $ python data.py

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

    rcv = context.socket(getattr(
        zmq,
        conf.data['incoming']['socket_type']))
    rcv.bind("tcp://{host}:{port}".format(**conf.data['incoming']))

    try:
        while True:

            rkey, message = rcv.recv_multipart()
            print("[data] Received message [%s] RKEY: [%s]" % (message, rkey))
            message = json.loads(message)

            message['profiler']['data_ts'] = time.time()

            # db store

    except:
        rcv.close()
        context.term()


if __name__ == '__main__':
    run()
