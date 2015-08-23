#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Receiver handler for traffic control.

Receives events from sensors to be routed
by SECTION (depends on SENSOR_ID).

Example:
    Execution mode:

        $ python receiver.py


Schema:

      SUB
    --------
 --| sensor |--
    --------
      PUB

       PULL
    ----------
 --| receiver |--
    ----------
       XPUB

        SUB
    ------------
 --| controller |--
    ------------
        PUSH

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
        conf.receiver['incoming']['socket_type']))
    rcv.bind("tcp://{host}:{port}".format(**conf.receiver['incoming']))

    pub = context.socket(getattr(
        zmq,
        conf.receiver['outgoing']['socket_type']))
    pub.bind("tcp://{host}:{port}".format(**conf.receiver['outgoing']))

    try:
        while True:

            rkey, message = rcv.recv_multipart()
            print("[receiver] Received message [%s] RKEY: [%s]" % (message, rkey))
            message = json.loads(message)

            message['profiler']['receiver_ts'] = time.time()

            pub.send_multipart([rkey, json.dumps(message)])
            print("[receiver] Sent message [%s] RKEY: [%s]" % (message, rkey))

    except:
        rcv.close()
        pub.close()
        context.term()


if __name__ == '__main__':
    run()

