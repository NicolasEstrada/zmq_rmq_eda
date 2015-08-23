#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Controller handler for traffic control.

Receives events from event handlers and process them
to find patterns, generates new aggregated events and
create sliding windows for further analysis.

Also takes care of CEP (Complex Event Processing).

Example:
    Execution mode:

        $ python controller.py


Schema:

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


        PULL
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

    queue = context.socket(getattr(
        zmq,
        conf.controller['incoming']['socket_type'])
    )
    queue.connect("tcp://{host}:{port}".format(**conf.controller['incoming']))
    queue.setsockopt(zmq.SUBSCRIBE, conf.controller['incoming']['routing_key'])

    pub = context.socket(getattr(
        zmq,
        conf.controller['outgoing']['socket_type'])
    )
    pub.connect("tcp://{host}:{port}".format(**conf.controller['outgoing']))

    cep = context.socket(getattr(
        zmq,
        conf.controller['cep']['socket_type'])
    )
    cep.connect("tcp://{host}:{port}".format(**conf.controller['cep']))

    try:

        while True:

            rkey, message = queue.recv_multipart()
            print("[controller] Received message [%s] RKEY: [%s]" % (message, rkey))

            message = json.loads(message)
            message['profiler']['riskAssessmentPT_ts'] = time.time()

            pub.send_multipart([rkey, json.dumps(message)])
            print("[controller - db] Sent message [%s] RKEY: [%s]" % (message, rkey))

            cep.send_multipart([rkey, json.dumps(message)])
            print("[controller - cep] Sent message [%s] RKEY: [%s]" % (message, rkey))

    except:
        queue.close()
        pub.close()
        cep.close()
        context.term()


if __name__ == '__main__':
    run()
