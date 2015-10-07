#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Sensor handler for traffic control.

This simulates the behaviour of sensor based system receiving events
from static log file collector to be sent for further processing.

Example:
    Execution mode:

        $ python sensor.py --id SENSOR_ID


Attributes:
    sensor_id (int): id of the sensor to be simulated


Schema:

      <csv file>
    --------------
 --| trace-driven |--
    --------------
         XPUB


      SUB
    --------
 --| sensor |--
    --------
      PUSH

       PULL
    ----------
 --| receiver |--
    ----------
       XPUB

"""

import sys
import time
import ujson as json
import argparse

import zmq

from config import zmq_config as conf

__author__ = "Nicolas Estrada"
__version__ = "1.0.0"
__email__ = "nicoestrada.i@gmail.com"
__status__ = "Development"


def run(sensor_id=0):

    context = zmq.Context()

    # receive socket configuration
    sensor_receive = context.socket(getattr(
        zmq,
        conf.sensor['incoming']['socket_type'])
    )
    sensor_receive_url = "tcp://{host}:{port}".format(**conf.sensor['incoming'])
    sensor_receive.connect(sensor_receive_url)
    sensor_receive.setsockopt(zmq.SUBSCRIBE, str(sensor_id))

    # publish socket configuration
    sensor_publish = context.socket(getattr(
        zmq,
        conf.sensor['outgoing']['socket_type'])
    )
    sensor_publish_url = "tcp://{host}:{port}".format(**conf.sensor['outgoing'])
    sensor_publish.connect(sensor_publish_url)

    try:

        while True:
            # Sensor receiving events

            rkey, message = sensor_receive.recv_multipart()
            message = json.loads(message)
            # print(
            #     "[SID %s] Received event [%s] RKEY: [%s]"
            #         % (str(sensor_id), message, rkey)
            # )

            message['profiler']['sensor_received_ts'] = time.time()

            # message['profiler']['sensor_received_id'] = sensor_id
            rkey = 'event'
            sensor_publish.send_multipart([rkey, json.dumps(message)])
            # print(
            #     "[SID %s] Sent event [%s] RKEY: [%s]"
            #         % (str(sensor_id), message, rkey)
            # )

    except KeyboardInterrupt:
        sensor_receive.close()
        sensor_publish.close()
        context.term()
        sys.exit(0)

    except:
        # Closing zmq connections and context
        sensor_receive.close()
        sensor_publish.close()
        context.term()
        raise
        # sys.exit(1)


if __name__ == '__main__':

    # Arguments for sensor id
    parser = argparse.ArgumentParser(
        description="""Script that receives data from event generator.""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-sid',
        '--sensor_id',
        default='',
        help='ID of sensor to be binded')

    args = parser.parse_args()
    run(args.sensor_id)