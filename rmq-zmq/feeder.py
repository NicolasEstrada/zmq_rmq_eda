#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Feeder documentation.

This scripts implements the feeders using ZeroMQ,
that sends raw messages.

usage: python feeder.py

"""

__author__ = "Nicolas Estrada"
__version__ = "0.0.1"

import sys
import time

import zmq

from message_profiler import MessageProfiler

# Connecting ...
context = zmq.Context()
feeder = context.socket(zmq.PUSH)
feeder.connect("tcp://localhost:10001")


# @profile
def send_message(socket, rkey, message):
  socket.send_multipart([rkey, message])
  return


try:
    with MessageProfiler(True) as mp:
        rkey = 'routing_key.example'
        message = '{"datetime": 1234567890123, "data": "LOTS_OF_DATA_INSIDE_LARGE_STRING"}'
        size_str = sys.getsizeof(rkey + message)
        while True:
            # feeder.send_multipart([rkey, message])
            send_message(feeder, rkey, message)
            mp.msg_sent(size_str)
            # print("Sent message [%s] RKEY: [%s]" % (message, rkey))
            time.sleep(0.1)
except:
    feeder.close()
    context.term()
