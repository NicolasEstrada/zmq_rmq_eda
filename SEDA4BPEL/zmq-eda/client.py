#!/usr/bin/env python

import zmq

context = zmq.Context()
client_request = context.socket(zmq.PUSH)

client_request.connect("tcp://localhost:10000")
