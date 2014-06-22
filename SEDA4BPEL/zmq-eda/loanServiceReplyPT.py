#!/usr/bin/env python

import zmq

context = zmq.Context()
loan_reply = context.socket(zmq.PULL)

loan_reply.connect("tcp://localhost:30000")