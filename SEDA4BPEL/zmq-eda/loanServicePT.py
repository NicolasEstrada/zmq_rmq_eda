#!/usr/bin/env python

import zmq

# Getting context and defining bindings
context = zmq.Context()

rcv = context.socket(zmq.PULL)
pub = context.socket(zmq.XPUB)

rcv.bind("tcp://*:10000")
pub.bind("tcp://*:20000")