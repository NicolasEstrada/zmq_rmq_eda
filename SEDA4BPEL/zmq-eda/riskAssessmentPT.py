#!/usr/bin/env python

import zmq

# Getting context and defining bindings
context = zmq.Context()

queue = context.socket(zmq.SUB)
pub = context.socket(zmq.PUSH)
pub_to_approval = context.socket(zmq.XPUB)

queue.connect("tcp://localhost:20000")
queue.setsockopt(zmq.SUBSCRIBE, 'riskAssessment')

pub.bind("tcp://*:30000")
pub_to_approval.bind("tcp://*:20000")