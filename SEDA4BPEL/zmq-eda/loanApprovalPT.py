#!/usr/bin/env python

import zmq

# Getting context and defining bindings
context = zmq.Context()

queue = context.socket(zmq.SUB)
pub = context.socket(zmq.PUSH)

queue.connect("tcp://localhost:20000")
queue.setsockopt(zmq.SUBSCRIBE, 'loanApproval')

pub.bind("tcp://*:30000")