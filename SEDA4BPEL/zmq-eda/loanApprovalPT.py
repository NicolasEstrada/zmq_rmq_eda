#!/usr/bin/env python

# python loanApprovalPT.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml -at 10

import sys
import json
import time

import zmq
# import yaml
# import argparse

from message_profiler import MessageProfiler

from celery import Celery

# app_loan_approvals = Celery('loanApprovalPT', backend='amqp', broker='amqp://guest@localhost//')
app_loan_approvals = Celery()
app_loan_approvals.config_from_object('config.celery-loanApprovalPT')

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included
APP_TIME = 600

ALLOWED_SOCKET_TYPES = ('PUSH', 'PULL', 'XPUB' ,'SUB')
ALLOWED_MESSAGE_TYPES = (
    'creditInformationMessage',
    'approvalMessage')

CONFIG_SECTION = 'loanApprovalPT'


@app_loan_approvals.task
def run(config, message_patterns, port,
        message_type, client_id, app_time=APP_TIME,
        limit_amount=0, limit_time=0):

    # Getting context and defining bindings
    context = zmq.Context()

    queue_loan = context.socket(getattr(
        zmq,
        config['incoming']['loanService']['socket_type']))
    # queue_risk = context.socket(getattr(
    #     zmq,
    #     config['incoming']['risk']['socket_type']))
    pub = context.socket(getattr(
        zmq,
        config['outgoing']['socket_type']))

    queue_loan.connect("tcp://{host}:{port}".format(**config['incoming']['loanService']))
    queue_loan.setsockopt(
        zmq.SUBSCRIBE,
        str(config['incoming']['loanService']['routing_key']))
    client_binding = "{0}_{1}".format(str(client_id),
        config['incoming']['loanService']['routing_key'])
    queue_loan.setsockopt(zmq.SUBSCRIBE, str(client_binding))

    # queue_risk.bind("tcp://{host}:{port}".format(**config['incoming']['risk']))
    # queue_risk.connect("tcp://{host}:{port}".format(**config['incoming']['risk']))
    # queue_risk.setsockopt(zmq.SUBSCRIBE, config['incoming']['risk']['routing_key'])

    pub.connect("tcp://{host}:{port}".format(**config['outgoing']))

    # poller = zmq.Poller()
    # poller.register(queue_loan, zmq.POLLIN)
    # poller.register(queue_risk, zmq.POLLIN)

    try:
        with MessageProfiler(CONFIG_SECTION, True) as mp:

            while ((limit_amount == 0 or mp.count_in <= limit_amount)
                    and (limit_time == 0 or time.time() - mp.start <= limit_time)):
                # socks = dict(poller.poll())

                # if socks.get(queue_loan) == zmq.POLLIN:
                #     rkey, message = queue_loan.recv_multipart()
                # elif socks.get(queue_risk) == zmq.POLLIN:
                    # rkey, message = queue_risk.recv_multipart()
                # else:
                #     print("[WARNING] Wrong socket for message [%s] RKEY: [%s]" % (message, rkey))
                #     continue

                rkey, message = queue_loan.recv_multipart()

                print("Received message [%s] RKEY: [%s]" % (message, rkey))

                if 'loanApproval' not in rkey:
                    print("[WARNING] Wrong rkey for message [%s] RKEY: [%s]" % (message, rkey))
                    continue

                message = json.loads(message)
                message['profiler']['loanApprovalPT_ts'] = time.time()
                message['accept'] = 'yes'
                rkey = str(config['outgoing']['routing_key'])
                time.sleep(app_time)

                size_str = sys.getsizeof(rkey + str(message))
                mp.msg_received(size_str)

                pub.send_multipart([rkey, json.dumps(message)])
                print("Sent message [%s] RKEY: [%s]" % (message, rkey))

                size_str = sys.getsizeof(rkey + str(message))
                mp.msg_sent(size_str)

            return mp.stats

    except:
        queue_loan.close()
        # queue_risk.close()
        pub.close()
        context.term()
