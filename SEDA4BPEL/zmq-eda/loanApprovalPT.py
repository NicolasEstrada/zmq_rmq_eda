#!/usr/bin/env python

# python loanApprovalPT.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml -at 10

import sys
import json
import time

import zmq
import yaml
import argparse

from message_profiler import MessageProfiler

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included
APP_TIME = 600

ALLOWED_SOCKET_TYPES = ('PUSH', 'PULL', 'XPUB' ,'SUB')
ALLOWED_MESSAGE_TYPES = (
    'creditInformationMessage',
    'approvalMessage')

CONFIG_SECTION = 'loanApprovalPT'


if __name__ == "__main__":

    # Arguments for loanApprovalPT
    parser = argparse.ArgumentParser(
        description="""Handler for loanApprovalPT that receives
        creditInformationMessages wait for 10 minutes, simulating
        approval process. After that, send the message to make the
        client response""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-cf',
        '--config_file',
        required=True,
        type=argparse.FileType('r'),
        help='config file to be used')
    parser.add_argument(
        '-pf',
        '--patterns_file',
        required=True,
        type=argparse.FileType('r'),
        help='file for message type format')
    parser.add_argument(
        '-at',
        '--app_time',
        required=True,
        type=int,
        default=APP_TIME,
        help='approval time in seconds')

    args = parser.parse_args()

    # Config files dependencies
    config = yaml.load(args.config_file)[CONFIG_SECTION]

    # Getting context and defining bindings
    context = zmq.Context()

    queue_loan = context.socket(getattr(
        zmq,
        config['incoming']['loanService']['socket_type']))
    queue_risk = context.socket(getattr(
        zmq,
        config['incoming']['risk']['socket_type']))
    pub = context.socket(getattr(
        zmq,
        config['outgoing']['socket_type']))

    queue_loan.connect("tcp://{host}:{port}".format(**config['incoming']['loanService']))
    queue_loan.setsockopt(zmq.SUBSCRIBE, config['incoming']['loanService']['routing_key'])

    queue_risk.bind("tcp://{host}:{port}".format(**config['incoming']['risk']))
    # queue_risk.connect("tcp://{host}:{port}".format(**config['incoming']['risk']))
    # queue_risk.setsockopt(zmq.SUBSCRIBE, config['incoming']['risk']['routing_key'])

    pub.connect("tcp://{host}:{port}".format(**config['outgoing']))

    poller = zmq.Poller()
    poller.register(queue_loan, zmq.POLLIN)
    poller.register(queue_risk, zmq.POLLIN)

    try:
        with MessageProfiler(True) as mp:

            while True:
                socks = dict(poller.poll())

                if socks.get(queue_loan) == zmq.POLLIN:
                    rkey, message = queue_loan.recv_multipart()
                elif socks.get(queue_risk) == zmq.POLLIN:
                    rkey, message = queue_risk.recv_multipart()
                else:
                    print("[WARNING] Wrong socket for message [%s] RKEY: [%s]" % (message, rkey))
                    continue

                print("Received message [%s] RKEY: [%s]" % (message, rkey))

                if rkey != 'loanApproval':
                    print("[WARNING] Wrong rkey for message [%s] RKEY: [%s]" % (message, rkey))
                    continue

                message = json.loads(message)
                message['profiler']['loanApprovalPT_ts'] = time.time()
                message['accept'] = 'yes'
                rkey = config['outgoing']['routing_key']
                time.sleep(args.app_time)

                size_str = sys.getsizeof(rkey + str(message))
                mp.received(size_str)

                pub.send_multipart([rkey, json.dumps(message)])
                print("Sent message [%s] RKEY: [%s]" % (message, rkey))

                size_str = sys.getsizeof(rkey + str(message))
                mp.sent(size_str)

    except:
        queue_loan.close()
        queue_risk.close()
        pub.close()
        context.term()
