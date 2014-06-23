#!/usr/bin/env python

# python loanApprovalPT.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml

import sys
import json
import time

import zmq
import yaml
import argparse

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included

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

    args = parser.parse_args()

    # Config files dependencies
    config = yaml.load(args.config_file)[CONFIG_SECTION]

    # Getting context and defining bindings
    context = zmq.Context()

    queue = context.socket(getattr(
        zmq,
        config['incoming']['socket_type']))
    pub = context.socket(getattr(
        zmq,
        config['outgoing']['socket_type']))

    queue.connect("tcp://{host}:{port}".format(**config['incoming']))
    # queue.connect("tcp://{host}:20001".format(**config['incoming']))
    # TODO: how to consume form multiple port or multiple senders?
    queue.setsockopt(zmq.SUBSCRIBE, config['incoming']['routing_key'])

    pub.bind("tcp://*:{port}".format(**config['outgoing']))

    try:
        while True:
            rkey, message = queue.recv_multipart()
            print("Received message [%s] RKEY: [%s]" % (message, rkey))

            if rkey != 'loanApproval':
                print("[WARNING] Wrong rkey for message [%s] RKEY: [%s]" % (message, rkey))
                continue

            message = json.loads(message)
            message['profiler']['loanApprovalPT_ts'] = time.time()
            rkey = config['outgoing']['routing_key']
            time.sleep(15)  # original set sleep to 600

            size_str = sys.getsizeof(rkey + str(message))

            pub.send_multipart([rkey, json.dumps(message)])
            print("Sent message [%s] RKEY: [%s]" % (message, rkey))
    except:
        queue.close()
        pub.close()
        context.term()
