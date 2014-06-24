#!/usr/bin/env python

# python loanServicePT.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml -t 10000

import sys
import json
import time

import zmq
import yaml
import argparse

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included

ALLOWED_SOCKET_TYPES = ('PUSH', 'PULL', 'XPUB' ,'SUB')
ALLOWED_MESSAGE_TYPES = ('creditInformationMessage')

CONFIG_SECTION = 'loanServicePT'


if __name__ == "__main__":

    # Arguments for loanServicePT
    parser = argparse.ArgumentParser(
        description="""Handler for loanServicePT that receives
        creditInformationMessages from clients and send them for
        riskAssesment or loanApproval depending bussiness criteria""",
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
        '-t',
        '--threshold',
        required=True,
        type=int,
        default=10000,
        help='threshold amount for non-required loan approval')

    args = parser.parse_args()

    # Config files dependencies
    config = yaml.load(args.config_file)[CONFIG_SECTION]

    # Getting context and defining bindings
    context = zmq.Context()

    rcv = context.socket(getattr(
        zmq,
        config['incoming']['socket_type']))
    pub = context.socket(getattr(
        zmq,
        config['outgoing']['socket_type']))

    rcv.bind("tcp://{host}:{port}".format(**config['incoming']))
    pub.bind("tcp://{host}:{port}".format(**config['outgoing']))

    try:
        while True:
            rkey, message = rcv.recv_multipart()
            print("Received message [%s] RKEY: [%s]" % (message, rkey))

            if rkey != 'loanService':
                print("[WARNING] Wrong rkey for message [%s] RKEY: [%s]" % (message, rkey))
                continue
            
            message = json.loads(message)

            if message['amount'] < args.threshold:
                rkey = config['outgoing']['routing_key']['low_amount']
            else:
                rkey = config['outgoing']['routing_key']['high_amount']

            size_str = sys.getsizeof(rkey + str(message))

            message['profiler']['loanServicePT_ts'] = time.time()
            pub.send_multipart([rkey, json.dumps(message)])
            print("Sent message [%s] RKEY: [%s]" % (message, rkey))
    except:
        rcv.close()
        pub.close()
        context.term()
