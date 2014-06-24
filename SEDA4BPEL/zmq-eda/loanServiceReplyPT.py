#!/usr/bin/env python

# python loanServiceReplyPT.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml

import sys
import json
import time

import zmq
import yaml
import argparse

from message_profiler import MessageProfiler

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included

ALLOWED_SOCKET_TYPES = ('PUSH', 'PULL', 'XPUB' ,'SUB')
ALLOWED_MESSAGE_TYPES = (
    'creditInformationMessage',
    'approvalMessage')

CONFIG_SECTION = 'loanServiceReplyPT'


if __name__ == "__main__":

    # Arguments for loanServiceReplyPT
    parser = argparse.ArgumentParser(
        description="""Handler for loanServiceReplyPT that receives
        creditInformationMessages/approvalMessages from other services
        and deliver the response to the correspondient client""",
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

    rcv = context.socket(getattr(
        zmq,
        config['incoming']['socket_type']))
    pub = context.socket(getattr(
        zmq,
        config['outgoing']['socket_type']))

    rcv.bind("tcp://{host}:{port}".format(**config['incoming']))
    pub.bind("tcp://{host}:{port}".format(**config['outgoing']))

    try:
        with MessageProfiler(True) as mp:

            while True:
                rkey, message = rcv.recv_multipart()
                print("Received message [%s] RKEY: [%s]" % (message, rkey))

                if rkey != 'loanServiceReply':
                    print("[WARNING] Wrong rkey for message [%s] RKEY: [%s]" % (message, rkey))
                    continue
                
                message = json.loads(message)

                size_str = sys.getsizeof(rkey + str(message))
                mp.received(size_str)

                message['profiler']['loanServiceReplyPT_ts'] = time.time()
                pub.send_multipart([
                    str(message['profiler']['client_id']),
                    json.dumps(message)])
                print("Message sent: [%s] RKEY: [%s]" % (message, rkey))

                size_str = sys.getsizeof(rkey + str(message))
                mp.sent(size_str)

    except:
        rcv.close()
        pub.close()
        context.term()
