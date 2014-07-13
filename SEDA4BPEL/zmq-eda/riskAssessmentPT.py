#!/usr/bin/env python

# python riskAssessmentPT.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml

import sys
import json
import time
import random

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

CONFIG_SECTION = 'riskAssessmentPT'


if __name__ == "__main__":

    # Arguments for riskAssessmentPT
    parser = argparse.ArgumentParser(
        description="""Handler for riskAssessmentPT that receives
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
    pub_low = context.socket(getattr(
        zmq,
        config['outgoing']['low_risk']['socket_type']))
    pub_approval = context.socket(getattr(
        zmq,
        config['outgoing']['approval']['socket_type']))

    queue.connect("tcp://{host}:{port}".format(**config['incoming']))
    queue.setsockopt(zmq.SUBSCRIBE, config['incoming']['routing_key'])

    pub_low.connect("tcp://{host}:{port}".format(**config['outgoing']['low_risk']))
    pub_approval.connect("tcp://{host}:{port}".format(**config['outgoing']['approval']))

    try:
        with MessageProfiler(CONFIG_SECTION, True) as mp:

            while True:
                rkey, message = queue.recv_multipart()
                print("Received message [%s] RKEY: [%s]" % (message, rkey))

                if rkey != 'riskAssessment':
                    print("[WARNING] Wrong rkey for message [%s] RKEY: [%s]" % (message, rkey))
                    continue

                message = json.loads(message)
                message['profiler']['riskAssessmentPT_ts'] = time.time()

                size_str = sys.getsizeof(rkey + str(message))
                mp.msg_received(size_str)

                rand = random.randint(0,4)
                if not rand:  # 80% for low risk
                    rkey = config['outgoing']['low_risk']['routing_key']
                    pub_low.send_multipart([rkey, json.dumps(message)])
                    message['level'] = 'low'
                    message['accept'] = 'yes'
                else:
                    rkey = config['outgoing']['approval']['routing_key']
                    pub_approval.send_multipart([rkey, json.dumps(message)])
                    if rand in (1,2):
                        message['level'] = 'medium'
                    elif rand in (3,4):
                        message['level'] = 'high'

                print("Sent message [%s] RKEY: [%s]" % (message, rkey))

                size_str = sys.getsizeof(rkey + str(message))
                mp.msg_sent(size_str)

    except:
        queue.close()
        pub_low.close()
        pub_approval.close()
        context.term()
