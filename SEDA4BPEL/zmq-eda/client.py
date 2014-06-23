#!/usr/bin/env python

# python client.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml -fn nico -n estrada -a 2000 1000 15000 3000 6000 500 300

import sys
import json
import time
import random

import zmq
import yaml
import argparse

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included

ALLOWED_SOCKET_TYPES = ('PUSH', 'PULL', 'XPUB' ,'SUB')
ALLOWED_MESSAGE_TYPES = ('creditInformationMessage')

CONFIG_SECTION = 'client'


if __name__ == "__main__":

    # Arguments for client
    parser = argparse.ArgumentParser(
        description="""Client handler that sends creditInformationMessages
        to the loanServicePT for loan approval evaluation""",
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
    # parser.add_argument(
    #     '-oh',
    #     '--outgoing_host',
    #     help='overrides the host within the config file')
    parser.add_argument(
        '-p',  # port must be > 1024 and <= 65535
        '--port',
        type=int,
        # choices=xrange(MIN_PORT, MAX_PORT + 1),
        help='overrides the port within the config file')
    # parser.add_argument(
    #     '-st',
    #     '--socket_type',
    #     choices=ALLOWED_SOCKET_TYPES,
    #     help='socket type to be used for outgoing messages')
    parser.add_argument(
        '-mt',
        '--message_type',
        choices=ALLOWED_MESSAGE_TYPES,
        help='message type for Loan Approval step')
    parser.add_argument(
        '-fn',
        '--first_name',
        required=True,
        type=str,
        help='first name of client request')
    parser.add_argument(
        '-n',
        '--name',
        required=True,
        type=str,
        help='name of client request')
    parser.add_argument(
        '-a',
        '--amount',
        nargs='*',
        required=True,
        type=int,
        help='loan amont requested')

    args = parser.parse_args()

    # Config files dependencies
    config = yaml.load(args.config_file)[CONFIG_SECTION]
    # import pdb; pdb.set_trace()
    message_patterns = yaml.load(args.patterns_file)
    generic_message = message_patterns[config['outgoing']['message_type']]
    generic_message.update({'profiler': message_patterns['profiler']})
    message = generic_message

    # Socket and servers options
    if args.port and args.port > MIN_PORT and args.port < MAX_PORT:
        config['outgoing']['port'] = args.port

    # Client message specific values
    for amount in args.amount:
        if amount <= 0:
            raise Exception("Value for amount must be greater than 0")
            sys.exit(1)

    values = {
        "firstName": args.first_name,
        "name": args.name,
        "amount": random.choice(args.amount)}

    context = zmq.Context()
    client_request = context.socket(getattr(
        zmq,
        config['outgoing']['socket_type']))

    client_request_url = "tcp://{host}:{port}".format(**config['outgoing'])
    client_request.connect(client_request_url)

    try:
        while True:
            message.update(values)
            rkey = config['outgoing']['routing_key']

            size_str = sys.getsizeof(rkey + str(message))

            message['profiler']['client_send_ts'] = time.time()
            client_request.send_multipart([rkey, json.dumps(message)])
            print("Sent message [%s] RKEY: [%s]" % (message, rkey))

            values.update({'amount': random.choice(args.amount)})
            time.sleep(5)
    except:
        client_request.close()
        context.term()
