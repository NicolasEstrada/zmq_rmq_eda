#!/usr/bin/env python

# python client.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml -fn nico -n estrada -a 2000 1000 15000 3000 6000 500 300 -ci 50001

import sys
import json
import time
import random

import zmq
import yaml
import argparse

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included
MIN_CLIENT_ID = 50000 # not included

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
        '-ci',
        '--client_id',
        type=int,
        required=True,
        help='client id')
    parser.add_argument(
        '-a',
        '--amount',
        nargs='*',
        required=True,
        type=int,
        help='loan amount requested')

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

    if (args.client_id and
        args.client_id > MIN_CLIENT_ID and
        args.client_id < MAX_PORT):

        client_id = args.client_id
    else:
        raise Exception(
            "Value for client_id must be greater than {0} and less than {1}"
            .format(MIN_CLIENT_ID, MAX_PORT))

    # Client message specific values
    for amount in args.amount:
        if amount <= 0:
            raise Exception("Value for amount must be greater than 0")

    values = {
        "firstName": args.first_name,
        "name": args.name,
        "amount": random.choice(args.amount)}

    context = zmq.Context()
    client_request = context.socket(getattr(
        zmq,
        config['outgoing']['socket_type']))
    client_receive = context.socket(getattr(
        zmq,
        config['incoming']['socket_type']))

    client_request_url = "tcp://{host}:{port}".format(**config['outgoing'])
    client_receive_url = "tcp://{host}:{port}".format(**config['incoming'])
    client_request.connect(client_request_url)
    client_receive.connect(client_receive_url)
    client_receive.setsockopt(zmq.SUBSCRIBE, str(client_id))

    try:
        while True:
            message.update(values)
            rkey = config['outgoing']['routing_key']

            size_str = sys.getsizeof(rkey + str(message))

            message['profiler']['client_send_ts'] = time.time()
            message['profiler']['client_id'] = client_id
            client_request.send_multipart([rkey, json.dumps(message)])
            print("Sent message [%s] RKEY: [%s]" % (message, rkey))

            values.update({'amount': random.choice(args.amount)})

            # Waiting loanService response to proceed
            rkey, message = client_receive.recv_multipart()
            message = json.loads(message)
            message['profiler']['client_received_ts'] = time.time()
            print("Received message [%s] RKEY: [%s], Elapsed time: [%s] seconds" % (
                message, rkey,
                message['profiler']['client_received_ts'] - message['profiler']['client_send_ts']))

            time.sleep(5)
    except:
        raise
        client_request.close()
        client_receive.close()
        context.term()
