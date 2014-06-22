#!/usr/bin/env python

import sys
import time

import zmq
import yaml
import argparse

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included
PORT_CHOICE = tuple(range(MIN_PORT, MAX_PORT + 1))

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
        required=True,
        type=int,
        help='loan amont requested')

    args = parser.parse_args()

    # with open(args.config_file) as cfile:
    #   config = yaml.load(args.config_file)[CONFIG_SECTION]

    # Config files dependencies
    config = yaml.load(args.config_file)[CONFIG_SECTION]
    # import pdb; pdb.set_trace()
    message_patterns = yaml.load(args.patterns_file)
    generic_message = message_patterns[config['outgoing']['message_type']]
    generic_message.update({'profiler': message_patterns['profiler']})
    # print config, '\n', '---------', '\n'
    # print message_patterns, '\n', '---------', '\n'
    # print generic_message, '\n', '---------', '\n'

    # Socket and servers options
    if args.port:
        config['outgoing']['port'] = args.port

    # Client message specific values
    if args.amount <= 0:
        raise Exception("Value for amount must be greater than 0")
        sys.exit(1)

    values = {
        "firstName": args.first_name,
        "name": args.name,
        "amount": args.amount}

    generic_message.update(values)
    # print generic_message, '\n', '---------', '\n'

    context = zmq.Context()
    # client_request = context.socket(zmq.PUSH)
    client_request = context.socket(getattr(
        zmq,
        config['outgoing']['socket_type']))

    client_request_url = "tcp://{host}:{port}".format(**config['outgoing'])
    client_request.connect(client_request_url)

    try:
        while True:
            rkey = config['outgoing']['routing_key']
            size_str = sys.getsizeof(rkey + generic_message)
            client_request.send_multipart([rkey, generic_message])
            # send_message(client_request, rkey, generic_message)
            # mp.msg_sent(size_str)
            print("Sent message [%s] RKEY: [%s]" % (generic_message, rkey))
            time.sleep(10)
    except:
        client_request.close()
        context.term()
