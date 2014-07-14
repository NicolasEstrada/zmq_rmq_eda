#!/usr/bin/env python

# python client.py -cf ./config/zmq-eda.yaml -pf ./config/loan_approval.yaml -fn nico -n estrada -a 2000 1000 15000 3000 6000 500 300 -ci 50001

import sys
import json
import time
import random

import zmq
import yaml
# import argparse

from message_profiler import MessageProfiler

from celery import Celery

app_clients = Celery('clients', backend='amqp', broker='amqp://guest@localhost//')

MIN_PORT = 1024  # not included
MAX_PORT = 65536  # not included
MIN_CLIENT_ID = 50000 # not included
WAIT_TIME = 2

ALLOWED_SOCKET_TYPES = ('PUSH', 'PULL', 'XPUB' ,'SUB')
ALLOWED_MESSAGE_TYPES = ('creditInformationMessage')

CONFIG_SECTION = 'client'


@app_clients.task
def run(config, message_patterns, port,
        message_type, first_name, name,
        client_id, amount, wait_time=WAIT_TIME,
        limit_amount=0, limit_time=0):

    generic_message = message_patterns[config['outgoing']['message_type']]
    generic_message.update({'profiler': message_patterns['profiler']})
    message = generic_message

    # Socket and servers options
    if port and port > MIN_PORT and port < MAX_PORT:
        config['outgoing']['port'] = port

    if (client_id and
        client_id > MIN_CLIENT_ID and
        client_id < MAX_PORT):

        client_id = client_id
    else:
        raise Exception(
            "Value for client_id must be greater than {0} and less than {1}"
            .format(MIN_CLIENT_ID, MAX_PORT))

    # Client message specific values
    for a in amount:
        if a <= 0:
            raise Exception("Value for amount must be greater than 0")

    values = {
        "firstName": first_name,
        "name": name,
        "amount": random.choice(amount)}

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
        log_fname = "{0}_{1}".format(CONFIG_SECTION, client_id)
        with MessageProfiler(log_fname, True) as mp:

            # import pdb; pdb.set_trace()
            while ( (-limit_amount <= -mp.count_in)
                    and (mp.start - time.time() <= -limit_time)):
                message.update(values)
                rkey = config['outgoing']['routing_key']

                message['profiler']['client_send_ts'] = time.time()
                message['profiler']['client_id'] = client_id
                client_request.send_multipart([rkey, json.dumps(message)])
                print("Sent message [%s] RKEY: [%s]" % (message, rkey))

                size_str = sys.getsizeof(rkey + str(message))
                mp.msg_sent(size_str)

                values.update({'amount': random.choice(amount)})

                # Waiting loanService response to proceed
                rkey, message = client_receive.recv_multipart()
                message = json.loads(message)
                message['profiler']['client_received_ts'] = time.time()
                print("Received message [%s] RKEY: [%s], Elapsed time: [%s] seconds" % (
                    message, rkey,
                    message['profiler']['client_received_ts'] - message['profiler']['client_send_ts']))

                size_str = sys.getsizeof(rkey + str(message))
                mp.msg_received(size_str)

                time.sleep(wait_time)

        # import pdb; pdb.set_trace()
        return mp.stats

    except:
        raise
        client_request.close()
        client_receive.close()
        context.term()


@app_clients.task
def result(list_of_dicts):
    # Sum by field and get results
    # print list_of_dicts
    # import pdb; pdb.set_trace()
    return list_of_dicts
