
import random
import string
import argparse

import yaml
# from celery import chord
from celery import group
from clients import (run,
                    ALLOWED_MESSAGE_TYPES,
                    WAIT_TIME, MIN_PORT, MAX_PORT,
                    MIN_CLIENT_ID, CONFIG_SECTION)

# python client_task.py -cf config/zmq-eda.yaml -pf config/loan_approval.yaml -a 13000 -lt 70


def random_string(length):
   return ''.join(random.choice(string.lowercase) for i in xrange(length))


if __name__ == '__main__':
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
    parser.add_argument(
        '-p',  # port must be > 1024 and <= 65535
        '--port',
        type=int,
        # choices=xrange(MIN_PORT, MAX_PORT + 1),
        help='overrides the port within the config file')
    parser.add_argument(
        '-mt',
        '--message_type',
        choices=ALLOWED_MESSAGE_TYPES,
        help='message type for Loan Approval step')
    parser.add_argument(
        '-fn',
        '--first_name',
        required=False,
        type=str,
        help='first name of client request')
    parser.add_argument(
        '-n',
        '--name',
        required=False,
        type=str,
        help='name of client request')
    # parser.add_argument(
    #     '-ci',
    #     '--client_id',
    #     type=int,
    #     required=True,
    #     help='client id')
    parser.add_argument(
        '-a',
        '--amount',
        nargs='*',
        required=True,
        type=int,
        help='loan amount requested')
    parser.add_argument(
        '-wt',
        '--wait_time',
        required=False,
        type=int,
        default=WAIT_TIME,
        help='wait time in seconds between request response received and next request')

    parser.add_argument(
        '-cq',
        '--client_quantity',
        required=False,
        type=int,
        default=1,
        help='number of client instances running')

    parser.add_argument(
        '-la',
        '--limit_amount',
        required=False,
        type=int,
        default=0,
        help='limit for client requests, 0 = no-limit')

    parser.add_argument(
        '-lt',
        '--limit_time',
        required=False,
        type=int,
        default=0,
        help='limit for client uptime, 0 = no-limit')

    args = parser.parse_args()

    # Config files dependencies
    config = yaml.load(args.config_file)[CONFIG_SECTION]
    message_patterns = yaml.load(args.patterns_file)

    # Limits for clients ids
    lower_limit = MIN_CLIENT_ID + 1
    upper_limit = MIN_CLIENT_ID + 1 + args.client_quantity

    # report = chord(
    #             run.s(
    #                 config, message_patterns, args.port,
    #                 args.message_type, random_string(random.randint(3,10)),
    #                 random_string(random.randint(4,10)), client_id, args.amount,
    #                 args.wait_time, args.limit_amount, args.limit_time
    #                 ) for client_id in xrange(lower_limit, upper_limit)
    #             )(result.s()).get()

    clients_job = group(
                run.s(
                    config, message_patterns, args.port,
                    args.message_type, random_string(random.randint(3,10)),
                    random_string(random.randint(4,10)), client_id, args.amount,
                    args.wait_time, args.limit_amount, args.limit_time
                    ) for client_id in xrange(lower_limit, upper_limit)
                )

    result = clients_job.apply_async(queue='clients_tasks')

    results = result.get()

    requests_received = 0
    response_time = 0

    print results

    # import pdb; pdb.set_trace()
    for i, res in enumerate(results):
        print i
        requests_received += res['low']['requests_received']
        response_time += res['low']['response_time']

    print "rqs received: ", requests_received, "; response_time: ", response_time

    message = "For {0} clients, the average response was: {1}".format(
        args.client_quantity,
        float(response_time / requests_received))
    print message
