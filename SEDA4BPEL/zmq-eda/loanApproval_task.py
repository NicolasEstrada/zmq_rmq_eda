
import time
import random
import string
import argparse

import yaml
from celery import group
from clients import MIN_CLIENT_ID
from loanApprovalPT import (run,
                    ALLOWED_MESSAGE_TYPES,
                    APP_TIME,
                    CONFIG_SECTION)


def random_string(length):
   return ''.join(random.choice(string.lowercase) for i in xrange(length))


if __name__ == '__main__':
    # Arguments for client
    parser = argparse.ArgumentParser(
        description="""Tasks manager for loanApprovalPT,
        starting necessary instances to receive clients requests""",
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
    # parser.add_argument(
    #     '-ci',
    #     '--client_id',
    #     type=int,
    #     required=True,
    #     help='client id')
    # parser.add_argument(
    #     '-a',
    #     '--amount',
    #     nargs='*',
    #     required=True,
    #     type=int,
    #     help='loan amount requested')
    parser.add_argument(
        '-at',
        '--app_time',
        required=False,
        type=int,
        default=APP_TIME,
        help='wait time in seconds between request response received and next request')

    parser.add_argument(
        '-iq',
        '--instances_quantity',
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
    upper_limit = MIN_CLIENT_ID + 1 + args.instances_quantity

    start_time = time.time()

    loanApproval_job = group(
                run.s(
                    config, message_patterns, args.port,
                    args.message_type, client_id,
                    app_time=args.app_time, limit_amount=args.limit_amount,
                    limit_time=args.limit_time, start_time=start_time
                    ) for client_id in xrange(lower_limit, upper_limit)
                )

    result = loanApproval_job.apply_async(queue='loanApprovalPT_tasks')

    print result.get(propagate=False)
