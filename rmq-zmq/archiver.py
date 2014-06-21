#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Archiver documentation.

This scripts implements the archivers using ZeroMQ receiving,
processing and storing incoming messages.

Example:
    Archivers usage example as follows:
    usage: python archiver.py [-h] [-c [CONFIG_FILE]] [-v [VHOST]]
                   [-q [QUEUE]] [-rk [ROUTING_KEY [ROUTING_KEY ...]]]
                   [-r [RULES]] [-oa] [-fo [FORMAT_OUTPUT]]
                   [-so [STORAGE_OUTPUT]] [-oo [output_level]]

Arguments:
  -c,  --config_file    config file name within config/ folder to be used
  -v,  --vhost          vhost to be used (included in a config section)
  -q,  --queue          queue name to be used
  -rk, --routing_key    routing keys to be used
  -r,  --rules          rules to be used
  -oa, --omit_archive   flag to omit raw data archiving
  -fo, --format_output  format of validated data
  -so, --storage_output format of raw data
  -oo, --omit_output    value to omit validated data archiving (
                            0 = no output, 1 = only valid ,2 = only invalid)

  -tm, --test_mode      flag for testing mode

"""

# python archiver.py -v thesis -rk routing_key.example -s thesis -posto plain

from __future__ import division

__author__ = "Nicolas Estrada"
__version__ = "0.0.1"

import os
import sys
import time
import gzip
import json
import argparse
from functools import partial

import zmq
import arrow
import msgpack

PARENT_DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PARENT_DIRECTORY)

from message_profiler import MessageProfiler
from python_utils import config_loader, logging_manager
from validation.process import MessageValidator, MessageProcessor


PROJECT = 'kraken'
PREFIX = 'hourly-archive_'

ALLOWED_OUTPUT_FORMAT = set((
    "plain",
    "gzip",
    "msgpack"
))

OUTPUT_LEVELS = {
    'OMIT_OUTPUT': 0,
}

RULES_PATH = 'validation/rules/'
FILENAME_TEMPLATE = "{name}{ext}"
SCRIPT_DIRNAME = os.path.dirname(os.path.abspath(__file__))


if __name__ == "__main__":
    # Arguments for spores
    parser = argparse.ArgumentParser(
        description="""Archiver that handles incoming data messages from
        RabbitMQ, process/clean them and finally store it for uploading.""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-c',
        '--config_file',
        help='config file to be used')
    parser.add_argument(
        '-v',
        '--vhost',
        help='vhost to be used')
    # parser.add_argument(
    #     '-q',
    #     '--queue',
    #     help='queue to be used')
    parser.add_argument(
        '-s',
        '--source',
        required=True,
        help='source key name')
    parser.add_argument(
        '-rk',
        '--routing_key',
        nargs='+',
        help='routings keys for queue binding')
    parser.add_argument(
        '-r',
        '--rules',
        nargs='?',
        help='rules to be used')
    parser.add_argument(
        '-oa',
        '--omit_archive',
        action='store_true',
        help='queue to be used')
    parser.add_argument(
        '-preo',
        '--pre_process_output',
        default='gzip',
        choices=ALLOWED_OUTPUT_FORMAT,
        help='storage output to be used')
    parser.add_argument(
        '-posto',
        '--post_process_output',
        default='gzip',
        choices=ALLOWED_OUTPUT_FORMAT,
        help='output format to be used')
    parser.add_argument(
        '-ol',
        '--output_level',
        type=int,
        default=3,
        help='omit output rule')
    # parser.add_argument(
    #     '-tm',
    #     '--test_mode',
    #     action='store_true',
    #     help='flag to activate test mode')

    args = parser.parse_args()

    if args.config_file is not None:
        config_file = args.config_file
    else:
        config_file = 'archiver.yaml'

    if args.vhost is not None:
        section = args.vhost
    else:
        section = 'default'

    logger = logging_manager.start_logger(
        'archiver-{0}'.format(args.source),
        use_root_logger=False)

    config = config_loader.load(
        config_file,
        section=section)

    # If we want to override the default queue and routing key
    # it could be done through commnd line arguments
    if args.routing_key:
        config['routing_key'] = args.routing_key

        logger.info("Using routing_key={0}".format(
            config['routing_key'][0]))
    else:
        logger.info("Flags: {0}".format(args._get_kwargs()))
        logger.info('Using default config file')

    # Initiating the validator module
    if args.rules is not None:
        try:
            rules = config_loader.load(
                '{0}.yaml'.format(args.rules),
                config_path=RULES_PATH)
        except IOError:
            rules = None
    else:
        rules = None

    if rules is not None:
        validator = MessageValidator(rules, logger=logger)
        processor = MessageProcessor(rules, logger=logger)
    else:
        validator = processor = None

    source = args.source


STORAGE_PATHS = {
    'raw': os.path.join(SCRIPT_DIRNAME, config['storage_path']),
    'valid': os.path.join(SCRIPT_DIRNAME, config['valid_path']),
    'invalid': os.path.join(SCRIPT_DIRNAME, config['invalid_path'])
}

STORAGE_HANDLERS = {
    'gzip': {
        'content': partial(json.dumps),
        'handler': partial(gzip.GzipFile, mode='ab'),
        'extension': '.gz'
    },
    'plain': {
        'content': partial(json.dumps),
        'handler': partial(open, mode='a'),
        'extension': ''
    },
    'msgpack': {
        'content': partial(msgpack.packb),
        'handler': partial(open, mode='a'),
        'extension': '.msgpack'
    }
}


# @profile
def store_message(data, path, fname, storage_format):
    # Check if the directory tree exists
    if not os.path.isdir(path):
        # If not, creates the non-existent directories
        os.makedirs(path)

    storage = STORAGE_HANDLERS[storage_format]
    filename = FILENAME_TEMPLATE.format(name=fname, ext=storage['extension'])
    file_path = os.path.join(path, filename)

    with storage['handler'](file_path) as output:
        output.write(storage['content'](data))
        output.write('\n')

# @profile
def handle_message(body, rkey):
    """This function takes care of storing messages in files, and adding
    some parameters like the complete routing key, and the date when the message
    was received. Each line is a JSON object.
    """

    # We need the type and the event to distribute the messages in
    # the right file
    routing_key = rkey.split('.')

    # Creating the archive message
    json_valid = False

    try:
        msg = json.loads(body)
    except TypeError:
        msg = body
        if isinstance(body, dict):
            json_valid = True
    except Exception:
        msg = body
    else:
        json_valid = True

    data = {
        "rk": routing_key,
        "dt": int(time.time() * 1000),
        "msg": msg
    }

    # Date for the archiving structure
    try:
        message_dt = arrow.get(msg['datetime'] / 1000)
    except TypeError:
        # If it realtes with an error message
        message_dt =arrow.get(data['dt'] / 1000)

    folder  = message_dt.format('YYYY-MM-DD')
    filename = message_dt.format('HH00.txt')

    if isinstance(source, basestring):
        post_path = source
    else:
        post_path = os.path.join(*source)

    # post_path = os.path.join(post_path, folder, filename)
    post_path = os.path.join(post_path, folder)

    # Writting archive files
    if not args.omit_archive:
        store_message(
            data,
            os.path.join(STORAGE_PATHS['raw'], post_path),
            filename,
            args.pre_process_output)

    # Validation section
    if args.output_level == OUTPUT_LEVELS['OMIT_OUTPUT']:
        pass
    elif json_valid:
        # In the case that the script have rules, there should be a validator
        # If no rules are present, no validator or processor is present
        if rules is not None:
            is_message_valid = validator.validate(data['msg'])
        else:
            is_message_valid = True

        if is_message_valid and args.output_level != 2:
            if processor is not None:
                data['msg'] = processor.process(data['msg'])

            store_message(
                data,
                os.path.join(STORAGE_PATHS['valid'], post_path),
                filename,
                args.post_process_output)
        else:
            # Invalid message
            if args.output_level != 1:
                store_message(
                    data,
                    os.path.join(STORAGE_PATHS['invalid'], post_path),
                    filename,
                    args.post_process_output)
    else:
        # Invalid message
        if args.output_level != 1:
            store_message(
                data,
                os.path.join(STORAGE_PATHS['invalid'], post_path),
                filename,
                args.post_process_output)
    return


def main():
    """Main function, in charge of make the connection to the server
    """

    context = zmq.Context()
    archiver = context.socket(zmq.PULL)
    archiver.connect("tcp://localhost:12001")

    try:
        with MessageProfiler(True) as mp:
            while True:
                rkey, message = archiver.recv_multipart()
                mp.msg_received(sys.getsizeof(rkey + message))
                # print("Received message: [%s] RKEY: [%s]" % (message, rkey))
                handle_message(message, rkey)

    except Exception, e:
        archiver.close()
        context.term()
        raise e

if __name__ == '__main__':
    logger.info('Starting the Archiver')
    main()
    logger.info('Archiver is resting in peace')


# context = zmq.Context()
# archiver = context.socket(zmq.SUB)
# archiver.connect("tcp://localhost:12001")
# archiver.setsockopt(zmq.SUBSCRIBE, b"routing_key.example")

# try:
#     while True:
#         rkey, message = archiver.recv_multipart()
#         print("Received message: [%s] RKEY: [%s]" % (message, rkey))

# except Exception, e:
#     archiver.close()
#     context.term()
#     raise e
