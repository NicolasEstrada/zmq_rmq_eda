#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Event generator for traffic control.

Generates events from a csv file input and send them
with the corresponding SENSOR_ID to its further process.

Example:
    Execution mode:

        $ python trace-driven.py


Schema:

     <csv file>
   --------------
--| trace-driven |--
   --------------
        XPUB


     SUB
   --------
--| sensor |--
   --------
     PUB

"""

import sys
import csv
import time
import json
import random
import argparse

import zmq
import arrow

from config import zmq_config as conf


def get_speed(avg_speed):
    # replace by random with standard deviation,
    # average and normal distribution
    return avg_speed * (1 + (random.randint(0, 20)  - 10) / 100.0)


def run():

    context = zmq.Context()

    pub = context.socket(getattr(
        zmq,
        conf.generator['outgoing']['socket_type'])
    )
    pub.bind("tcp://{host}:{port}".format(**conf.generator['outgoing']))

    with open(data_file_path, 'rb') as data:

        data_reader = csv.reader(data)
        offset = 0

        for i, row in enumerate(data_reader):
            if i > 0:
                date = "{0}-{1:02d}-{2:02d} {3:02d}:{4:02d}:00".format(
                    row[2],
                    int(row[3]),
                    int(row[4]),
                    int(row[5]),
                    int(row[6]))

                timestamp = arrow.get(date)
                scans = int(row[7].replace(",", ""))
                avg_speed = float(row[8].replace(",", "."))

                log = "Datetime: {0}, Scans: {1}, Speed: {2}".format(
                    timestamp,
                    scans,
                    avg_speed)

                for n in xrange(scans):
                    # TODO: event generator with ts within range

                    sensor_id = row[0]
                    event_id = offset + n + 1
                    speed = get_speed(avg_speed)

                    print "[S{0}][{1}] Speed: {2} \n".format(
                        sensor_id,
                        event_id,
                        speed)

                    message = dict(
                        sensor_id = str(sensor_id),
                        event_id = event_id,
                        speed = speed,
                        profiler = dict(
                            created_ts = time.time())
                        )
                    rkey = str(sensor_id)

                    pub.send_multipart([
                        rkey,
                        json.dumps(message)])

                    print("Message sent: [%s] RKEY: [%s]" % (message, rkey))

                    time.sleep(.5)

                offset += n + 1


if __name__ == '__main__':
    # WINDOW SIZE < AVERAGE MEDITIONS

    # Arguments for trace-driven test
    parser = argparse.ArgumentParser(
        description="""Script that fetch data from a csv file
        to get it ready and reproduce it as is ocurring now.""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-ifp',
        '--input_file_path',
        required=True,
        help='input file path to be used')

    args = parser.parse_args()

    if args.input_file_path is not None:
        data_file_path = args.input_file_path
    else:
        sys.exit(1)

    run()
