
import sys
import csv
import random
import argparse

import arrow

offset = 0

def get_speed(avg_speed):
    # replace by random with standard deviation, average and normal distribution
    return avg_speed * (1 + (random.randint(0, 20)  - 10) / 100.0)

if __name__ == '__main__':

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


    with open(data_file_path, 'rb') as data:

        data_reader = csv.reader(data)

        for i, row in enumerate(data_reader):
            if i > 0:
                date = "{0}-{1:02d}-{2:02d} {3:02d}:{4:02d}:00".format(
                    row[2],
                    int(row[3]),
                    int(row[4]),
                    int(row[5]),
                    int(row[6]))

                # print date
                timestamp = arrow.get(date)
                scans = int(row[7].replace(",", ""))
                avg_speed = float(row[8].replace(",", "."))

                log = "Datetime: {0}, Scans: {1}, Speed: {2}".format(
                    timestamp,
                    scans,
                    avg_speed)

                for n in xrange(scans):
                    # TODO: instead of print, should be send/publish
                    print "[S{0}][{1}] Speed: {2} \n".format(
                        row[0],
                        offset + n + 1,
                        get_speed(avg_speed))

                offset += n + 1
