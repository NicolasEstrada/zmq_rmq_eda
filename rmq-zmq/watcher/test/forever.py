from __future__ import print_function

import sys
import time


def main():
    print('Parameters: {}'.format(sys.argv[1:]))
    while True:
        time.sleep(10)


if __name__ == '__main__':
    main()
