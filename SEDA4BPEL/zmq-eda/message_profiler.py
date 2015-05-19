import time
import json

LOG_STR = ("Elapsed time: {secs} s | Messages received: {count_in} | "
           "Messages sent: {count_out} | MB in: {mb_in} | MB out: {mb_out} | "
           "Messages-in ratio: {ratio_in} msg/s | Messages-out ratio: {ratio_out} msg/s | "
           "MB-in ratio: {mb_ratio_in} MB/s | MB-out ratio: {mb_ratio_out} MB/s")

class MessageProfiler(object):
    def __init__(self, name="default", verbose=False):
        self.verbose = verbose
        self.name = name

    def __enter__(self):
        self.start = time.time()
        self.count_in = 0
        self.count_out = 0
        self.bytes_in = 0
        self.bytes_out = 0
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.ratio_in = float(self.count_in / self.secs)
        self.mb_ratio_in = float(self.bytes_in / (self.secs * 1024 * 1024))
        self.ratio_out = float(self.count_out / self.secs)
        self.mb_ratio_out = float(self.bytes_out / (self.secs * 1024 * 1024))
        self.mb_in = self.bytes_in / (1024 * 1024)
        self.mb_out = self.bytes_out / (1024 * 1024)

        self.stats = {
            "secs": self.secs,
            "count_in": self.count_in,
            "count_out": self.count_out,
            "ratio_in": self.ratio_in,
            "ratio_out": self.ratio_out,
            "mb_in": self.mb_in,
            "mb_out": self.mb_out,
            "mb_ratio_in": self.mb_ratio_in,
            "mb_ratio_out": self.mb_ratio_out
        }

        # TODO: Handle log folder creation
        # with open("log/{0}.log".format(self.name), "a") as f:
        #     f.write(json.dumps(self.stats) + '\n')

        if self.verbose:
            msg = LOG_STR.format(**self.stats)
            print msg

    def msg_received(self, bytes):
        self.count_in += 1
        self.bytes_in += bytes

    def msg_sent(self, bytes):
        self.count_out += 1
        self.bytes_out += bytes


class ClientMessageProfiler(MessageProfiler):
    def __init__(self, name="default", verbose=False):
        self.response_time = {
            "low": {
                "requests_received": 0,
                "response_time": 0
            },

            "default": {
                "requests_received": 0,
                "response_time": 0
            },

            "high": {
                "requests_received": 0,
                "response_time": 0
            }
        }
        super(ClientMessageProfiler, self).__init__(name, verbose)

    def update_response_time(self, key, elapsed_time):
        try:
            self.response_time[key]["requests_received"] += 1
            self.response_time[key]["response_time"] += elapsed_time
        except KeyError:
            self.response_time[key] = {
                "requests_received": 0,
                "response_time": 0
            }

            self.response_time[key]["requests_received"] += 1
            self.response_time[key]["response_time"] += elapsed_time

    def get_response_time(self):
        return self.response_time
