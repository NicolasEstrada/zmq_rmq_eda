import time

LOG_STR = ("Elapsed time: {0} ms | Messages received: {1} | "
           "Messages sent: {2} | Bytes in: {3} | Bytes out: {4} | "
           "Messages in ratio: {5} msg/s | Messages out ratio: {6} msg/s | "
           "Bytes in ratio: {7} b/s | Bytes out ratio: {8}")

class MessageProfiler(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

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
        self.msecs = self.secs * 1000  # millisecs
        self.ratio_in = float(self.count_in / self.secs)
        self.bratio_in = float(self.bytes_in / self.secs)
        self.ratio_out = float(self.count_out / self.secs)
        self.bratio_out = float(self.bytes_out / self.secs)
        if self.verbose:
            msg = LOG_STR.format(
                self.msecs, self.count_in, self.count_out,
                self.bytes_in, self.bytes_out, self.ratio_in,
                self.ratio_out, self.bratio_in, self.bratio_out)
            print msg

    def msg_received(self, bytes):
        self.count_in += 1
        self.bytes_in += bytes

    def msg_sent(self, bytes):
        self.count_out += 1
        self.bytes_out += bytes