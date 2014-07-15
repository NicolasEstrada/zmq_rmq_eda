import signal

class Timeout():
    """Timeout class using ALARM signal."""
    class Timeout(Exception):
        pass
 
    def __init__(self, sec):
        self.sec = sec
 
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.setitimer(signal.ITIMER_REAL, self.sec, 0)
 
    def __exit__(self, *args):
        signal.alarm(0)
 
    def raise_timeout(self, *args):
        raise Timeout.Timeout()