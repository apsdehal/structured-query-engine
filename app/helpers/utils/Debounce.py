"""
Taken from https://gist.github.com/ChrisTM/5834503
"""
import threading
from datetime import datetime, timedelta
from functools import wraps


class Debounce(object):
    """
    To create a function that will only be called if it has
    been called for 1 minute:
        @throttle(minutes=1)
        def my_fun():
            pass
    """
    def __init__(self, seconds=0, minutes=0, hours=0):
        self.debounce_period = timedelta(
            seconds=seconds, minutes=minutes, hours=hours
        ).total_seconds()
        self.time_of_last_call = datetime.min

    def __call__(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            time_since_last_call = now - self.time_of_last_call
            self.time_of_last_call = now

            if time_since_last_call.total_seconds() < self.debounce_period:
                self.timer.cancel()

            self.timer = threading.Timer(self.debounce_period, fn, args=args, kwargs=kwargs)
            self.timer.start()
        return wrapper
