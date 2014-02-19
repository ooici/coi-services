import gevent
from gevent.event import Event

def looping_call(interval, callable):
    """
    Returns a greenlet running your callable in a loop and an Event you can set
    to terminate the loop cleanly.
    """
    ev = Event()
    def loop(interval, callable):
        while not ev.wait(timeout=interval):
            callable()
    return gevent.spawn(loop, interval, callable), ev

