#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.oms_events
@file    ion/agents/platform/rsn/simulator/oms_events.py
@author  Carlos Rueda
@brief   OMS simulator event definitions and supporting functions.
         Demo program included that allows to run both a listener server and a
         notifier. See demo program usage at the end of this file.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import sys
from time import sleep
import time
import ntplib
from urlparse import urlparse
import httplib
import yaml
import json

from ion.agents.platform.rsn.simulator.logger import Logger
log = Logger.get_logger()

if not getattr(log, "trace", None):
    setattr(log, "trace", log.debug)


##########################################################################
# The "event type" concept was removed from the interface (~Apr/2013).
# To minimize changes in the code, simply introduce an 'ALL' event type here.


class EventInfo(object):
    EVENT_TYPES = {
        'ALL': {
            'name':          'on battery',
            'severity':      3,
            'group':         'power',
        }
    }


class EventNotifier(object):

    def __init__(self):

        # _listeners: { event_type: {url: reg_time, ...}, ... }
        # initialize with empty dict for each event type:
        self._listeners = dict((et, {}) for et in EventInfo.EVENT_TYPES)

    def add_listener(self, url, event_type):
        assert event_type in EventInfo.EVENT_TYPES

        url_dict = self._listeners[event_type]

        if not url in url_dict:
            url_dict[url] = ntplib.system_to_ntp_time(time.time())
            log.trace("added listener=%s for event_type=%s", url, event_type)

        return url_dict[url]

    def remove_listener(self, url, event_type):
        assert event_type in EventInfo.EVENT_TYPES

        url_dict = self._listeners[event_type]

        unreg_time = 0
        if url in url_dict:
            unreg_time = ntplib.system_to_ntp_time(time.time())
            del url_dict[url]
            log.trace("removed listener=%s for event_type=%s", url, event_type)

        return unreg_time

    def notify(self, event_instance):
        """
        Notifies the event to all associated listeners.
        """
        assert isinstance(event_instance, dict)

        urls = self._listeners['ALL']
        if not len(urls):
            # no event listeners for event_type; just ignore notification:
            return

        # copy list to get a snapshot of the current dictionary and thus avoid
        # concurrent modification kind of runtime errors like:
        # RuntimeError: dictionary changed size during iteration
        urls = list(urls)
        for url in urls:
            self._notify_listener(url, event_instance)

    def _notify_listener(self, url, event_instance):
        """
        Notifies event to given listener.
        """
        if url == "http://NO_OMS_NOTIFICATIONS":  # pragma: no cover
            # developer convenience -see ion.agents.platform.rsn.oms_event_listener
            return

        log.debug("Notifying event_instance=%s to listener=%s", str(event_instance), url)

        # include url in event instance for diagnostic/debugging purposes:
        event_instance['listener_url'] = url

        # prepare payload (array of event instances in JSON format):
        payload = json.dumps([event_instance], indent=2)
        log.trace("payload=\n%s", payload)
        headers = {
            "Content-type": "application/json",
            "Accept": "text/plain"
        }

        conn = None
        try:
            o = urlparse(url)
            url4conn = o.netloc
            path     = o.path

            conn = httplib.HTTPConnection(url4conn)
            conn.request("POST", path, body=payload, headers=headers)
            response = conn.getresponse()
            data = response.read()
            log.trace("RESPONSE: %s, %s, %s", response.status, response.reason, data)
        except Exception as e:
            # the actual listener is no longer there; just log a message
            log.warn("event notification HTTP request failed: %r: %s", url, e)
        finally:
            if conn:
                conn.close()


class EventGenerator(object):
    """
    Simple helper to generate and trigger event notifications.
    """

    def __init__(self, notifier, events_filename=None):
        self._notifier = notifier

        self._events = None
        if events_filename:
            try:
                with open(events_filename, 'r') as f:
                    log.info('loading events from: %s', events_filename)
                    pyobj = yaml.load(f)
                    self._events = [dict(obj) for obj in pyobj]
            except Exception as ex:
                log.warn('could not load events from %s. Continuing with '
                         'hard-coded event', events_filename)

        self._keep_running = True
        self._index = 0  # in EventInfo.EVENT_TYPES or self._events

        # self._runnable set depending on whether we're under pyon or not
        if 'pyon' in sys.modules:
            from gevent import Greenlet
            self._runnable = Greenlet(self._run)
            log.debug("!!!! EventGenerator: pyon detected: using Greenlet")
        else:
            from threading import Thread
            self._runnable = Thread(target=self._run)
            self._runnable.setDaemon(True)
            log.debug("!!!! EventGenerator: pyon not detected: using Thread")

    def generate_and_notify_event(self):
        if self._events:
            if self._index >= len(self._events):
                self._index = 0

            event = self._events[self._index]
            self._index += 1

            event_id    = event['event_id']
            platform_id = event['platform_id']
            message     = event.get('message', "%s's message" % event_id)
            group       = event.get('group', '')
            severity    = event.get('severity', 3)

        else:
            if self._index >= len(EventInfo.EVENT_TYPES):
                self._index = 0
            event_type = EventInfo.EVENT_TYPES.values()[self._index]
            self._index += 1

            event_id    = "TODO_some_event_id"
            platform_id = "TODO_some_platform_id"
            message = "%s (synthetic event generated from simulator)" % event_type['name']
            group = event_type['group']
            severity = event_type['severity']

        timestamp = ntplib.system_to_ntp_time(time.time())
        first_time_timestamp = timestamp
        event_instance = {
            'event_id':              event_id,
            'message':               message,
            'platform_id':           platform_id,
            'timestamp':             timestamp,
            'first_time_timestamp':  first_time_timestamp,
            'severity':              severity,
            'group':                 group,
        }

        log.debug("notifying event_instance=%s", str(event_instance))
        self._notifier.notify(event_instance)

    def start(self):
        self._runnable.start()

    def _run(self):
        sleep(3)  # wait a bit before first event
        while self._keep_running:
            self.generate_and_notify_event()
            # sleep for a few secs regularly checking we still are running
            secs = 7
            while self._keep_running and secs > 0:
                sleep(0.3)
                secs -= 0.3
        log.trace("event generation stopped.")

    def stop(self):
        log.trace("stopping event generation...")
        self._keep_running = False


if __name__ == "__main__":  # pragma: no cover
    #
    # first, call this demo program with command line argument 'listener',
    # then, on a second terminal, with argument 'notifier'
    #
    host, port = "localhost", 8000
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "listener":
        # run listener
        from gevent.pywsgi import WSGIServer
        def application(environ, start_response):
            #print('listener got environ=%s' % str(environ))
            print(" ".join(('%s=%s' % (k, environ[k])) for k in [
                'CONTENT_LENGTH','CONTENT_TYPE', 'HTTP_ACCEPT']))
            input = environ['wsgi.input']
            body = "".join(input.readlines())
            print('body=\n%s' % body)
            #
            # note: the expected content format is JSON and we can in general
            # parse with either json or yaml ("every JSON file is also a valid
            # YAML file" -- http://yaml.org/spec/1.2/spec.html#id2759572):
            #
            event_instance = yaml.load(body)
            print('event_instance=%s' % str(event_instance))

            # respond OK:
            headers = [('Content-Type', 'text/plain') ]
            status = '200 OK'
            start_response(status, headers)
            return "MY-RESPONSE. BYE"
        print("%s:%s: listening for event notifications..." % (host, port))
        WSGIServer((host, port), application).serve_forever()

    elif len(sys.argv) > 1 and sys.argv[1] == "notifier":
        # run notifier
        notifier = EventNotifier()
        url = "http://%s:%s" % (host, port)
        for event_type in EventInfo.EVENT_TYPES.keys():
            notifier.add_listener(url, event_type)
            print("registered listener to event_type=%r" % event_type)
        generator = EventGenerator(notifier)
        secs = 15
        print("generating events for %s seconds ..." % secs)
        generator.start()
        sleep(secs)
        generator.stop()

    else:
        print("usage: call me with arg 'listener' or 'notifier'")


"""
Test program

TERMINAL 1:
$ bin/python  ion/agents/platform/rsn/simulator/oms_events.py listener
localhost:8000: listening for event notifications...

TERMINAL 2:
$ bin/python  ion/agents/platform/rsn/simulator/oms_events.py notifier
oms_simulator: setting log level to: logging.WARN
registered listener to event_type='ALL'
generating events for 15 seconds ...


TERMINAL 1:
CONTENT_LENGTH=270 CONTENT_TYPE=application/json HTTP_ACCEPT=text/plain
body=
{
  "group": "power",
  "severity": 3,
  "url": "http://localhost:8000",
  "timestamp": 3578265811.422655,
  "platform_id": "TODO_some_platform_id",
  "message": "on battery (synthetic event generated from simulator)",
  "first_time_timestamp": 3578265811.422655
}
event_instance={'platform_id': 'TODO_some_platform_id', 'group': 'power', 'severity': 3, 'url': 'http://localhost:8000', 'timestamp': 3578265811.422655, 'message': 'on battery (synthetic event generated from simulator)', 'first_time_timestamp': 3578265811.422655}
127.0.0.1 - - [2013-05-22 19:43:31] "POST / HTTP/1.1" 200 118 0.002814
CONTENT_LENGTH=270 CONTENT_TYPE=application/json HTTP_ACCEPT=text/plain
body=
{
  "group": "power",
  "severity": 3,
  "url": "http://localhost:8000",
  "timestamp": 3578265818.647295,
  "platform_id": "TODO_some_platform_id",
  "message": "on battery (synthetic event generated from simulator)",
  "first_time_timestamp": 3578265818.647295
}
event_instance={'platform_id': 'TODO_some_platform_id', 'group': 'power', 'severity': 3, 'url': 'http://localhost:8000', 'timestamp': 3578265818.647295, 'message': 'on battery (synthetic event generated from simulator)', 'first_time_timestamp': 3578265818.647295}
127.0.0.1 - - [2013-05-22 19:43:38] "POST / HTTP/1.1" 200 118 0.003455
"""
