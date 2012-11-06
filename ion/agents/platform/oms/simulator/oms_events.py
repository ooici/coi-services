#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.oms_events
@file    ion/agents/platform/oms/simulator/oms_events.py
@author  Carlos Rueda
@brief   OMS simulator event definitions and supporting functions.
         Demo program included that allows to run both a listener server and a
         notifier.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


import sys
from time import sleep
import time
import httplib
import yaml

from ion.agents.platform.oms.simulator.logger import Logger
log = Logger.get_logger()

if not getattr(log, "trace", None):
    setattr(log, "trace", log.debug)


class EventInfo(object):
    EVENT_TYPES = {

        '44.77': {
            'ref_id':        '44.77',
            'name':          'on battery',
            'severity':      3,
            'platform_type': 'UPS',
            'group':         'power',
            },

        '44.78': {
            'ref_id':        '44.78',
            'name':          'low battery',
            'severity':      3,
            'platform_type': 'UPS',
            'group':         'power',
            },
    }


class EventNotifier(object):

    def __init__(self):

        # { event_type: {url: reg_time, ...}, ... }
        # initialize with empty dict for each event type:
        self._listeners = dict((at, {}) for at in EventInfo.EVENT_TYPES)

    def add_listener(self, url, event_type):
        assert event_type in EventInfo.EVENT_TYPES

        url_dict = self._listeners[event_type]

        if not url in url_dict:
            url_dict[url] = time.time()
            log.trace("added listener=%s for event_type=%s", url, event_type)

        return url_dict[url]

    def remove_listener(self, url, event_type):
        assert event_type in EventInfo.EVENT_TYPES

        url_dict = self._listeners[event_type]

        unreg_time = 0
        if url in url_dict:
            unreg_time = time.time()
            del url_dict[url]
            log.trace("removed listener=%s for event_type=%s", url, event_type)

        return unreg_time

    def notify(self, event_instance):
        """
        Notifies the event to all associated listeners.
        """
        assert isinstance(event_instance, dict)
        assert 'ref_id' in event_instance
        event_type = event_instance['ref_id']
        assert event_type in EventInfo.EVENT_TYPES

        urls = self._listeners[event_type]
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
        log.debug("Notifying event_instance=%s to listener=%s", str(event_instance), url)

        # include url in event instance:
        event_instance['url'] = url

        payload = yaml.dump(event_instance, default_flow_style=False)
        log.trace("payload=\n\t%s", payload.replace('\n', '\n\t'))
        headers = {"Content-type": "text/plain",
                   "Accept": "text/plain"}

        # remove "http://" prefix for the connection itself
        if url.lower().startswith("http://"):
            url4conn = url[len("http://"):]
        else:
            url4conn = url

        conn = httplib.HTTPConnection(url4conn)
        try:
            conn.request("POST", "", body=payload, headers=headers)
            response = conn.getresponse()
            data = response.read()
            log.trace("RESPONSE: %s, %s, %s", response.status, response.reason, data)
        except Exception as e:
            # the actual listener is no longer there; just log a message
            log.warn("event notification HTTP request failed: %r: %s", url, e)
        finally:
            conn.close()


class EventGenerator(object):
    """
    Simple helper to generate and trigger event notifications.
    """

    def __init__(self, notifier):
        self._notifier = notifier
        self._keep_running = True
        self._index = 0  # in EventInfo.EVENT_TYPES

        # self._runnable set depending on whether we're under pyon or not
        if 'pyon' in sys.modules:
            from gevent import Greenlet
            self._runnable = Greenlet(self._run)
            log.debug("!!!! EventGenerator: pyon detected: using Greenlet")
        else:
            from threading import Thread
            self._runnable = Thread(target=self._run)
            log.debug("!!!! EventGenerator: pyon not detected: using Thread")

    def generate_and_notify_event(self):
        if self._index >= len(EventInfo.EVENT_TYPES):
            self._index = 0

        event_type = EventInfo.EVENT_TYPES.values()[self._index]
        self._index += 1

        #
        # TODO create a more complete event_instance
        #
        ref_id = event_type['ref_id']
        platform_id = "TODO_some_platform_id_of_type_%s" % event_type['platform_type']
        message = "%s (synthetic event generated from simulator)" % event_type['name']
        group = event_type['group']
        timestamp = time.time()
        event_instance = {
            'ref_id':       ref_id,
            'message':      message,
            'platform_id':  platform_id,
            'timestamp':    timestamp,
            'group':        group,
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


if __name__ == "__main__":
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
            input = environ['wsgi.input']
            body = "\n".join(input.readlines())
            event_instance = yaml.load(body)
            print('listener got event_instance=%s' % str(event_instance))
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
$ bin/python  ion/agents/platform/oms/simulator/oms_events.py listener
localhost:8000: listening for event notifications...

TERMINAL 2:
$ bin/python  ion/agents/platform/oms/simulator/oms_events.py notifier
registered listener to event_type='44.78'
registered listener to event_type='44.77'
generating events for 15 seconds ...

TERMINAL 1:
listener got event_instance={'platform_id': 'TODO_some_platform_id_of_type_UPS', 'group': 'power', 'url': 'http://localhost:8000', 'timestamp': 1352221197.337092, 'message': 'low battery (synthetic event generated from simulator)', 'ref_id': '44.78'}
127.0.0.1 - - [2012-11-06 08:59:57] "POST / HTTP/1.1" 200 118 0.002844
listener got event_instance={'platform_id': 'TODO_some_platform_id_of_type_UPS', 'group': 'power', 'url': 'http://localhost:8000', 'timestamp': 1352221204.549481, 'message': 'on battery (synthetic event generated from simulator)', 'ref_id': '44.77'}
127.0.0.1 - - [2012-11-06 09:00:04] "POST / HTTP/1.1" 200 118 0.002282
"""
