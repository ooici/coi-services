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


from gevent import Greenlet, sleep

import time
import httplib
import yaml

from ion.agents.platform.oms.simulator.logger import Logger
log = Logger.get_logger()


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


class EventGenerator(Greenlet):
    """
    Simple helper to generate and trigger event notifications.
    """

    def __init__(self, notifier):
        Greenlet.__init__(self)
        self._notifier = notifier
        self._keep_running = True
        self._index = 0  # in EventInfo.EVENT_TYPES

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
        event_type = EventInfo.EVENT_TYPES.keys()[0]
        notifier.add_listener(url, event_type)
        print("registered listener to event_type=%r" % event_type)
        generator = EventGenerator(notifier)
        secs = 6
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
generating events for 6 seconds ...
generated event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794478.977933, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.78'}
generated event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794479.983365, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.77'}
generated event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794480.983749, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.78'}
generated event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794481.989795, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.77'}
generated event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794482.990179, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.78'}
generated event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794483.996283, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.77'}

TERMINAL 1:
listener got event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794478.977933, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.78'}
127.0.0.1 - - [2012-09-27 18:07:58] "POST / HTTP/1.1" 200 118 0.002234
listener got event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794480.983749, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.78'}
127.0.0.1 - - [2012-09-27 18:08:00] "POST / HTTP/1.1" 200 118 0.002637
listener got event_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794482.990179, 'message': 'synthetic event', 'group': 'power', 'ref_id': '44.78'}
127.0.0.1 - - [2012-09-27 18:08:02] "POST / HTTP/1.1" 200 118 0.002571
"""
