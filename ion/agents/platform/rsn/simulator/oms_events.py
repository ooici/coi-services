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
import httplib
import yaml
import json

from ion.agents.platform.rsn.simulator.logger import Logger
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

        # prepare payload (JSON format):
        payload = json.dumps(event_instance, indent=2)
        log.trace("payload=\n%s", payload)
        headers = {
            "Content-type": "application/json",
            "Accept": "text/plain"
        }

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
            self._runnable.setDaemon(True)
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
        timestamp = ntplib.system_to_ntp_time(time.time())
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
2013-01-30 20:28:19,128 DEBUG    MainThread oms_simulator  :67 add_listener added listener=http://localhost:8000 for event_type=44.78
registered listener to event_type='44.78'
2013-01-30 20:28:19,128 DEBUG    MainThread oms_simulator  :67 add_listener added listener=http://localhost:8000 for event_type=44.77
registered listener to event_type='44.77'
2013-01-30 20:28:19,128 DEBUG    MainThread oms_simulator  :159 __init__ !!!! EventGenerator: pyon not detected: using Thread
generating events for 15 seconds ...
2013-01-30 20:28:22,129 DEBUG    Thread-1 oms_simulator  :184 generate_and_notify_event notifying event_instance={'platform_id': 'TODO_some_platform_id_of_type_UPS', 'timestamp': 3568595302.1292267, 'message': 'low battery (synthetic event generated from simulator)', 'group': 'power', 'ref_id': '44.78'}
2013-01-30 20:28:22,129 DEBUG    Thread-1 oms_simulator  :109 _notify_listener Notifying event_instance={'platform_id': 'TODO_some_platform_id_of_type_UPS', 'timestamp': 3568595302.1292267, 'message': 'low battery (synthetic event generated from simulator)', 'group': 'power', 'ref_id': '44.78'} to listener=http://localhost:8000
2013-01-30 20:28:22,129 DEBUG    Thread-1 oms_simulator  :116 _notify_listener payload=
{
  "group": "power",
  "url": "http://localhost:8000",
  "timestamp": 3568595302.1292267,
  "ref_id": "44.78",
  "platform_id": "TODO_some_platform_id_of_type_UPS",
  "message": "low battery (synthetic event generated from simulator)"
}
2013-01-30 20:28:22,134 DEBUG    Thread-1 oms_simulator  :133 _notify_listener RESPONSE: 200, OK, MY-RESPONSE. BYE
2013-01-30 20:28:29,335 DEBUG    Thread-1 oms_simulator  :184 generate_and_notify_event notifying event_instance={'platform_id': 'TODO_some_platform_id_of_type_UPS', 'timestamp': 3568595309.335513, 'message': 'on battery (synthetic event generated from simulator)', 'group': 'power', 'ref_id': '44.77'}
2013-01-30 20:28:29,335 DEBUG    Thread-1 oms_simulator  :109 _notify_listener Notifying event_instance={'platform_id': 'TODO_some_platform_id_of_type_UPS', 'timestamp': 3568595309.335513, 'message': 'on battery (synthetic event generated from simulator)', 'group': 'power', 'ref_id': '44.77'} to listener=http://localhost:8000
2013-01-30 20:28:29,335 DEBUG    Thread-1 oms_simulator  :116 _notify_listener payload=
{
  "group": "power",
  "url": "http://localhost:8000",
  "timestamp": 3568595309.335513,
  "ref_id": "44.77",
  "platform_id": "TODO_some_platform_id_of_type_UPS",
  "message": "on battery (synthetic event generated from simulator)"
}
2013-01-30 20:28:29,339 DEBUG    Thread-1 oms_simulator  :133 _notify_listener RESPONSE: 200, OK, MY-RESPONSE. BYE
2013-01-30 20:28:34,128 DEBUG    MainThread oms_simulator  :202 stop stopping event generation...
2013-01-30 20:28:34,140 DEBUG    Thread-1 oms_simulator  :199 _run event generation stopped.



TERMINAL 1:
CONTENT_LENGTH=242 CONTENT_TYPE=application/json HTTP_ACCEPT=text/plain
body=
{
  "group": "power",
  "url": "http://localhost:8000",
  "timestamp": 3568595302.1292267,
  "ref_id": "44.78",
  "platform_id": "TODO_some_platform_id_of_type_UPS",
  "message": "low battery (synthetic event generated from simulator)"
}
event_instance={'platform_id': 'TODO_some_platform_id_of_type_UPS', 'group': 'power', 'url': 'http://localhost:8000', 'timestamp': 3568595302.1292267, 'message': 'low battery (synthetic event generated from simulator)', 'ref_id': '44.78'}
127.0.0.1 - - [2013-01-30 20:28:22] "POST / HTTP/1.1" 200 118 0.002464
CONTENT_LENGTH=240 CONTENT_TYPE=application/json HTTP_ACCEPT=text/plain
body=
{
  "group": "power",
  "url": "http://localhost:8000",
  "timestamp": 3568595309.335513,
  "ref_id": "44.77",
  "platform_id": "TODO_some_platform_id_of_type_UPS",
  "message": "on battery (synthetic event generated from simulator)"
}
event_instance={'platform_id': 'TODO_some_platform_id_of_type_UPS', 'group': 'power', 'url': 'http://localhost:8000', 'timestamp': 3568595309.335513, 'message': 'on battery (synthetic event generated from simulator)', 'ref_id': '44.77'}
127.0.0.1 - - [2013-01-30 20:28:29] "POST / HTTP/1.1" 200 118 0.002172
"""
