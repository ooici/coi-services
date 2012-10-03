#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.oms_alarms
@file    ion/agents/platform/oms/simulator/oms_alarms.py
@author  Carlos Rueda
@brief   OMS simulator alarm definitions and supporting functions.
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


class AlarmInfo(object):
    ALARM_TYPES = {

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


class AlarmNotifier(object):

    def __init__(self):

        # { alarm_type: {url: reg_time, ...}, ... }
        # initialize with empty dict for each alarm type:
        self._listeners = dict((at, {}) for at in AlarmInfo.ALARM_TYPES)

    def add_listener(self, url, alarm_type):
        assert alarm_type in AlarmInfo.ALARM_TYPES

        url_dict = self._listeners[alarm_type]

        if not url in url_dict:
            url_dict[url] = time.time()
            log.info("added listener=%s for alarm_type=%s", url, alarm_type)

        return url_dict[url]

    def remove_listener(self, url, alarm_type):
        assert alarm_type in AlarmInfo.ALARM_TYPES

        url_dict = self._listeners[alarm_type]

        unreg_time = 0
        if url in url_dict:
            unreg_time = time.time()
            del url_dict[url]
            log.info("removed listener=%s for alarm_type=%s", url, alarm_type)

        return unreg_time

    def notify(self, alarm_instance):
        """
        Notifies the alarm to all associated listeners.
        """
        assert isinstance(alarm_instance, dict)
        assert 'ref_id' in alarm_instance
        alarm_type = alarm_instance['ref_id']
        assert alarm_type in AlarmInfo.ALARM_TYPES

        urls = self._listeners[alarm_type]
        if not len(urls):
            log.trace("No alarm listeners for alarm_type=%s", alarm_type)
            return

        # copy list to get a snapshot of the current dictionary and thus avoid
        # concurrent modification kind of runtime errors like:
        # RuntimeError: dictionary changed size during iteration
        urls = list(urls)
        for url in urls:
            self._notify_listener(url, alarm_instance)

    def _notify_listener(self, url, alarm_instance):
        """
        Notifies alarm to given listener.
        """
        log.debug("Notifying alarm_instance=%s to listener=%s", str(alarm_instance), url)

        # remove "http://" prefix
        if url.lower().startswith("http://"):
            url = url[len("http://"):]

        # include url in alarm instance:
        alarm_instance['url'] = url

        conn = httplib.HTTPConnection(url)
        try:
            payload = yaml.dump(alarm_instance, default_flow_style=False)
            log.trace("payload=\n\t%s", payload.replace('\n', '\n\t'))
            headers = {"Content-type": "text/plain",
                       "Accept": "text/plain"}
            conn.request("POST", "", body=payload, headers=headers)
            response = conn.getresponse()
            data = response.read()
            log.trace("RESPONSE: %s, %s, %s", response.status, response.reason, data)
        finally:
            conn.close()


class AlarmGenerator(Greenlet):
    """
    Simple helper to generate and trigger alarm notifications.
    """

    def __init__(self, notifier):
        Greenlet.__init__(self)
        self._notifier = notifier
        self._keep_running = True
        self._index = 0  # in AlarmInfo.ALARM_TYPES

    def generate_alarm(self):
        """
        Generates a synthetic alarm.
        """
        if self._index >= len(AlarmInfo.ALARM_TYPES):
            self._index = 0

        alarm_type = AlarmInfo.ALARM_TYPES.values()[self._index]
        self._index += 1

        #
        # TODO create a more complete alarm_instance
        #
        ref_id = alarm_type['ref_id']
        platform_id = "some platform_id of type %r (TODO)" % alarm_type['platform_type']
        message = "%s (synthetic alarm)" % alarm_type['name']
        group = "%s (synthetic alarm)" % alarm_type['group']
        timestamp = time.time()
        alarm_instance = {
            'ref_id':       ref_id,
            'message':      message,
            'platform_id':  platform_id,
            'timestamp':    timestamp,
            'group':        group,
        }
        log.debug("notifying alarm_instance=%s", str(alarm_instance))
        self._notifier.notify(alarm_instance)

    def _run(self):
        while self._keep_running:
            self.generate_alarm()
            sleep(1)

    def stop(self):
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
            alarm_instance = yaml.load(body)
            print('listener got alarm_instance=%s' % str(alarm_instance))
            headers = [('Content-Type', 'text/plain') ]
            status = '200 OK'
            start_response(status, headers)
            return "MY-RESPONSE. BYE"
        print("%s:%s: listening for alarm notifications..." % (host, port))
        WSGIServer((host, port), application).serve_forever()

    elif len(sys.argv) > 1 and sys.argv[1] == "notifier":
        # run notifier
        notifier = AlarmNotifier()
        url = "http://%s:%s" % (host, port)
        alarm_type = AlarmInfo.ALARM_TYPES.keys()[0]
        notifier.add_listener(url, alarm_type)
        print("registered listener to alarm_type=%r" % alarm_type)
        generator = AlarmGenerator(notifier)
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
$ bin/python  ion/agents/platform/oms/simulator/oms_alarms.py listener
localhost:8000: listening for alarm notifications...

TERMINAL 2:
$ bin/python  ion/agents/platform/oms/simulator/oms_alarms.py notifier
registered listener to alarm_type='44.78'
generating events for 6 seconds ...
generated alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794478.977933, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.78'}
generated alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794479.983365, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.77'}
generated alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794480.983749, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.78'}
generated alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794481.989795, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.77'}
generated alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794482.990179, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.78'}
generated alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794483.996283, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.77'}

TERMINAL 1:
listener got alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794478.977933, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.78'}
127.0.0.1 - - [2012-09-27 18:07:58] "POST / HTTP/1.1" 200 118 0.002234
listener got alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794480.983749, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.78'}
127.0.0.1 - - [2012-09-27 18:08:00] "POST / HTTP/1.1" 200 118 0.002637
listener got alarm_instance={'platform_id': 'some platform_id (todo)', 'timestamp': 1348794482.990179, 'message': 'synthetic alarm', 'group': 'power', 'ref_id': '44.78'}
127.0.0.1 - - [2012-09-27 18:08:02] "POST / HTTP/1.1" 200 118 0.002571
"""
