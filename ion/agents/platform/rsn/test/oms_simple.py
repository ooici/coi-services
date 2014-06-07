#!/usr/bin/env python

# NOTE(2014-05-23):  This program has not been run now for several months.
# Need to coordinate with RSN before attempting to run it again.

"""
@package ion.agents.platform.rsn.test.oms_simple
@file    ion/agents/platform/rsn/test/oms_simple.py
@author  Carlos Rueda
@brief   Program that connects to the real RSN OMS endpoint to do basic
         verification of the operations. Note that VPN is required.
         Also, port 5000 on the localhost (via corresponding fully-qualified
         domain name as returned by socket.getfqdn()) needs to be accessible
         from OMS for the event notification to be received here.

         For usage, call:
           bin/python ion/agents/platform/rsn/test/oms_simple.py --help

@see     https://confluence.oceanobservatories.org/display/CIDev/RSN+OMS+endpoint+implementation+verification
@see     https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
from gevent.pywsgi import WSGIServer
from ion.agents.platform.responses import InvalidResponse
from pyon.util.breakpoint import breakpoint

import xmlrpclib
import sys
import pprint
import socket
import yaml
import os


#
# OmsEventListener: Almost exact copy from a previous version of
# ion/agents/platform/rsn/oms_event_listener.py which was later on changed
# to listen to the relayed OMSDeviceStatusEvent's by the service gateway.
# Some comments in the code may not be relevant anymore.
#
class OmsEventListener(object):
    """
    HTTP server to get RSN OMS event notifications and do corresponding
    notifications to driver/agent via callback.
    """

    def __init__(self, platform_id, notify_driver_event):
        """
        Creates a listener.

        @param notify_driver_event callback to notify event events. Must be
                                    provided.
        """

        self._platform_id = platform_id
        self._notify_driver_event = notify_driver_event

        self._http_server = None
        self._url = None

        # _notifications: if not None, [event_instance, ...]
        self._notifications = None

        # _no_notifications: flag only intended for developing purposes
        # see ion.agents.platform.rsn.simulator.oms_events
        self._no_notifications = os.getenv("NO_OMS_NOTIFICATIONS") is not None
        if self._no_notifications:  # pragma: no cover
            log.warn("%r: NO_OMS_NOTIFICATIONS env variable defined: "
                     "no notifications will be done", self._platform_id)
            self._url = "http://NO_OMS_NOTIFICATIONS"

    @property
    def url(self):
        """
        The URL that can be used to register a listener to the OMS.
        This is None if there is no HTTP server currently running.
        """
        return self._url

    def keep_notifications(self, keep=True, reset=True):
        """
        By default, received event notifications are not kept. Call this with
        True (the default) to keep them, or with False to not keep them.
        If they are currently kept and the reset param is True (the default),
        then the notifications list is reinitialized.
        """
        if keep:
            if not self._notifications or reset:
                self._notifications = []
        else:
            self._notifications = None

    @property
    def notifications(self):
        """
        The current list of received notifications. This will be None if such
        notifications are not being kept.
        """
        return self._notifications

    def start_http_server(self, host='localhost', port=0):
        """
        Starts a HTTP server that handles the notification of received events.

        @param host Host, by default 'localhost'.
        @param port Port, by default 0 to get one dynamically.
        """

        # reinitialize notifications if we are keeping them:
        if self._notifications:
            self._notifications = []

        if self._no_notifications:
            return

        log.info("%r: starting http server for receiving event notifications at"
                 " %s:%s ...", self._platform_id, host, port)
        try:
            self._http_server = WSGIServer((host, port), self._application,
                                           log=sys.stdout)
            self._http_server.start()
        except Exception:
            log.exception("%r: Could not start http server for receiving event"
                          " notifications", self._platform_id)
            raise

        host_name, host_port = self._http_server.address

        log.info("%r: http server started at %s:%s", self._platform_id, host_name, host_port)

        exposed_host_name = host_name

        ######################################################################
        # adjust exposed_host_name:
        # **NOTE**: the adjustment below is commented out because is not robust
        # enough. For example, the use of the external name for the host would
        # require the particular port to be open to the world.
        # And in any case, this overall handling needs a different approach.
        #
        # # If the given host is 'localhost', need to get the actual hostname
        # # for the exposed URL:
        # if host is 'localhost':
        #     exposed_host_name = socket.gethostname()
        ######################################################################

        self._url = "http://%s:%s" % (exposed_host_name, host_port)
        log.info("%r: http server exposed URL = %r", self._platform_id, self._url)

    def _application(self, environ, start_response):

        input = environ['wsgi.input']
        body = "\n".join(input.readlines())
        # log.trace('%r: notification received payload=%s', self._platform_id, body)
        event_instance = yaml.load(body)

        self._event_received(event_instance)

        # generic OK response  TODO determine appropriate variations if any
        status = '200 OK'
        headers = [('Content-Type', 'text/plain')]
        start_response(status, headers)
        return status

    def _event_received(self, event_instance):
        log.trace('%r: received event_instance=%s', self._platform_id, event_instance)

        if self._notifications:
            self._notifications.append(event_instance)
        else:
            self._notifications = [event_instance]

        log.debug('%r: notifying event_instance=%s', self._platform_id, event_instance)

        # driver_event = ExternalEventDriverEvent(event_instance)
        # self._notify_driver_event(driver_event)
        self._notify_driver_event(event_instance)

    def stop_http_server(self):
        """
        Stops the http server.
        @retval the dict of received notifications or None if they are not kept.
        """
        if self._http_server:
            log.info("%r: HTTP SERVER: stopping http server: url=%r",
                     self._platform_id, self._url)
            self._http_server.stop()

        self._http_server = None
        self._url = None

        return self._notifications


DEFAULT_RSN_OMS_URI = "http://alice:1234@10.180.80.10:9021/"
DEFAULT_MAX_WAIT    = 70

INVALID_PLATFORM_ID = InvalidResponse.PLATFORM_ID

# use full-qualified domain name as the external host for the registration
HTTP_SERVER_HOST = socket.getfqdn()
HTTP_SERVER_PORT = 5000

EVENT_LISTENER_URL = "http://%s:%d/oms" % (HTTP_SERVER_HOST, HTTP_SERVER_PORT)

# max time to wait to receive the test event
max_wait = 0

# launch IPython shell?
launch_breakpoint = False

tried = {}

INCLUDE_WRITE_OPERS = False


def launch_listener():  # pragma: no cover
    def notify_driver_event(evt):
        print("notify_driver_event received: %s" % str(evt.event_instance))

    print 'launching listener, port=%d ...' % HTTP_SERVER_PORT
    oms_event_listener = OmsEventListener("dummy_plat_id", notify_driver_event)
    oms_event_listener.keep_notifications()
    oms_event_listener.start_http_server(host='', port=HTTP_SERVER_PORT)
    print 'listener launched'
    return oms_event_listener


def main(uri):  # pragma: no cover
    oms_event_listener = launch_listener()

    print '\nconnecting to %r ...' % uri
    proxy = xmlrpclib.ServerProxy(uri, allow_none=True)
    print 'connection established.'

    pp = pprint.PrettyPrinter()

    def show_listeners():
        from datetime import datetime
        from ion.agents.platform.util import ntp_2_ion_ts

        event_listeners = proxy.event.get_registered_event_listeners()
        print("Event listeners (%d):" % len(event_listeners))
        for a, b in sorted(event_listeners.iteritems(),
                           lambda a, b: int(a[1] - b[1])):
            time = datetime.fromtimestamp(float(ntp_2_ion_ts(b)) / 1000)
            print("   %s  %s" % (time, a))
        print

    def format_val(value):
        prefix = "\t\t"
        print "\n%s%s" % (prefix, pp.pformat(value).replace("\n", "\n" + prefix))

    def format_err(msg):
        prefix = "\t\t"
        print "\n%s%s" % (prefix, msg.replace("\n", "\n" + prefix))

    def get_method(handler_name, method_name):
        """
        Gets the method from the proxy.
        @param handler_name  Name of the handler; can be None to indicate get
                             method directly from proxy.
        @param method_name   Method's name

        @return              callable; None if any error getting the method
        """

        # get method:
        if handler_name:
            # get handler:
            try:
                handler = getattr(proxy, handler_name)
            except Exception as e:
                print "error getting handler %s: %s: %s" % (handler_name, type(e), str(e))
                return None
            try:
                method = getattr(handler, method_name)
                return method
            except Exception as e:
                print "error method %s.%s: %s: %s" % (handler_name, method_name, type(e), str(e))
                return None
        else:
            try:
                method = getattr(proxy, method_name)
                return method
            except Exception as e:
                print "error getting proxy's method %s: %s: %s" % (method_name, type(e), str(e))
                return None

    def run(full_method_name, *args):
        """
        Runs a method against the proxy.

        @param full_method_name
        @param args
        """
        global tried

        tried[full_method_name] = ""

        handler_name, method_name = full_method_name.split(".")

        # get the method
        method = get_method(handler_name, method_name)
        if method is None:
            tried[full_method_name] = "could not get handler or method"
            return

        sargs = ", ".join(["%r" % a for a in args])

        sys.stdout.write("\n%s(%s) -> " % (full_method_name, sargs))
        sys.stdout.flush()

        # run method
        retval, reterr = None, None
        try:
            retval = method(*args)
            tried[full_method_name] = "OK"
            # print "%r" % retval
            format_val(retval)
        except xmlrpclib.Fault as e:
            if e.faultCode == 8001:
                reterr = "-- NOT FOUND (fault %s)" % e.faultCode
            else:
                reterr = "-- Fault %d: %s" % (e.faultCode, e.faultString)
                # raise
                # print "Exception: %s: %s" % (type(e), str(e))
                # tried[full_method_name] = str(e)

            tried[full_method_name] = reterr
            format_err(reterr)

        return retval, reterr

    def verify_entry_in_dict(retval, reterr, entry):
        if reterr is not None:
            return retval, reterr

        if not isinstance(retval, dict):
            reterr = "-- expecting a dict with entry %r" % entry
        elif entry not in retval:
            reterr = "-- expecting a dict with entry %r" % entry
        else:
            retval = retval[entry]

        print("full_method_name = %s" % full_method_name)
        if reterr:
            tried[full_method_name] = reterr
            format_err(reterr)

        return retval, reterr

    def verify_test_event_notified(retval, reterr, event):
        print("waiting for a max of %d secs for test event to be notified..." % max_wait)
        import time

        wait_until = time.time() + max_wait
        got_it = False
        while not got_it and time.time() <= wait_until:
            time.sleep(1)
            for evt in oms_event_listener.notifications:
                if event['message'] == evt['message']:
                    got_it = True
                    break

        # print("Received external events: %s" % oms_event_listener.notifications)
        if not got_it:
            reterr = "error: didn't get expected test event notification within %d " \
                     "secs. (Got %d event notifications.)" % (
                     max_wait, len(oms_event_listener.notifications))

        print("full_method_name = %s" % full_method_name)
        if reterr:
            tried[full_method_name] = reterr
            format_err(reterr)

        return retval, reterr

    show_listeners()

    if launch_breakpoint:
        breakpoint(locals())

    print "Basic verification of the operations:\n"

    #----------------------------------------------------------------------
    full_method_name = "hello.ping"
    retval, reterr = run(full_method_name)
    if retval and retval.lower() != "pong":
        error = "expecting 'pong'"
        tried[full_method_name] = error
        format_err(error)

    platform_id = "dummy_platform_id"

    #----------------------------------------------------------------------
    full_method_name = "config.get_platform_metadata"
    retval, reterr = run(full_method_name, platform_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)

    #----------------------------------------------------------------------
    full_method_name = "attr.get_platform_attribute_values"
    retval, reterr = run(full_method_name, platform_id, [])
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)

    port_id = "dummy_port_id"

    #----------------------------------------------------------------------
    full_method_name = "port.get_platform_ports"
    retval, reterr = run(full_method_name, platform_id)
    retval, reterr = verify_entry_in_dict(retval, reterr, platform_id)
    if retval is not None:
        if isinstance(retval, dict):
            if len(retval):
                port_id = retval.keys()[0]
            else:
                reterr = "empty dict of ports for platform %r" % platform_id
        else:
            reterr = "expecting a dict {%r: ...}. got: %s" % (platform_id, type(retval))
        if reterr:
            tried[full_method_name] = reterr
            format_err(reterr)

    instrument_id = "dummy_instrument_id"

    if reterr is None:
        full_method_name = "port.get_platform_ports"
        retval, reterr = run(full_method_name, "dummy_platform_id")
        orig_retval = retval
        retval, reterr = verify_entry_in_dict(retval, reterr, "dummy_platform_id")
        if retval != INVALID_PLATFORM_ID:
            reterr = "expecting dict {%r: %r}. got: %r" % (
                "dummy_platform_id", INVALID_PLATFORM_ID, orig_retval)
            tried[full_method_name] = reterr
            format_err(reterr)

    instrument_id = "dummy_instrument_id"

    src = "oms_simple"

    if INCLUDE_WRITE_OPERS:
        #----------------------------------------------------------------------
        full_method_name = "port.turn_on_platform_port"
        retval, reterr = run(full_method_name, platform_id, port_id, src)

        #----------------------------------------------------------------------
        full_method_name = "port.turn_off_platform_port"
        retval, reterr = run(full_method_name, platform_id, port_id, src)

        #----------------------------------------------------------------------
        full_method_name = "port.set_over_current"
        ma, us = 0, 0
        retval, reterr = run(full_method_name, platform_id, port_id, ma, us, src)

    #----------------------------------------------------------------------
    url = EVENT_LISTENER_URL

    #----------------------------------------------------------------------
    full_method_name = "event.register_event_listener"
    retval, reterr = run(full_method_name, url)
    retval, reterr = verify_entry_in_dict(retval, reterr, url)

    #----------------------------------------------------------------------
    full_method_name = "event.get_registered_event_listeners"
    retval, reterr = run(full_method_name)
    urls = retval
    retval, reterr = verify_entry_in_dict(retval, reterr, url)

    #----------------------------------------------------------------------
    full_method_name = "event.unregister_event_listener"
    if isinstance(urls, dict):
        # this part just as a convenience to unregister listeners that were
        # left registered by some error in a prior interaction.
        prefix = "http://127.0.0.1:"  # or some other needed prefix
        for url2 in urls:
            if url2.find(prefix) >= 0:
                retval, reterr = run(full_method_name, url2)
                retval, reterr = verify_entry_in_dict(retval, reterr, url2)
                if reterr is not None:
                    break
    if reterr is None:
        retval, reterr = run(full_method_name, url)
        retval, reterr = verify_entry_in_dict(retval, reterr, url)

    # the following to specifically verify reception of test event
    if max_wait:
        full_method_name = "event.register_event_listener"
        retval, reterr = run(full_method_name, EVENT_LISTENER_URL)
        retval, reterr = verify_entry_in_dict(retval, reterr, EVENT_LISTENER_URL)

    full_method_name = "event.generate_test_event"
    event = {
        'message'      : "fake event triggered from CI using OMS' generate_test_event",
        'platform_id'  : "fake_platform_id",
        'severity'     : "3",
        'group '       : "power",
    }
    retval, reterr = run(full_method_name, event)

    if max_wait:
        verify_test_event_notified(retval, reterr, event)

        full_method_name = "event.unregister_event_listener"
        retval, reterr = run(full_method_name, EVENT_LISTENER_URL)
        retval, reterr = verify_entry_in_dict(retval, reterr, EVENT_LISTENER_URL)
    elif not reterr:
        ok_but = "OK (but verification of event reception was not performed)"
        tried[full_method_name] = ok_but
        format_err(ok_but)

    show_listeners()

    #######################################################################
    print("\nSummary of basic verification:")
    print("\n(INCLUDE_WRITE_OPERS=%s)" % INCLUDE_WRITE_OPERS)
    okeys = 0
    for full_method_name, result in sorted(tried.iteritems()):
        print("%20s %-40s: %s" % ("", full_method_name, result))
        if result.startswith("OK"):
            okeys += 1
    print("OK methods %d out of %s" % (okeys, len(tried)))


if __name__ == "__main__":  # pragma: no cover

    import argparse

    parser = argparse.ArgumentParser(description="Basic CI-OMS verification program")
    parser.add_argument("-u", "--uri",
                        help="RSN OMS URI (default: %s)" % DEFAULT_RSN_OMS_URI,
                        default=DEFAULT_RSN_OMS_URI)
    parser.add_argument("-w", "--wait",
                        help="Max wait time for test event (default: %d)" % DEFAULT_MAX_WAIT,
                        default=DEFAULT_MAX_WAIT)
    parser.add_argument("-b", "--breakpoint",
                        help="Launch IPython shell at beginning",
                        action='store_const', const=True)

    opts = parser.parse_args()

    uri = opts.uri
    max_wait = int(opts.wait)
    launch_breakpoint = bool(opts.breakpoint)

    main(uri)
