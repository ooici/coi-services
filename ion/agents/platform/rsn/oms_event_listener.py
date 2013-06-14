#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.oms_event_listener
@file    ion/agents/platform/rsn/oms_event_listener.py
@author  Carlos Rueda
@brief   HTTP server to get and notify CI about RSN OMS event notifications
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.platform.platform_driver_event import ExternalEventDriverEvent

from gevent.pywsgi import WSGIServer
import socket
import sys
import yaml
import os


class OmsEventListener(object):
    """
    HTTP server to get RSN OMS event notifications and do corresponding
    notifications to driver/agent via callback.
    """
    #
    # TODO The whole asynchronous reception of external RSN OMS events needs
    # to be handled in a different way, in particular, it should be a separate
    # service (not a supporting class per platform driver instance) that
    # listens on a well-known host:port and that publishes the corresponding
    # CI events directly.
    #

    def __init__(self, notify_driver_event):
        """
        Creates a listener.

        @param notify_driver_event callback to notify event events. Must be
                                    provided.
        """

        assert notify_driver_event, "notify_driver_event callback must be provided"
        self._notify_driver_event = notify_driver_event

        self._http_server = None
        self._url = None

        # _notifications: if not None, [event_instance, ...]
        self._notifications = None

        # __no_notifications: flag only intended for developing purposes
        self.__no_notifications = os.getenv("NO_OMS_NOTIFICATIONS") is not None
        if self.__no_notifications:  # pragma: no cover
            log.warn("NO_OMS_NOTIFICATIONS env variable defined: no notifications will be done")

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

        log.info("starting http server for receiving event notifications at"
                 " %s:%s ...", host, port)
        try:
            self._http_server = WSGIServer((host, port), self.__application,
                                           log=sys.stdout)
            self._http_server.start()
        except:
            log.exception("Could not start http server for receiving event notifications")
            raise

        host_name, host_port = self._http_server.address

        log.info("http server started at %s:%s" % (host_name, host_port))

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
        log.info("http server exposed URL = %r", self._url)

    def __application(self, environ, start_response):

        input = environ['wsgi.input']
        body = "\n".join(input.readlines())
#        log.trace('notification received payload=%s', body)
        event_instance = yaml.load(body)
        log.trace('notification received event_instance=%s', event_instance)

        self._event_received(event_instance)

        # generic OK response  TODO determine appropriate variations if any
        status = '200 OK'
        headers = [('Content-Type', 'text/plain')]
        start_response(status, headers)
        return status

    def _event_received(self, event_instance):
        log.trace('received event_instance=%s', event_instance)

        if self._notifications:
            self._notifications.append(event_instance)
        else:
            self._notifications = [event_instance]

        if self.__no_notifications:  # pragma: no cover
            return

        log.debug('notifying event_instance=%s', event_instance)

        driver_event = ExternalEventDriverEvent(event_instance)
        self._notify_driver_event(driver_event)

    def stop_http_server(self):
        """
        Stops the http server.
        @retval the dict of received notifications or None if they are not kept.
        """
        if self._http_server:
            log.info("HTTP SERVER: stopping http server: url=%r", self._url)
            self._http_server.stop()

        self._http_server = None
        self._url = None

        return self._notifications
