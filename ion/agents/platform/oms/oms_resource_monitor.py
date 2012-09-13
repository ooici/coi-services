#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_resource_monitor
@file    ion/agents/platform/oms/oms_resource_monitor.py
@author  Carlos Rueda
@brief   Platform resource monitoring
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.platform_driver import AttributeValueDriverEvent

from gevent import Greenlet, sleep


class OmsResourceMonitor(object):
    """
    """

    def __init__(self, oms, platform_id, attr_defn, notify_driver_event):
        """
        @param oms
        @param platform_id
        @param attr_defn
        @param notify_driver_event
        """

        assert platform_id, "must give a valid platform ID"
        assert 'attr_id' in attr_defn, "must include monitorCycleSeconds"
        assert 'monitorCycleSeconds' in attr_defn, "must include monitorCycleSeconds"

        self._oms = oms
        self._platform_id = platform_id
        self._attr_defn = attr_defn
        self._notify_driver_event = notify_driver_event

        self._attr_id = attr_defn['attr_id']
        self._monitorCycleSeconds = attr_defn['monitorCycleSeconds']

        # timestamp of last retrieved attribute value
        self._last_ts = None

        self._active = False

    def __str__(self):
        return "%s{platform_id=%r; attr_defn=%r}" % (self.__class__.__name__,
            self._platform_id, self._attr_defn)

    def start(self):
        """
        Starts greenlet for resource monitoring.
        """
        log.info("starting resource monitoring %s" % str(self))
        self._active = True
        runnable = Greenlet(self._run)
        runnable.start()

    def _run(self):
        """
        The target for the greenlet.
        """
        while self._active:
            sleep(self._monitorCycleSeconds)
            if self._active:
                self._retrieve_attribute_value()

        log.info("greenlet stopped.")

    def _retrieve_attribute_value(self):
        """
        Retrieves the attribute value from the OMS.
        """
        log.info("%r: retrieving attribute %r" % (self._platform_id, self._attr_id))

        attrNames = [self._attr_id]
        from_time = self._last_ts if self._last_ts else 0

        retval = self._oms.getPlatformAttributeValues(self._platform_id, attrNames, from_time)
        log.info("getPlatformAttributeValues for %r = %s" % (self._platform_id, retval))

        if not self._platform_id in retval:
            raise PlatformException("Unexpected: response does not include "
                    "requested platform '%s'" % self._platform_id)

        attrs = retval[self._platform_id]
        log.info("attrs = %s" % str(attrs))
        if self._attr_id in attrs:
            value_and_ts = attrs[self._attr_id]
            self._value_retrieved(value_and_ts)
        else:
            log.info("No value reported for attribute=%r in platform=%r"
                     " with from_time=%r" % (
                        self._attr_id, self._platform_id, from_time))

    def _value_retrieved(self, value_and_ts):
        """
        A value has been retrieved from OMS. Create and notify corresponding
        event to platform agent.
        """
        log.info("platform=%r; attr=%r: value retrieved = %s" % (
            self._platform_id, self._attr_id, str(value_and_ts)))

        value, ts = value_and_ts
        self._last_ts = ts

        driver_event = AttributeValueDriverEvent(self._platform_id,
                                              self._attr_id, value, ts)
        self._notify_driver_event(driver_event)

    def stop(self):
        log.info("stopping resource monitoring %s" % str(self))
        self._active = False
