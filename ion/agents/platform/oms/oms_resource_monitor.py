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
        log.debug("%r: starting resource monitoring %s" % (self._platform_id, str(self)))
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

        log.debug("%r: greenlet stopped." % self._platform_id)

    def _retrieve_attribute_value(self):
        """
        Retrieves the attribute value from the OMS.
        """
        log.debug("%r: retrieving attribute %r" % (self._platform_id, self._attr_id))

        attrNames = [self._attr_id]
        from_time = self._last_ts if self._last_ts else 0

        retval = self._oms.getPlatformAttributeValues(self._platform_id, attrNames, from_time)
        log.debug("%r: getPlatformAttributeValues returned %s" % (self._platform_id, retval))

        if not self._platform_id in retval:
            log.warn("%r: unexpected: response does not include data for me." % self._platform_id)
            return

        retrieved_vals = retval[self._platform_id]
        log.debug("%r: retrieved_vals = %s" % (self._platform_id, str(retrieved_vals)))
        if self._attr_id in retrieved_vals:
            value, ts = retrieved_vals[self._attr_id]
            if value and ts:
                self._value_retrieved((value, ts))
            else:
                log.debug("%r: No value reported for attribute=%r from_time=%r" % (
                    self._platform_id, self._attr_id, from_time))
        else:
            log.warn("%r: unexpected: response does not include requested attribute %r" % (
                self._platform_id, self._attr_id))

    def _value_retrieved(self, value_and_ts):
        """
        A value has been retrieved from OMS. Create and notify corresponding
        event to platform agent.
        """
        log.debug("%r: attr=%r: value retrieved = %s" % (
            self._platform_id, self._attr_id, str(value_and_ts)))

        value, ts = value_and_ts
        self._last_ts = ts

        driver_event = AttributeValueDriverEvent(self._platform_id,
                                              self._attr_id, value, ts)
        self._notify_driver_event(driver_event)

    def stop(self):
        log.debug("%r: stopping resource monitoring %s" % (self._platform_id, str(self)))
        self._active = False
