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

from ion.agents.platform.platform_driver import AttributeValueDriverEvent

import logging
from gevent import Greenlet, sleep


# A small increment to the latest received timestamp for purposes of the next
# request so we don't get that last sample repeated:
_DELTA_TIME = 0.0001


class OmsResourceMonitor(object):
    """
    Monitor for a specific attribute in a given platform.
    """

    def __init__(self, oms, platform_id, attr_id, attr_defn, notify_driver_event):
        """
        Creates a monitor for a specific attribute in a given platform.
        Call start to start the monitoring greenlet.

        @param oms The CI-OMS object
        @param platform_id Platform ID
        @param attr_id Attribute name
        @param attr_defn Corresp. attribute definition
        @param notify_driver_event Callback to notify whenever a value is
                retrieved.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: OmsResourceMonitor entered. attr_defn=%s",
                      platform_id, attr_defn)

        assert platform_id, "must give a valid platform ID"
        assert 'monitorCycleSeconds' in attr_defn, "must include monitorCycleSeconds"

        self._oms = oms
        self._platform_id = platform_id
        self._attr_defn = attr_defn
        self._notify_driver_event = notify_driver_event

        self._attr_id = attr_id
        self._monitorCycleSeconds = attr_defn['monitorCycleSeconds']

        # timestamp of last retrieved attribute value
        self._last_ts = None

        self._active = False

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: OmsResourceMonitor created. attr_defn=%s",
                      self._platform_id, attr_defn)

    def __str__(self):
        return "%s{platform_id=%r; attr_id=%r; attr_defn=%r}" % (
            self.__class__.__name__,
            self._platform_id, self._attr_id, self._attr_defn)

    def start(self):
        """
        Starts greenlet for resource monitoring.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: starting resource monitoring %s", self._platform_id, str(self))
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

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: attr_id=%r: greenlet stopped.", self._platform_id, self._attr_id)

    def _retrieve_attribute_value(self):
        """
        Retrieves the attribute value from the OMS.
        """
        attrNames = [self._attr_id]
        from_time = (self._last_ts + _DELTA_TIME) if self._last_ts else 0.0

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: retrieving attribute %r from_time %f",
                      self._platform_id, self._attr_id, from_time)

        retval = self._oms.getPlatformAttributeValues(self._platform_id, attrNames, from_time)

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: getPlatformAttributeValues returned %s", self._platform_id, retval)

        if not self._platform_id in retval:
            log.warn("%r: unexpected: response does not include data for me.", self._platform_id)
            return

        retrieved_vals = retval[self._platform_id]

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: retrieved attribute %r values from_time %f = %s",
                      self._platform_id, self._attr_id, from_time, str(retrieved_vals))

        if self._attr_id in retrieved_vals:
            values = retrieved_vals[self._attr_id]
            if values:
                self._values_retrieved(values)

            elif log.isEnabledFor(logging.DEBUG):
                log.debug("%r: No values reported for attribute=%r from_time=%f",
                    self._platform_id, self._attr_id, from_time)
        else:
            log.warn("%r: unexpected: response does not include requested attribute %r",
                self._platform_id, self._attr_id)

    def _values_retrieved(self, values):
        """
        A values response has been received from OMS. Create and notify
        corresponding event to platform agent.
        """
        if log.isEnabledFor(logging.DEBUG):
            ln = len(values)
            # just show a couple of elements
            arrstr = "["
            if ln <= 3:
                vals = [str(e) for e in values[:ln]]
                arrstr += ", ".join(vals)
            else:
                vals = [str(e) for e in values[:2]]
                last_e = values[-1]
                arrstr += ", ".join(vals)
                arrstr += ", ..., " +str(last_e)
            arrstr += "]"
            log.debug("%r: attr=%r: values retrieved(%s) = %s",
                self._platform_id, self._attr_id, ln, arrstr)

        # update _last_ts based on last element in values:
        _, ts = values[-1]
        self._last_ts = ts

        driver_event = AttributeValueDriverEvent(ts, self._platform_id,
                                              self._attr_id, values)
        self._notify_driver_event(driver_event)

    def stop(self):
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%r: stopping resource monitoring %s", self._platform_id, str(self))
        self._active = False
