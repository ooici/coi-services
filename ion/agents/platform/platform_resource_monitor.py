#!/usr/bin/env python

"""
@package ion.agents.platform.platform_resource_monitor
@file    ion/agents/platform/platform_resource_monitor.py
@author  Carlos Rueda
@brief   Platform resource monitoring
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.resource_monitor import ResourceMonitor


class PlatformResourceMonitor(object):
    """
    Resource monitoring for a given platform.
    """

    def __init__(self, platform_id, attr_info, get_attribute_values, notify_driver_event):
        """
        @param platform_id Platform ID
        @param attr_info Attribute information
        @param get_attribute_values Function to retrieve attribute
                 values for the specific platform, called like this:
                 get_attribute_values([attr_id], from_time)
                 for each attr_id in the platform.
        @param notify_driver_event Callback to notify whenever a value is
                retrieved.
        """

        self._platform_id = platform_id
        self._attr_info = attr_info
        self._get_attribute_values = get_attribute_values
        self._notify_driver_event = notify_driver_event

        if log.isEnabledFor(logging.DEBUG):
            log.debug("CIDEVSA-450 %r: PlatformResourceMonitor instance created", self._platform_id)

        # _monitors: dict { attr_id: ResourceMonitor }
        self._monitors = {}

    def start_resource_monitoring(self):
        """
        Starts greenlets to periodically retrieve values of the attributes
        associated with my platform, and do corresponding event notifications.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("CIDEVSA-450 %r: starting resource monitoring: attr_info=%s",
                  self._platform_id, str(self._attr_info))

        #
        # TODO attribute grouping so one single greenlet is launched for a
        # group of attributes having same or similar monitoring rate. For
        # simplicity at the moment, start a greenlet per attribute.
        #

        for attr_id, attr_defn in self._attr_info.iteritems():
            if log.isEnabledFor(logging.DEBUG):
                log.debug("CIDEVSA-450 %r: dispatching resource monitoring for attr_id=%r attr_defn=%s",
                      self._platform_id, attr_id, attr_defn)

            if 'monitorCycleSeconds' in attr_defn:
                self._start_monitor_greenlet(attr_id, attr_defn)
            else:
                log.warn(
                    "CIDEVSA-450 %r: unexpected: attribute info does not contain %r "
                    "for attribute %r. attr_defn = %s",
                        self._platform_id,
                        'monitorCycleSeconds', attr_id, str(attr_defn))

    def _start_monitor_greenlet(self, attr_id, attr_defn):
        """
        Creates and starts a ResourceMonitor
        """
        assert 'monitorCycleSeconds' in attr_defn
        if log.isEnabledFor(logging.DEBUG):
            log.debug("CIDEVSA-450 %r: _start_monitor_greenlet attr_id=%r attr_defn=%s",
                  self._platform_id, attr_id, attr_defn)

        resmon = ResourceMonitor(self._platform_id,
                                 attr_id, attr_defn,
                                 self._get_attribute_values,
                                 self._notify_driver_event)
        self._monitors[attr_id] = resmon
        resmon.start()

    def stop_resource_monitoring(self):
        """
        Stops all the monitoring greenlets.
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("CIDEVSA-450 %r: stopping resource monitoring", self._platform_id)

        for resmon in self._monitors.itervalues():
            resmon.stop()
        self._monitors.clear()

    def destroy(self):
        """
        Simply calls self.stop_resource_monitoring()
        """
        self.stop_resource_monitoring()
