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

        log.debug("%r: PlatformResourceMonitor instance created", self._platform_id)

        # _monitors: dict { rate_millis: ResourceMonitor }
        self._monitors = {}

    def _group_by_monitoring_rate(self, group_size_millis=1000):
        """
        Groups the list of attr defs according to similar monitoring rate.

        @param group_size_millis
                    each group will contain the attributes having a
                    monitoring rate within this interval in milliseconds.
                    By default, 1000).

        @return { rate_millis : [attr_defn, ...], ... },
                    where rate_millis is an int indicating the monitoring
                    rate to be used for the corresponding list of attr defs.
        """
        groups = {}
        for attr_defn in self._attr_info.itervalues():
            if 'monitorCycleSeconds' not in attr_defn:
                log.warn("%r: unexpected: attribute info does not contain %r. "
                         "attr_defn = %s",
                         self._platform_id,
                         'monitorCycleSeconds', attr_defn)
                continue

            monitorCycleSeconds = float(attr_defn['monitorCycleSeconds'])
            monitorCycleMillis = monitorCycleSeconds * 1000
            rate_millis = int(group_size_millis) * int(round(monitorCycleMillis / group_size_millis))
            if rate_millis not in groups:
                groups[rate_millis] = []
            groups[rate_millis].append(attr_defn)

        log.debug("%r: _group_by_monitoring_rate = %s", self._platform_id, groups)
        return groups

    def start_resource_monitoring(self):
        """
        Starts greenlets to periodically retrieve values of the attributes
        associated with my platform, and do corresponding event notifications.
        """

        log.debug("%r: starting resource monitoring: attr_info=%s",
                  self._platform_id, self._attr_info)

        # attributes ar grouped by similar monitoring rate so a single
        # greenlet is used for each group:
        groups = self._group_by_monitoring_rate()
        for rate_millis, attr_defns in groups.iteritems():
            self._start_monitor_greenlet(rate_millis, attr_defns)

    def _start_monitor_greenlet(self, rate_millis, attr_defns):
        """
        Creates and starts a ResourceMonitor
        """
        log.debug("%r: _start_monitor_greenlet rate_millis=%d attr_defns=%s",
                  self._platform_id, rate_millis, attr_defns)

        resmon = ResourceMonitor(self._platform_id,
                                 rate_millis, attr_defns,
                                 self._get_attribute_values,
                                 self._notify_driver_event)
        self._monitors[rate_millis] = resmon
        resmon.start()

    def stop_resource_monitoring(self):
        """
        Stops all the monitoring greenlets.
        """
        log.debug("%r: stopping resource monitoring", self._platform_id)

        for resmon in self._monitors.itervalues():
            resmon.stop()
        self._monitors.clear()

    def destroy(self):
        """
        Simply calls self.stop_resource_monitoring()
        """
        self.stop_resource_monitoring()
