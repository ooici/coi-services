#!/usr/bin/env python

"""
@package ion.agents.platform.platform_resource_monitor
@file    ion/agents/platform/platform_resource_monitor.py
@author  Carlos Rueda
@brief   Platform resource monitoring handling for all the associated attributes
"""

__author__ = 'Carlos Rueda'



from pyon.public import log
import logging

from ion.agents.platform.resource_monitor import ResourceMonitor
from ion.agents.platform.resource_monitor import _STREAM_NAME
from ion.agents.platform.platform_driver_event import AttributeValueDriverEvent

from gevent import Greenlet, sleep
from gevent.coros import RLock

import pprint


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

        # _monitors: dict { rate_secs: ResourceMonitor }
        self._monitors = {}

        # buffers used by the monitoring greenlets to put retrieved data in
        # and by the publisher greenlet to process that data to construct
        # aggregated AttributeValueDriverEvent objects that the platform
        # agent finally process to create and publish granules.
        self._buffers = {}

        # to synchronize access to the buffers
        self._lock = RLock()

        # publishing rate in seconds, set by _set_publisher_rate
        self._pub_rate = None
        self._publisher_active = False

        # for debugging purposes
        self._pp = pprint.PrettyPrinter()

    def _group_by_monitoring_rate(self, group_size_secs=1):
        """
        Groups the list of attr defs according to similar monitoring rate.

        @param group_size_secs
                    each group will contain the attributes having a
                    monitoring rate within this interval in seconds.
                    By default, 1).

        @return { rate_secs : [attr_defn, ...], ... },
                    where rate_secs is an int indicating the monitoring
                    rate to be used for the corresponding list of attr defs.
        """

        # first, collect attrDefs by individual rate:
        by_rate = {}  # { rate: [attrDef, ...], ... }
        for attr_defn in self._attr_info.itervalues():
            if 'monitor_cycle_seconds' not in attr_defn:
                log.warn("%r: unexpected: attribute info does not contain %r. "
                         "attr_defn = %s",
                         self._platform_id,
                         'monitor_cycle_seconds', attr_defn)
                continue

            rate = float(attr_defn['monitor_cycle_seconds'])
            if not rate in by_rate:
                by_rate[rate] = []
            by_rate[rate].append(attr_defn)

        groups = {}
        if not by_rate:
            # no attributes to monitor, just return the empty grouping:
            return groups

        # merge elements in groups from by_rate having a similar rate
        prev_rates = []
        prev_defns = []
        for rate in sorted(by_rate.iterkeys()):
            attr_defns = by_rate[rate]

            if not prev_rates:
                # first pass in the iteration.
                prev_rates.append(rate)
                prev_defns += attr_defns

            elif abs(rate - min(prev_rates)) < group_size_secs:
                # merge this similar element:
                prev_rates.append(rate)
                prev_defns += attr_defns

            else:
                # group completed: it is indexed by the maximum
                # of the collected previous rates:
                groups[max(prev_rates)] = prev_defns
                # re-init stuff for next group:
                prev_defns = attr_defns
                prev_rates = [rate]

        if prev_rates:
            # last group completed:
            groups[max(prev_rates)] = prev_defns

        if log.isEnabledFor(logging.DEBUG):  # pragma: not cover
            from pprint import PrettyPrinter
            log.debug("%r: _group_by_monitoring_rate = %s",
                      self._platform_id, PrettyPrinter().pformat(groups))
        return groups

    def start_resource_monitoring(self):
        """
        Starts greenlets to periodically retrieve values of the attributes
        associated with my platform, and to generate aggregated events that
        will be used by the platform agent to create and publish
        corresponding granules.
        """

        log.debug("%r: starting resource monitoring: attr_info=%s",
                  self._platform_id, self._attr_info)

        self._init_buffers()

        # attributes are grouped by similar monitoring rate so a single
        # greenlet is used for each group:
        groups = self._group_by_monitoring_rate()
        for rate_secs, attr_defns in groups.iteritems():
            self._start_monitor_greenlet(rate_secs, attr_defns)

        if self._monitors:
            self._start_publisher_greenlet()

    def _start_monitor_greenlet(self, rate_secs, attr_defns):
        """
        Creates and starts a ResourceMonitor
        """
        log.debug("%r: _start_monitor_greenlet rate_secs=%s attr_defns=%s",
                  self._platform_id, rate_secs, attr_defns)

        resmon = ResourceMonitor(self._platform_id,
                                 rate_secs, attr_defns,
                                 self._get_attribute_values,
                                 self._receive_from_monitor)
        self._monitors[rate_secs] = resmon
        resmon.start()

    def stop_resource_monitoring(self):
        """
        Stops the publisher greenlet and all the monitoring greenlets.
        """
        log.debug("%r: stopping resource monitoring", self._platform_id)

        self._stop_publisher_greenlet()

        for resmon in self._monitors.itervalues():
            resmon.stop()
        self._monitors.clear()

        with self._lock:
            self._buffers.clear()

    def destroy(self):
        """
        Simply calls self.stop_resource_monitoring()
        """
        self.stop_resource_monitoring()

    def _init_buffers(self):
        """
        Initializes self._buffers (empty arrays for each attribute)
        """
        self._buffers = {}
        for attr_id, attr_defn in self._attr_info.iteritems():
            if 'monitor_cycle_seconds' not in attr_defn:
                log.warn("%r: unexpected: attribute info does not contain %r. "
                         "attr_defn = %s",
                         self._platform_id,
                         'monitor_cycle_seconds', attr_defn)
                continue

            self._buffers[attr_id] = []

    def _receive_from_monitor(self, driver_event):
        """
        Callback to receive data from the monitoring greenlets and update the
        internal buffer for further processing by the publisher greenlet.

        @param driver_event An AttributeValueDriverEvent
        """
        with self._lock:
            if len(self._buffers) == 0:
                # we are not currently monitoring.
                return

            log.debug('%r: received driver_event from monitor=%s',
                      self._platform_id, driver_event)

            for param_name, param_value in driver_event.vals_dict.iteritems():
                self._buffers[param_name] += param_value

    def _set_publisher_rate(self):
        """
        Gets the rate for the publisher greenlet.
        This is equal to the minimum of the monitoring rates.
        """
        self._pub_rate = min(self._monitors.keys())

    def _start_publisher_greenlet(self):
        if self._publisher_active == True:
            return
        
        self._set_publisher_rate()

        self._publisher_active = True
        runnable = Greenlet(self._run_publisher)
        runnable.start()
        log.debug("%r: publisher greenlet started, dispatch rate=%s",
                  self._platform_id, self._pub_rate)

    def _run_publisher(self):
        """
        The target run function for the publisher greenlet.
        """
        while self._publisher_active:

            # loop to incrementally sleep up to self._pub_rate while promptly
            # reacting to request for termination
            slept = 0
            while self._publisher_active and slept < self._pub_rate:
                # sleep in increments of 0.5 secs
                incr = min(0.5, self._pub_rate - slept)
                sleep(incr)
                slept += incr

            # dispatch publication (if still active):
            with self._lock:
                if self._publisher_active:
                    self._dispatch_publication()

        log.debug("%r: publisher greenlet stopped. _pub_rate=%s",
                  self._platform_id, self._pub_rate)

    def _dispatch_publication(self):
        """
        Inspects the collected data in the buffers to create and notify an
        aggregated AttributeValueDriverEvent.

        Keeps all samples for each attribute, reporting all associated timestamps
        and filling with None values for missing values at particular timestamps,
        but an attribute is included *only* if it has at least an actual value.

        @note The platform agent will translate any None entries to
              corresponding fill_values.
        """

        # step 1:
        # - collect all actual values in a dict indexed by timestamp
        # - keep track of the attributes having actual values
        by_ts = {}  # { ts0 : { attr_n : val_n, ... }, ... }
        attrs_with_actual_values = set()
        for attr_id, attr_vals in self._buffers.iteritems():
            for v, ts in attr_vals:
                if not ts in by_ts:
                    by_ts[ts] = {}

                by_ts[ts][attr_id] = v

                attrs_with_actual_values.add(attr_id)

            # re-init buffer for this attribute:
            self._buffers[attr_id] = []

        if not attrs_with_actual_values:
            # No new data collected at all; nothing to publish, just return:
            log.debug("%r: _dispatch_publication: no new data collected.", self._platform_id)
            return

        """
        # step 2:
        # - put None's for any missing attribute value per timestamp:
        for ts in by_ts:
            # only do this for attrs_with_actual_values:
            # (note: these attributes do have actual values, but not necessarily
            # at every reported timestamp in this cycle):
            for attr_id in attrs_with_actual_values:
                if not attr_id in by_ts[ts]:
                    by_ts[ts][attr_id] = None
        """

        # step 2:
        # - put None's for any missing attribute value per timestamp:
        # EH. Here I used all attributes instead of only the measured ones
        # so the agent can properly populate rdts and construct granules.
        for ts in by_ts:
            # only do this for attrs_with_actual_values:
            # (note: these attributes do have actual values, but not necessarily
            # at every reported timestamp in this cycle):
            for attr_id in self._buffers.keys():
                if not attr_id in by_ts[ts]:
                    by_ts[ts][attr_id] = None

        """        
        # step 3:
        # - construct vals_dict for the event:
        vals_dict = {}
        for attr_id in attrs_with_actual_values:
            vals_dict[attr_id] = []
            for ts in sorted(by_ts.iterkeys()):
                val = by_ts[ts][attr_id]
                vals_dict[attr_id].append((val, ts))
        """
        
        # step 3:
        # - construct vals_dict for the event:
        # EH. Here I used all attributes instead of only the measured ones
        # so the agent can properly populate rdts and construct granules.
        vals_dict = {}
        for attr_id in self._buffers.keys():
            vals_dict[attr_id] = []
            for ts in sorted(by_ts.iterkeys()):
                val = by_ts[ts][attr_id]
                vals_dict[attr_id].append((val, ts))

        # finally, create and notify event:
        driver_event = AttributeValueDriverEvent(self._platform_id,
                                                 _STREAM_NAME,
                                                 vals_dict)

        log.debug("%r: _dispatch_publication: notifying event: %s",
                  self._platform_id, driver_event)

        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            log.trace("%r: vals_dict:\n%s",
                      self._platform_id, self._pp.pformat(driver_event.vals_dict))

        self._notify_driver_event(driver_event)

    def _stop_publisher_greenlet(self):
        if self._publisher_active:
            log.debug("%r: stopping publisher greenlet", self._platform_id)
            self._publisher_active = False
