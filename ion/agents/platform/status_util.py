#!/usr/bin/env python

"""
@package ion.agents.platform.status_util
@file    ion/agents/platform/status_util.py
@author  Carlos Rueda
@brief   Helper class for aggregate and rollup status handling.
         Some basic algorithms adapted from observatory_util.py
@see     https://confluence.oceanobservatories.org/display/CIDev/Platform+agent+statuses
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log

from ion.agents.platform.exceptions import PlatformException

from interface.objects import AggregateStatusType
from interface.objects import DeviceStatusType

import logging


# The consolidation operation is adapted from observatory_util.py to
# work on DeviceStatusType (instead of StatusType).
def _consolidate_status(statuses, warn_if_unknown=False):
    """Intelligently merge statuses with current value"""

    # Any critical means all critical
    if DeviceStatusType.STATUS_CRITICAL in statuses:
        return DeviceStatusType.STATUS_CRITICAL

    # Any warning means all warning
    if DeviceStatusType.STATUS_WARNING in statuses:
        return DeviceStatusType.STATUS_WARNING

    # Any unknown is fine unless some are ok -- then it's a warning
    if DeviceStatusType.STATUS_OK in statuses:
        if DeviceStatusType.STATUS_UNKNOWN in statuses and warn_if_unknown:
            return DeviceStatusType.STATUS_WARNING
        else:
            return DeviceStatusType.STATUS_OK

    # 0 results are OK, 0 or more are unknown
    return DeviceStatusType.STATUS_UNKNOWN


class StatusUtil(object):
    """
    Helper class for aggregate and rollup status handling.

    @see https://confluence.oceanobservatories.org/display/CIDev/Platform+agent+statuses
    """

    def __init__(self, pa):
        """
        @param pa   The associated platform agent object to access the
                    elements handled by this helper.
        """
        self._platform_id            = pa._platform_id
        self.aparam_child_agg_status = pa.aparam_child_agg_status
        self.aparam_aggstatus        = pa.aparam_aggstatus
        self.aparam_rollup_status    = pa.aparam_rollup_status
        self.resource_id             = pa.resource_id
        self._event_publisher        = pa._event_publisher

    def got_device_aggregate_status_event(self, evt, *args, **kwargs):
        """
        Reacts to a DeviceAggregateStatusEvent from a platform's child.
        It updates the local image of the child status for the corresponding
        status name, then updates the rollup status for that status name.
        If this rollup status changes, then a subsequent DeviceAggregateStatusEvent
        is published.
        The consolidation operation is adapted from observatory_util.py to
        work on DeviceStatusType (instead of StatusType).

        @param evt    DeviceAggregateStatusEvent from child.
        """
        log.debug("%r: _got_device_aggregate_status_event: %s",
                  self._platform_id, evt)

        if evt.origin not in self.aparam_child_agg_status:
            # should not happen.
            msg = "%r: got event from unrecognized origin=%s" % (
                  self._platform_id, evt.origin)
            log.error(msg)
            raise PlatformException(msg)

        if evt.type_ != "DeviceAggregateStatusEvent":
            # should not happen.
            msg = "%r: Got event for different event_type=%r but subscribed to %r" % (
                self._platform_id, evt.type_, "DeviceAggregateStatusEvent")
            log.error(msg)
            raise PlatformException(msg)

        status_name = evt.status_name
        child_origin = evt.origin
        child_status = evt.status

        old_status = self.aparam_child_agg_status[child_origin][status_name]
        if child_status == old_status:
            #
            # My image of the child status is not changing, so nothing to do:
            #
            return

        # update the specific status
        self.aparam_child_agg_status[child_origin][status_name] = child_status

        # get all status values for the status name, that is,
        # all from the children and from the platform itself ...
        all_status_values = [s[status_name] for s in self.aparam_child_agg_status.values()]
        all_status_values.append(self.aparam_aggstatus[status_name])
        # ... to calculate the new rollup_status:
        new_rollup_status = _consolidate_status(all_status_values)

        old_rollup_status = self.aparam_rollup_status[status_name]
        if old_rollup_status == new_rollup_status:
            #
            # The specific status changed, but the rollup one did not;
            # no need to propagate any event up the tree from here:
            #
            return

        # Here, rollup status has changed: update rollup_status for this
        # device and status category, and publish event to notify all
        # interested ancestors:

        self.aparam_rollup_status[status_name] = new_rollup_status

        description = "event generated from platform_id=%r" % self._platform_id
        description += " triggered by event from origin=%r" % child_origin
        evt_out = dict(event_type='DeviceAggregateStatusEvent',
                       origin_type="PlatformDevice",
                       origin=self.resource_id,
                       status_name=status_name,
                       status=new_rollup_status,
                       prev_status=old_rollup_status,
                       roll_up_status=True,
                       description=description)

        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            self._log_agg_status_update(log.trace, evt, new_rollup_status)

        log.debug("%r: publishing event: %s", self._platform_id, evt_out)
        self._event_publisher.publish_event(**evt_out)

    def got_device_status_event(self, evt, *args, **kwargs):

        # TODO determine how exactly the PA should react to DeviceStatusEvent
        # from its children.
        #
        # NOTE that aggregate status handling is done via DeviceAggregateStatusEvent.
        #
        pass

    def _log_agg_status_update(self, logfun, evt, new_rollup_status):  # pragma: no cover
        """
        Logs formatted statuses for easier inspection; looks like:

013-04-10 21:10:24,193 TRACE Dummy-174 ion.agents.platform.status_util:204 'LV01A': event published triggered by event from child '6c9ed2a39e2b426890e14de986c48db9': AGGREGATE_COMMS -> STATUS_CRITICAL
                               aggstatus : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
        12977248dd594e0ca4048bfbd28cfb56 : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
        a583e69d83e549088764757d7beaa9a4 : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
        6c9ed2a39e2b426890e14de986c48db9 : {'AGGREGATE_COMMS': 'STATUS_CRITICAL ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
        42301443895f4f038845f772c4af437d : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
                           rollup_status : {'AGGREGATE_COMMS': 'STATUS_CRITICAL ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
Published event: AGGREGATE_COMMS -> STATUS_CRITICAL
        """

        status_name = evt.status_name
        child_origin = evt.origin
        child_status = evt.status

        # show the event from the child:
        msg = "%s -> %s\n" % (
            AggregateStatusType._str_map[status_name],
            DeviceStatusType._str_map[child_status])

        # show aparam_aggstatus:
        vs = dict((AggregateStatusType._str_map[k2],
                   "%-16s" % DeviceStatusType._str_map[v2]) for
                  (k2, v2) in self.aparam_aggstatus.items())
        msg += "%40s : %s\n" % ("aggstatus", vs)

        # show updated aparam_child_agg_status:
        for k, v in self.aparam_child_agg_status.iteritems():
            vs = dict((AggregateStatusType._str_map[k2],
                       "%-16s" % DeviceStatusType._str_map[v2]) for
                      (k2, v2) in v.items())
            msg += "%40s : %s\n" % (k, vs)

        # show updated aparam_rollup_status:
        vs = dict((AggregateStatusType._str_map[k2],
                   "%-16s" % DeviceStatusType._str_map[v2]) for
                  (k2, v2) in self.aparam_rollup_status.items())
        msg += "%40s : %s\n" % ("rollup_status", vs)

        # show published event with the specific new_rollup_status:
        msg += "Published event: %s -> %s\n" % (
            AggregateStatusType._str_map[status_name],
            DeviceStatusType._str_map[new_rollup_status]
        )

        logfun("%r: event published triggered by event from child %r: %s",
               self._platform_id, child_origin, msg)
