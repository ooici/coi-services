#!/usr/bin/env python

"""
@package ion.agents.platform.status_util
@file    ion/agents/platform/status_util.py
@author  Carlos Rueda
@brief   Helper class for aggregate and rollup status handling.
         Some basic algorithms adapted from observatory_util.py
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
    """

    def __init__(self, pa):
        """
        @param pa   The associated platform agent object to access the
                    elements handled by this helper.
        """
        self._platform_id            = pa._platform_id
        self.aparam_child_agg_status = pa.aparam_child_agg_status
        self.aparam_aggstatus        = pa.aparam_aggstatus
        self.resource_id             = pa.resource_id
        self._event_publisher        = pa._event_publisher

    def got_device_aggregate_status_event(self, evt, *args, **kwargs):
        """
        Reacts to a DeviceAggregateStatusEvent from a platform's child.
        It updates the local image of the child status for the corresponding
        status name, then updates the consolidated for that status name. If this
        consolidated status changes, then a subsequent DeviceAggregateStatusEvent
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

        # calculate new aggstatus:
        new_aggstatus = _consolidate_status(
            [s[status_name] for s in self.aparam_child_agg_status.values()])

        old_aggstatus = self.aparam_aggstatus[status_name]
        if old_aggstatus == new_aggstatus:
            #
            # The specific status changed, but the consolidated one did not;
            # no need to propagate any event up the tree:
            #
            return

        # Here, consolidated status has changed: update aggstatus for this
        # device and status category, and publish event to notify parent:

        self.aparam_aggstatus[status_name] = new_aggstatus

        description = "event generated from platform_id=%r" % self._platform_id
        description += " triggered by event from origin=%r" % child_origin
        evt_out = dict(event_type='DeviceAggregateStatusEvent',
                       origin_type="PlatformDevice",
                       origin=self.resource_id,
                       status_name=status_name,
                       status=new_aggstatus,
                       prev_status=old_aggstatus,
                       description=description)

        if log.isEnabledFor(logging.TRACE):  # pragma: no cover
            self._log_agg_status_update(log.trace, evt, new_aggstatus)

        log.debug("%r: publishing event: %s", self._platform_id, evt_out)
        self._event_publisher.publish_event(**evt_out)

    def got_device_status_event(self, evt, *args, **kwargs):

        # TODO determine how exactly the PA should react to DeviceStatusEvent
        # from its children.
        #
        # NOTE that aggregate status handling is done via DeviceAggregateStatusEvent.
        #
        pass

    def _log_agg_status_update(self, logfun, evt, new_aggstatus):  # pragma: no cover
        """
        Logs formatted statuses for easier inspection; looks like:

2013-04-08 22:22:25,436 DEBUG Dummy-160 ion.agents.platform.status_util:163 'LV01B': event published triggered by event from origin='05c78de65bf7400fac9d56e5d69a72ef'.
Got from child AGGREGATE_COMMS = STATUS_WARNING
updated statuses:
        4fffa3db5a6440c19dbb8b7d975598de : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
        05c78de65bf7400fac9d56e5d69a72ef : {'AGGREGATE_COMMS': 'STATUS_WARNING  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
                            consolidated : {'AGGREGATE_COMMS': 'STATUS_WARNING  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
                                     agg : STATUS_WARNING
Published AGGREGATE_COMMS = STATUS_WARNING

        """
        status_name = evt.status_name
        child_origin = evt.origin
        child_status = evt.status

        msg = "Got from child %s = %s\n" % (
            AggregateStatusType._str_map[status_name],
            DeviceStatusType._str_map[child_status])

        msg += "updated statuses:\n"
        for k, v in self.aparam_child_agg_status.iteritems():
            vs = dict((AggregateStatusType._str_map[k2],
                       "%-16s" % DeviceStatusType._str_map[v2]) for
                      (k2, v2) in v.items())
            msg += "%40s : %s\n" % (k, vs)

        vs = dict((AggregateStatusType._str_map[k2],
                   "%-16s" % DeviceStatusType._str_map[v2]) for
                  (k2, v2) in self.aparam_aggstatus.items())
        msg += "%40s : %s\n" % ("consolidated", vs)

        # 'agg" is the consolidated across the status categories; similar
        # to _rollup_statuses in observatory_util.py.
        agg = _consolidate_status(self.aparam_aggstatus.values())
        msg += "%40s : %s\n" % ("agg", DeviceStatusType._str_map[agg])

        msg += "Published %s = %s\n" % (
            AggregateStatusType._str_map[status_name],
            DeviceStatusType._str_map[new_aggstatus]
        )

        logfun("%r: event published triggered by event from origin=%r.\n%s",
               self._platform_id, child_origin, msg)
