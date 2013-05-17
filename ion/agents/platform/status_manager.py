#!/usr/bin/env python

"""
@package ion.agents.platform.status_manager
@file    ion/agents/platform/status_manager.py
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

from pyon.event.event import EventSubscriber

import logging

from gevent.coros import RLock


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


class StatusManager(object):
    """
    Supporting class for status handling (aparam_child_agg_status,
    aparam_aggstatus, aparam_rollup_status) and related handling with event
    subscribers and publications.

    @see https://confluence.oceanobservatories.org/display/CIDev/Platform+agent+statuses
    """

    def __init__(self, pa):
        """
        Initializes all status parameters according to the immediate
        children of this platform and starts the related subscribers.

        The PlatformAgent must have been already initialized to properly
        access the handled elements.

        Note that the number of subscribers and entries for other
        related status information will increase and decrease
        as we get device_added and device_removed events.

        @param pa   The associated platform agent object to access the
                    elements handled by this helper.
        """

        assert pa._platform_id is not None
        assert pa._children_resource_ids is not None

        self._platform_id            = pa._platform_id
        self.resource_id             = pa.resource_id
        self._event_publisher        = pa._event_publisher
        self.aparam_child_agg_status = pa.aparam_child_agg_status
        self.aparam_aggstatus        = pa.aparam_aggstatus
        self.aparam_rollup_status    = pa.aparam_rollup_status

        # All EventSubscribers created: {origin: EventSubscriber, ...}
        self._event_subscribers = {}

        # set to False by a call to destroy
        self._active = True

        # RLock to synchronize access to the various mutable variables here.
        self._lock = RLock()

        # init statuses, and subscribers for the given children
        with self._lock:
            # initialize my own statuses:
            for status_name in AggregateStatusType._str_map.keys():
                self.aparam_aggstatus[status_name]     = DeviceStatusType.STATUS_UNKNOWN
                self.aparam_rollup_status[status_name] = DeviceStatusType.STATUS_UNKNOWN

            # do status preparations for the children
            for origin in pa._children_resource_ids:
                self._prepare_new_child(origin)

        # diagnostics report on demand:
        self._diag_sub = None
        self._start_diagnostics_subscriber()

    def destroy(self):
        """
        Terminates the status handling.
        Stops all event subscribers and clears self._event_subscribers,
        self.aparam_rollup_status, self.aparam_child_agg_status.
        """

        with self._lock:
            self._active = False

        with self._lock:
            ess = self._event_subscribers.copy()

            self._event_subscribers.clear()
            self.aparam_child_agg_status.clear()
            for status_name in AggregateStatusType._str_map.keys():
                self.aparam_rollup_status[status_name] = DeviceStatusType.STATUS_UNKNOWN

            for origin, es in ess.iteritems():
                self._stop_event_subscriber(origin, es)

        if self._diag_sub:  # pragma: no cover
            self._stop_event_subscriber(None, self._diag_sub)
            self._diag_sub = None

    def _stop_event_subscriber(self, origin, es):
        try:
            es.stop()
        except Exception as ex:
            log.warn("%r: error stopping event subscriber: origin=%r: %s",
                     self._platform_id, origin, ex)

    def publish_device_added_event(self, sub_resource_id):
        """
        PlatformAgent calls this method to publish a DeviceStatusEvent
        indicating that the given child has been launched.

        @param sub_resource_id   resource id of child
        """

        values = [sub_resource_id]
        evt = dict(event_type='DeviceStatusEvent',
                   sub_type="device_added",
                   origin_type="PlatformDevice",
                   origin=self.resource_id,
                   values=values)
        try:
            log.debug('%r: publish_device_added_event for %r: %s',
                      self._platform_id, sub_resource_id, evt)

            self._event_publisher.publish_event(**evt)

        except:
            log.exception('%r: platform agent could not publish event: %s',
                          self._platform_id, evt)

    def publish_device_removed_event(self, sub_resource_id):
        """
        PlatformAgent calls this method to publish a DeviceStatusEvent
        indicating that the given child has been stopped.

        @param sub_resource_id   resource id of child
        """

        values = [sub_resource_id]
        evt = dict(event_type='DeviceStatusEvent',
                   sub_type="device_removed",
                   origin_type="PlatformDevice",
                   origin=self.resource_id,
                   values=values)
        try:
            log.debug('%r: publish_device_removed_event for %r: %s',
                      self._platform_id, sub_resource_id, evt)

            self._event_publisher.publish_event(**evt)

        except:
            log.exception('%r: platform agent could not publish event: %s',
                          self._platform_id, evt)

    def publish_device_failed_command_event(self, sub_resource_id, cmd, err_msg):
        """
        PlatformAgent calls this method to publish a DeviceStatusEvent
        indicating that the given child failed to complete the given command.

        @param sub_resource_id   resource id of child (included in values)
        @param cmd               command (included in description)
        @param err_msg           error message (included in description)
        """

        values = [sub_resource_id]
        description = "cmd=%r; err_msg=%r" % (str(cmd), err_msg)
        evt = dict(event_type='DeviceStatusEvent',
                   sub_type="device_failed_command",
                   origin_type="PlatformDevice",
                   origin=self.resource_id,
                   values=values,
                   description=description)
        try:
            log.debug('%r: publish_device_failed_command_event for %r: %s',
                      self._platform_id, sub_resource_id, evt)

            self._event_publisher.publish_event(**evt)

        except:
            log.exception('%r: platform agent could not publish event: %s',
                          self._platform_id, evt)

    def set_aggstatus(self, status_name, status):
        """
        Sets a particular "aggstatus" for the platform itself.
        The rollup status is updated and an event is published if that rollup
        status changed.

        @param status_name   the particular status category (AggregateStatusType)
        @param status        the status value (DeviceStatusType)
        """

        assert status_name in AggregateStatusType._str_map
        assert status in DeviceStatusType._str_map

        log.debug("%r: set_aggstatus: %s <- %s",
                  self._platform_id,
                  AggregateStatusType._str_map[status_name],
                  DeviceStatusType._str_map[status])

        with self._lock:
            # do the update:
            self.aparam_aggstatus[status_name] = status

            # update aparam_rollup_status:
            self._update_rollup_status_and_publish(status_name)

    #-------------------------------------------------------------------
    # supporting methods related with device_added, device_removed events
    #-------------------------------------------------------------------

    def _start_subscriber_device_status_event(self, origin):
        """
        @param origin    the resource_id associated with child
        """
        event_type = "DeviceStatusEvent"
        sub = EventSubscriber(event_type=event_type,
                              origin=origin,
                              callback=self._got_device_status_event)

        with self._lock:
            self._event_subscribers[origin] = sub
            sub.start()

        log.debug("%r: registered event subscriber for event_type=%r"
                  " coming from origin=%r",
                  self._platform_id, event_type, origin)

    def _got_device_status_event(self, evt, *args, **kwargs):
        """
        Handles "device_added" and "device_removed" DeviceStatusEvents.
        """

        expected_subtypes = ("device_added", "device_removed", "device_failed_command")

        with self._lock:
            if not self._active:
                log.warn("%r: _got_device_status_event called but "
                         "manager has been destroyed",
                         self._platform_id)
                return

        # we are only interested in DeviceStatusEvent directly:
        # (note that also subclasses of DeviceStatusEvent will be notified here)
        if evt.type_ != "DeviceStatusEvent":
            log.trace("%r: ignoring event type %r. Only handle DeviceStatusEvent directly.",
                      self._platform_id, evt.type_)
            return

        sub_type = evt.sub_type

        log.debug("%r: _got_device_status_event: %s\n sub_type=%r",
                  self._platform_id, evt, evt.sub_type)

        assert sub_type in expected_subtypes, \
            "Unexpected sub_type=%r. Expecting one of %r" % (sub_type, expected_subtypes)

        with self._lock:
            if sub_type == "device_added":
                self._device_added_event(evt)
            elif sub_type == "device_removed":
                self._device_removed_event(evt)
            else:
                self.device_failed_command_event(evt)

    def _device_added_event(self, evt):
        """
        Handles the device_added event to do all related preparations and
        updates statuses.
        """

        # look at the event's origin itself to make sure is included:
        self._prepare_new_child(evt.origin)

        # the actual child added is in the values component of the event:
        if isinstance(evt.values, (list, tuple)):
            # normally it will be just one element
            for sub_resource_id in evt.values:
                self._prepare_new_child(sub_resource_id)
        else:
            log.warn("%r: Got device_added event with invalid values member: %r",
                     self._platform_id, evt)
            return

        # finally re-publish event so ancestors also get notified:
        # only adjustment is that now I'm the origin:
        evt = dict(event_type  = evt.type_,
                   sub_type    = evt.sub_type,
                   origin_type = evt.origin_type,
                   origin      = self.resource_id,
                   description = evt.description,
                   values      = evt.values)
        try:
            log.debug('%r: _device_added_event: re-publishing: %s',
                      self._platform_id, evt)

            self._event_publisher.publish_event(**evt)

        except:
            log.exception('%r: platform agent could not publish event: %s',
                          self._platform_id, evt)

    def _prepare_new_child(self, origin):
        """
        Does all status related preparations related with the new child, and do
        status updates, which may result in events being published.

        @param origin   resource id of the child that has been added.
        """

        with self._lock:
            if origin in self._event_subscribers:
                # already prepared -- nothing to do.
                return

            # initialize aparam_child_agg_status for this new child:
            self.aparam_child_agg_status[origin] = {}
            for status_name in AggregateStatusType._str_map.keys():
                self.aparam_child_agg_status[origin][status_name] = DeviceStatusType.STATUS_UNKNOWN

            # start subscribers:
            self._start_subscriber_device_status_event(origin)
            self._start_subscriber_device_aggregate_status_event(origin)

            # update aparam_rollup_status:
            # (note: presumably the new UNKNOWN entries will not cause any changes
            # in the rollups, but we call this here for consistency)
            for status_name in AggregateStatusType._str_map.keys():
                self._update_rollup_status_and_publish(status_name, origin)

    def _device_removed_event(self, evt):
        """
        Handles the device_removed event to remove associated information and
        status updates, which mauy result in events being published.
        """

        # the actual child removed is in the values component of the event:
        if isinstance(evt.values, (list, tuple)):
            # normally it will be just one element but handle as array:
            for sub_resource_id in evt.values:
                self._remove_child(sub_resource_id)
        else:
            log.warn("%r: Got device_removed event with invalid values member: %r",
                     self._platform_id, evt)
            return

        # finally re-publish event so ancestors also get notified:
        # only adjustment is that now I'm the origin:
        evt = dict(event_type  = evt.type_,
                   sub_type    = evt.sub_type,
                   origin_type = evt.origin_type,
                   origin      = self.resource_id,
                   description = evt.description,
                   values      = evt.values)
        try:
            log.debug('%r: _device_removed_event: re-publishing: %s',
                      self._platform_id, evt)

            self._event_publisher.publish_event(**evt)

        except:
            log.exception('%r: platform agent could not publish event: %s',
                          self._platform_id, evt)

    def _remove_child(self, origin):
        """
        Removes the preparations related with the removed child.

        @param origin   resource id of the child that has been removed.
        """

        with self._lock:
            if origin in self._event_subscribers:
                self._terminate_event_subscriber(origin)

            if not origin in self.aparam_child_agg_status:
                return

            del self.aparam_child_agg_status[origin]

            # update aparam_rollup_status:
            for status_name in AggregateStatusType._str_map.keys():
                self._update_rollup_status_and_publish(status_name, origin)

    def _terminate_event_subscriber(self, origin):
        """
        Terminates event subscriber for the given origin and removes the
        entry from _event_subscribers.
        """
        es = self._event_subscribers[origin]
        try:
            es.stop()

        except Exception as ex:
            log.warn("%r: error stopping event subscriber: origin=%r: %s",
                     self._platform_id, origin, ex)

        finally:
            del self._event_subscribers[origin]

    def device_failed_command_event(self, evt):
        """
        @todo Handles the device_failed_command event
        """
        # TODO what should be done?
        log.debug("%r: device_failed_command_event: evt=%s",
                  self._platform_id, str(evt))

    #-------------------------------------------------------------------
    # supporting methods related with aggregate and rollup status
    #-------------------------------------------------------------------

    def _start_subscriber_device_aggregate_status_event(self, origin):
        """
        Starts an event subscriber for aggregate status events from the given
        child (origin).

        @param origin    the resource_id associated with child
        """
        event_type = "DeviceAggregateStatusEvent"
        sub = EventSubscriber(event_type=event_type,
                              origin=origin,
                              callback=self._got_device_aggregate_status_event)

        with self._lock:
            self._event_subscribers[origin] = sub
            sub.start()

        log.debug("%r: registered event subscriber for event_type=%r",
                  self._platform_id, event_type)

    def _got_device_aggregate_status_event(self, evt, *args, **kwargs):
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

        with self._lock:
            if not self._active:
                log.warn("%r: _got_device_aggregate_status_event called but "
                         "manager has been destroyed",
                         self._platform_id)
                return

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

        with self._lock:
            old_status = self.aparam_child_agg_status[child_origin][status_name]
            if child_status == old_status:
                #
                # My image of the child status is not changing, so nothing to do:
                #
                return

            # update the specific status
            self.aparam_child_agg_status[child_origin][status_name] = child_status

            new_rollup_status = self._update_rollup_status_and_publish(status_name, child_origin)

        if new_rollup_status and log.isEnabledFor(logging.TRACE):  # pragma: no cover
            self._log_agg_status_update(log.trace, evt, new_rollup_status)

    def _update_rollup_status_and_publish(self, status_name, child_origin=None):
        """
        Re-consolidates the rollup status for the given status and publishes
        event in case this status changed.

        @param status_name   the specific status category
        @param child_origin  the origin of the child that triggered the
                             update, if any. None by default

        @return new_rollup_status
                             The new rollup status (also indicating that an event
                             was published), or None if no publication was necessary
        """

        with self._lock:
            # get all status values for the status name, that is,
            # all from the children ...
            all_status_values = [s[status_name] for s in self.aparam_child_agg_status.values()]

            # plus status from the platform itself ...
            all_status_values.append(self.aparam_aggstatus[status_name])

            # ... to calculate the new rollup_status:
            new_rollup_status = _consolidate_status(all_status_values)

            # see if we have a new rollup status:
            old_rollup_status = self.aparam_rollup_status[status_name]
            if old_rollup_status == new_rollup_status:
                #
                # The specific status changed, but the rollup one did not;
                # no need to propagate any event up the tree from here:
                #
                return None

            # Here, rollup status has changed: update rollup_status for this
            # device and status category,

            self.aparam_rollup_status[status_name] = new_rollup_status

        # and publish event to notify all interested ancestors:
        description = "event generated from platform_id=%r" % self._platform_id
        if child_origin:
            description += " triggered by event from child=%r" % child_origin

        evt_out = dict(event_type='DeviceAggregateStatusEvent',
                       origin_type="PlatformDevice",
                       origin=self.resource_id,
                       status_name=status_name,
                       status=new_rollup_status,
                       prev_status=old_rollup_status,
                       roll_up_status=True,
                       description=description)

        log.debug("%r: publishing event: %s", self._platform_id, evt_out)
        self._event_publisher.publish_event(**evt_out)

        return new_rollup_status

    #----------------------------------
    # misc
    #----------------------------------

    def _log_agg_status_update(self, logfun, evt, new_rollup_status):  # pragma: no cover
        """
        Logs formatted statuses for easier inspection; looks like:

013-04-10 21:10:24,193 TRACE Dummy-174 ion.agents.platform.status_manager:204 'LV01A': event published triggered by event from child '6c9ed2a39e2b426890e14de986c48db9': AGGREGATE_COMMS -> STATUS_CRITICAL
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

    def _start_diagnostics_subscriber(self):  # pragma: no cover
        """
        For debugging/diagnostics purposes.
        Registers a subscriber to DeviceEvent events with origin="command_line"
        and sub_type="diagnoser" to log the current statuses via log.info.
        This method does nothing if the logging level is not enabled for INFO
        for this module.

        From the pycc command line, the event can be sent as indicated in
        publish_event_for_diagnostics().

        """
        # TODO perhaps a more visible/official command for diagnostic purposes,
        # and for resource agents in general should be considered, something
        # like RESOURCE_AGENT_EVENT_REPORT_DIAGNOSTICS.

        if not log.isEnabledFor(logging.INFO):
            return

        event_type  = "DeviceEvent"
        origin      = "command_line"
        sub_type    = "diagnoser"

        def got_event(evt, *args, **kwargs):
            if not self._active:
                log.warn("%r: got_event called but manager has been destroyed",
                         self._platform_id)
                return

            if evt.type_ != event_type:
                log.trace("%r: ignoring event type %r. Only handle %r directly",
                          self._platform_id, evt.type_, event_type)
                return

            if evt.sub_type != sub_type:
                log.trace("%r: ignoring event sub_type %r. Only handle %r",
                          self._platform_id, evt.sub_type, sub_type)
                return

            statuses = formatted_statuses(self.aparam_aggstatus,
                                          self.aparam_child_agg_status,
                                          self.aparam_rollup_status)
            log.info("%r: statuses:\n%s\n", self._platform_id, statuses)

        self._diag_sub = EventSubscriber(event_type=event_type,
                                    origin=origin,
                                    sub_type=sub_type,
                                    callback=got_event)
        self._diag_sub.start()

        log.info("%r: registered diagnostics event subscriber", self._platform_id)


#----------------------------------
# some utilities

def publish_event_for_diagnostics():  # pragma: no cover
    """
    Convenient method to do the publication of the event to generate diagnostic
    information about the statuses kept in each running platform agent.

    ><> from ion.agents.platform.status_manager import publish_event_for_diagnostics
    ><> publish_event_for_diagnostics()

    and something like the following will be logged out:

    2013-05-16 15:09:06,754 INFO Dummy-360 ion.agents.platform.status_manager:673 'LJ01D': statuses:
                                   aggstatus : {'AGGREGATE_COMMS': 'STATUS_OK       ', 'AGGREGATE_POWER': 'STATUS_OK       ', 'AGGREGATE_DATA': 'STATUS_OK       ', 'AGGREGATE_LOCATION': 'STATUS_OK       '}
            09b9091514904d608527f970453da519 : {'AGGREGATE_COMMS': 'STATUS_OK       ', 'AGGREGATE_POWER': 'STATUS_OK       ', 'AGGREGATE_DATA': 'STATUS_OK       ', 'AGGREGATE_LOCATION': 'STATUS_OK       '}
            7e25d59091464e4f8b042e63df929cb0 : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
                               rollup_status : {'AGGREGATE_COMMS': 'STATUS_OK       ', 'AGGREGATE_POWER': 'STATUS_OK       ', 'AGGREGATE_DATA': 'STATUS_OK       ', 'AGGREGATE_LOCATION': 'STATUS_OK       '}
    """

    from pyon.event.event import EventPublisher
    ep = EventPublisher()
    evt = dict(event_type='DeviceEvent', sub_type='diagnoser', origin='command_line')
    print("publishing: %s" % str(evt))
    ep.publish_event(**evt)


def formatted_statuses(aggstatus, child_agg_status, rollup_status):  # pragma: no cover
    """
    returns a string with formatted statuses like so:

                           aggstatus : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
    12977248dd594e0ca4048bfbd28cfb56 : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
    a583e69d83e549088764757d7beaa9a4 : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
    6c9ed2a39e2b426890e14de986c48db9 : {'AGGREGATE_COMMS': 'STATUS_CRITICAL ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
    42301443895f4f038845f772c4af437d : {'AGGREGATE_COMMS': 'STATUS_UNKNOWN  ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
                       rollup_status : {'AGGREGATE_COMMS': 'STATUS_CRITICAL ', 'AGGREGATE_POWER': 'STATUS_UNKNOWN  ', 'AGGREGATE_DATA': 'STATUS_UNKNOWN  ', 'AGGREGATE_LOCATION': 'STATUS_UNKNOWN  '}
    """

    msg = ""

    # aggstatus:
    vs = dict((AggregateStatusType._str_map[k2],
               "%-16s" % DeviceStatusType._str_map[v2]) for
              (k2, v2) in aggstatus.items())
    msg += "%40s : %s\n" % ("aggstatus", vs)

    # child_agg_status:
    for k, v in child_agg_status.iteritems():
        vs = dict((AggregateStatusType._str_map[k2],
                   "%-16s" % DeviceStatusType._str_map[v2]) for
                  (k2, v2) in v.items())
        msg += "%40s : %s\n" % (k, vs)

    # rollup_status:
    vs = dict((AggregateStatusType._str_map[k2],
               "%-16s" % DeviceStatusType._str_map[v2]) for
              (k2, v2) in rollup_status.items())
    msg += "%40s : %s\n" % ("rollup_status", vs)

    return msg
