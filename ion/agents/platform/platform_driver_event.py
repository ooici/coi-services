#!/usr/bin/env python

"""
@package ion.agents.platform.platform_driver_event
@file    ion/agents/platform/platform_driver_event.py
@author  Carlos Rueda
@brief   Classes for platform driver events. These classes are for internal
         coordination within the platform agent framework (they are not
         in principle exposed to the rest of the CI).
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


class DriverEvent(object):
    """
    Base class for platform driver events.
    """
    def __init__(self):
        pass


class AttributeValueDriverEvent(DriverEvent):
    """
    Event to notify the retrieved value for a platform attribute.
    """
    def __init__(self, platform_id, stream_name, vals_dict):
        DriverEvent.__init__(self)
        self._platform_id = platform_id
        self._stream_name = stream_name
        self._vals_dict = vals_dict

    @property
    def platform_id(self):
        return self._platform_id

    @property
    def stream_name(self):
        return self._stream_name

    @property
    def vals_dict(self):
        return self._vals_dict

    def __str__(self):
        return "%s(platform_id=%r, stream_name=%r, vals_dict=%r)" % (
            self.__class__.__name__, self.platform_id, self.stream_name,
            self.vals_dict)

    def brief(self):
        """
        A brief string representation.
        """
        summary = {attr_id: "(%d vals)" % len(vals)
                   for attr_id, vals in self.vals_dict.iteritems()}
        return "%s(platform_id=%r, stream_name=%r, vals_dict=%r)" % (
            self.__class__.__name__, self.platform_id, self.stream_name,
            summary)


class ExternalEventDriverEvent(DriverEvent):
    """
    Event to notify an external event.
    """
    def __init__(self, event_instance):
        DriverEvent.__init__(self)
        self._event_instance = event_instance

    @property
    def event_instance(self):
        return self._event_instance

    def __str__(self):
        return "%s(event_instance=%s)" % (
            self.__class__.__name__, self.event_instance)


class StateChangeDriverEvent(DriverEvent):
    """
    Event to notify a driver has entered a state.
    """
    def __init__(self, state):
        DriverEvent.__init__(self)
        self._state = state

    @property
    def state(self):
        return self._state

    def __str__(self):
        return "%s(state=%r)" % (self.__class__.__name__, self.state)


class AsyncAgentEvent(DriverEvent):
    """
    Event to tell the agent to send a given event to its FSM.
    """
    def __init__(self, event):
        DriverEvent.__init__(self)
        self._event = event

    @property
    def event(self):
        return self._event

    def __str__(self):
        return "%s(event=%r)" % (self.__class__.__name__, self.event)
