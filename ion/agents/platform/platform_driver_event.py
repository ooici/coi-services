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
    def __init__(self, platform_id, stream_name, attr_id, value):
        DriverEvent.__init__(self)
        self._platform_id = platform_id
        self._stream_name = stream_name
        self._attr_id = attr_id
        self._value = value

    @property
    def platform_id(self):
        return self._platform_id

    @property
    def stream_name(self):
        return self._stream_name

    @property
    def attr_id(self):
        return self._attr_id

    @property
    def value(self):
        return self._value

    def __str__(self):
        return "%s(platform_id=%r, stream_name=%r, attr_id=%r, value=%r)" % (
            self.__class__.__name__, self.platform_id, self.stream_name,
            self.attr_id, self.value)


class ExternalEventDriverEvent(DriverEvent):
    """
    Event to notify an external event.
    """
    def __init__(self, event_type, event_instance):
        DriverEvent.__init__(self)
        self._event_type = event_type
        self._event_instance = event_instance

    @property
    def event_type(self):
        return self._event_type

    @property
    def event_instance(self):
        return self._event_instance

    def __str__(self):
        return "%s(event_type=%r, event_instance=%s)" % (
            self.__class__.__name__, self.event_type, self.event_instance)
