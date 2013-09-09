#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.oms_client
@file    ion/agents/platform/rsn/oms_client.py
@author  Carlos Rueda
@brief   CIOMSClient captures the CI-OMS interface.
         See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
         See module ion.agents.platform.responses.

"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


# required attributes for instrument connection:
REQUIRED_INSTRUMENT_ATTRIBUTES = [
    'maxCurrentDraw', 'initCurrent', 'dataThroughput', 'instrumentType'
]


class CIOMSClient(object):
    """
    This class captures the interface with OMS mainly for purposes of serving
    as a base class for the simulator.

    See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface

    Note: the real OMS interface uses "handlers" for grouping operations
    (for example, "ping" is a method of the "hello" handler). Here we define
    all operations at the base level and emulate the "handler" mechanism via
    corresponding properties defined as `self`.
    """

    @property
    def hello(self):
        return self

    @property
    def config(self):
        return self

    @property
    def attr(self):
        return self

    @property
    def event(self):
        return self

    @property
    def port(self):
        return self

    @property
    def instr(self):
        return self

    def ping(self):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_map(self):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_types(self):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_metadata(self, platform_id):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_attributes(self, platform_id):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_attribute_values(self, platform_id, attrs):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def set_platform_attribute_values(self, platform_id, attrs):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_ports(self, platform_id):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def connect_instrument(self, platform_id, port_id, instrument_id, attributes):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def disconnect_instrument(self, platform_id, port_id, instrument_id):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_connected_instruments(self, platform_id, port_id):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def turn_on_platform_port(self, platform_id, port_id):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def turn_off_platform_port(self, platform_id, port_id):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def register_event_listener(self, url):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def unregister_event_listener(self, url):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_registered_event_listeners(self):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def generate_test_event(self, event):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_checksum(self, platform_id):
        """
        See https://confluence.oceanobservatories.org/display/syseng/CIAD+MI+SV+CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover
