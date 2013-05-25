#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.oms_client
@file    ion/agents/platform/rsn/oms_client.py
@author  Carlos Rueda
@brief   CIOMSClient captures the CI-OMS interface.
         See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
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

    See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface

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
        Basic verification of connection with OMS.
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_map(self):
        """
        Returns platform map. This is the network object model in the OMS.

        @retval [(platform_id, parent_platform_id), ...]
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_types(self):
        """
        Returns the types of platforms in the network

        @retval { platform_type: description, ... } Dict of platform types in
         the network.
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_metadata(self, platform_id):
        """
        Returns the metadata for a requested platform.

        @param platform_id Platform ID
        @retval { platform_id: {mdAttrName: mdAttrValue, ...\, ... }
                dict with a single entry for the requested platform ID with a
                dictionary for corresponding metadata
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_attributes(self, platform_id):
        """
        Returns the attributes associated to a given platform.

        @param platform_id Platform ID
        @retval {platform_id: {attrName : info, ...}, ...}
                dict with a single entry for the requested platform ID with an
                info dictionary for each attribute in that platform.
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_attribute_values(self, platform_id, attrs):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def set_platform_attribute_values(self, platform_id, attrs):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_platform_ports(self, platform_id):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def connect_instrument(self, platform_id, port_id, instrument_id, attributes):
        """
        Adds an instrument to a platform port.

        @param platform_id	 	 Platform ID
        @param port_id	 	     Port ID
        @param instrument_id	 Instrument ID
        @param attributes	 	 {'maxCurrentDraw': value, 'initCurrent': value,
                                 'dataThroughput': value, 'instrumentType': value}

        @retval
        """
        raise NotImplementedError()  #pragma: no cover

    def disconnect_instrument(self, platform_id, port_id, instrument_id):
        """
        Removes an instrument from a platform port.

        @param platform_id	 	 Platform ID
        @param port_id	 	     Port ID
        @param instrument_id	 Instrument ID

        @retval
        """
        raise NotImplementedError()  #pragma: no cover

    def get_connected_instruments(self, platform_id, port_id):
        """
        Retrieves the IDs of the instruments connected to a platform port.

        @param platform_id	 	 Platform ID
        @param port_id	 	     Port ID

        @retval
        """
        raise NotImplementedError()  #pragma: no cover

    def turn_on_platform_port(self, platform_id, port_id):
        """
        Turns on a port in a platform. This operation should be called after the
        instruments to be associated with the port have been added (see connect_instrument).

        @param platform_id	 	 Platform ID
        @param port_id	 	     PortID ID

        @retval TBD
        """
        raise NotImplementedError()  #pragma: no cover

    def turn_off_platform_port(self, platform_id, port_id):
        """
        Turns off a port in a platform.

        @param platform_id	 	 Platform ID
        @param port_id	 	     PortID ID

        @retval TBD
        """
        raise NotImplementedError()  #pragma: no cover

    def register_event_listener(self, url):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def unregister_event_listener(self, url):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_registered_event_listeners(self):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_checksum(self, platform_id):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover
