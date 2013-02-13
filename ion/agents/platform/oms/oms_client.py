#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_client
@file    ion/agents/platform/oms/oms_client.py
@author  Carlos Rueda
@brief   OmsClient captures the CI-OMS interface.
         See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


class NormalResponse(object):
    INSTRUMENT_DISCONNECTED       = 'OK_INSTRUMENT_DISCONNECTED'
    PORT_TURNED_ON                = 'OK_PORT_TURNED_ON'
    PORT_ALREADY_ON               = 'OK_PORT_ALREADY_ON'
    PORT_TURNED_OFF               = 'OK_PORT_TURNED_OFF'
    PORT_ALREADY_OFF              = 'OK_PORT_ALREADY_OFF'

class InvalidResponse(object):
    PLATFORM_ID                   = 'INVALID_PLATFORM_ID'
    ATTRIBUTE_NAME                = 'INVALID_ATTRIBUTE_NAME'
    ATTRIBUTE_VALUE_OUT_OF_RANGE  = 'ERROR_ATTRIBUTE_VALUE_OUT_OF_RANGE'
    ATTRIBUTE_NOT_WRITABLE        = 'ERROR_ATTRIBUTE_NOT_WRITABLE'
    PORT_ID                       = 'INVALID_PORT_ID'
    PORT_IS_ON                    = 'ERROR_PORT_IS_ON'

    INSTRUMENT_ID                 = 'INVALID_INSTRUMENT_ID'
    INSTRUMENT_ALREADY_CONNECTED  = 'ERROR_INSTRUMENT_ALREADY_CONNECTED'
    INSTRUMENT_NOT_CONNECTED      = 'ERROR_INSTRUMENT_NOT_CONNECTED'
    MISSING_INSTRUMENT_ATTRIBUTE  = 'MISSING_INSTRUMENT_ATTRIBUTE'
    INVALID_INSTRUMENT_ATTRIBUTE  = 'INVALID_INSTRUMENT_ATTRIBUTE'

    PLATFORM_TYPE                 = 'INVALID_PLATFORM_TYPE'
    EVENT_LISTENER_URL            = 'INVALID_EVENT_LISTENER_URL'
    EVENT_TYPE                    = 'INVALID_EVENT_TYPE'

# required attributes for instrument connection:
REQUIRED_INSTRUMENT_ATTRIBUTES = [
    'maxCurrentDraw', 'initCurrent', 'dataThroughput', 'instrumentType'
]


class OmsClient(object):
    """
    This class captures the interface with OMS.

    See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface

    Note that a preliminary implementation of the real OMS interface used
    "handlers" for grouping operations (for example, "ping" is actually a
    method of the "hello" handler). Here we define all operations at the
    base level. As a simple mechanism to emulate the "handler" mechanism,
    corresponding properties are defined as `self`.
    """

    @property
    def hello(self):
        return self

    @property
    def config(self):
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

    def get_platform_attribute_values(self, platform_id, attrNames, from_time):
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
        Returns information for each port in a given platform.

        @param platform_id	 	 Platform ID

        @retval {platform_id: {port_id: portInfo, ...} }
                Dict with information for each port in the platform
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

    def describe_event_types(self, event_type_ids):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def get_events_by_platform_type(self, platform_types):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def register_event_listener(self, url, event_types):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplementedError()  #pragma: no cover

    def unregister_event_listener(self, url, event_types):
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
