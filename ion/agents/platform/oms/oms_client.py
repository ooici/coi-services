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


class InvalidResponse(object):
    PLATFORM_ID                   = 'INVALID_PLATFORM_ID'
    ATTRIBUTE_NAME                = 'INVALID_ATTRIBUTE_NAME'
    ATTRIBUTE_VALUE_OUT_OF_RANGE  = 'ATTRIBUTE_VALUE_OUT_OF_RANGE'
    ATTRIBUTE_NOT_WRITABLE        = 'ATTRIBUTE_NOT_WRITABLE'
    PORT_ID                       = 'INVALID_PORT_ID'

    PLATFORM_TYPE                 = 'INVALID_PLATFORM_TYPE'
    EVENT_LISTENER_URL            = 'INVALID_EVENT_LISTENER_URL'
    EVENT_TYPE                    = 'INVALID_EVENT_TYPE'

VALID_PORT_ATTRIBUTES = [
    'maxCurrentDraw', 'initCurrent', 'dataThroughput', 'instrumentType'
]


class OmsClient(object):
    """
    This class captures the interface with OMS.

    See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface

    Note that the real OMS interface uses "handlers" for grouping operations
    (for example, "ping" is actually a method of the "hello" handler). Here we
    define all operations at the base level. As a simple trick to emulate the
    "handler" mechanism, corresponding properties are defined as self.
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
        raise NotImplemented()

    def getPlatformMap(self):
        """
        Returns platform map. This is the network object model in the OMS.

        @retval [(platform_id, parent_platform_id), ...]
        """
        raise NotImplemented()

    def getRootPlatformID(self):
        """
        Returns the ID of the root platform in the network.
        @retval the ID of the root platform in the network.
        """
        raise NotImplemented()

    def getSubplatformIDs(self, platform_id):
        """
        Returns the IDs of the sub-platforms of the given platform.
        @retval     {platform_id: [sub_platform_id, ...]}
                    Dict with single entry for the desired platform with list
                    of IDs of the corresponding sub-platforms. ||
        """
        raise NotImplemented()

    def getPlatformTypes(self):
        """
        Returns the types of platforms in the network

        @retval { platform_type: description, ... } Dict of platform types in
         the network.
        """
        raise NotImplemented()

    def getPlatformMetadata(self, platform_id):
        """
        Returns the metadata for a requested platform.

        @param platform_id Platform ID
        @retval { platform_id: {mdAttrName: mdAttrValue, ...\, ... }
                dict with a single entry for the requested platform ID with a
                dictionary for corresponding metadata
        """
        raise NotImplemented()

    def getPlatformAttributes(self, platform_id):
        """
        Returns the attributes associated to a given platform.

        @param platform_id Platform ID
        @retval {platform_id: {attrName : info, ...}, ...}
                dict with a single entry for the requested platform ID with an
                info dictionary for each attribute in that platform.
        """
        raise NotImplemented()

    def getPlatformAttributeValues(self, platform_id, attrNames, from_time):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplemented()

    def setPlatformAttributeValues(self, platform_id, attrs):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplemented()

    def getPlatformPorts(self, platform_id):
        """
        Returns information for each port in a given platform.

        @param platform_id	 	 Platform ID

        @retval {platform_id: {port_id: portInfo, ...} }
                Dict with information for each port in the platform
        """
        raise NotImplemented()

    def setUpPort(self, platform_id, port_id, attributes):
        """
        Sets up a port in a platform.

        @param platform_id	 	 Platform ID
        @param port_id	 	     PortID ID
        @param attributes	 	 {'maxCurrentDraw': value, 'initCurrent': value,
                                 'dataThroughput': value, 'instrumentType': value}

        @retval TBD
        """
        raise NotImplemented()

    def turnOnPort(self, platform_id, port_id):
        """
        Turns on a port in a platform. The port should have previously set up with setUpPort

        @param platform_id	 	 Platform ID
        @param port_id	 	     PortID ID

        @retval TBD
        """
        raise NotImplemented()

    def turnOffPort(self, platform_id, port_id):
        """
        Turns off a port in a platform.

        @param platform_id	 	 Platform ID
        @param port_id	 	     PortID ID

        @retval TBD
        """
        raise NotImplemented()

    def describeEventTypes(self, event_type_ids):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplemented()

    def getEventsByPlatformType(self, platform_types):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplemented()

    def registerEventListener(self, url, event_types):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplemented()

    def unregisterEventListener(self, url, event_types):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplemented()

    def getRegisteredEventListeners(self):
        """
        See https://confluence.oceanobservatories.org/display/CIDev/CI-OMS+interface
        """
        raise NotImplemented()
