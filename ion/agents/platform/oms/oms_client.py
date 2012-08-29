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

    def getPlatformAttributeNames(self, platform_id):
        """
        Returns the names of the attributes associated to a given platform.

        @param platform_id Platform ID
        @retval [attrName, ...]
        """
        raise NotImplemented()

    def getPlatformAttributeValues(self, platAttrMap, from_time):
        """
        Returns the values for specific attributes associated with a given set
        of platforms since a given time.

        @param platAttrMap {platform_id: [attrName, ...], ...} dict indexed by
                           platform ID indicating the desired attributes per
                           platform.
        @param from_time NTP v4 compliant string; time from which the values are requested

        @retval {platform_id: {attrName : [(attrValue, timestamp), ...], ...}, ...}
                dict indexed by platform ID with (value, timestamp) pairs for
                each attribute. Timestamps are NTP v4 compliant strings
        """
        raise NotImplemented()

    def getPlatformAttributeInfo(self, platAttrMap):
        """
        Returns information for specific attributes associated with a given set of platforms.

        @param platAttrMap {platform_id: [attrName, ...], ...}	 dict indexed by platform ID indicating the desired attributes per platform

        @retval {platform_id: {attrName : info, ...}, ...}	 dict indexed by
                platform ID with info dictionary for each attribute.
                info = {'units': val, 'monitorCycleSeconds': val, 'OID' : val }
        """
        raise NotImplemented()

    def getPlatformPorts(self, platform_id):
        """
        Returns the IDs of the ports associated with a given platform.

        @param platform_id	 	 Platform ID

        @retval [port_id, ...]	 List of associated port IDs
        """
        raise NotImplemented()

    def getPortInfo(self, platform_id, port_id):
        """
        Returns IP address for a given port in a platform.

        @param platform_id	 	 Platform ID

        @retval {ip: val}	 dict with associated port info.
                        Note: Returned dict may also include TCP port and
                        other info as appropriate. TBD
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
