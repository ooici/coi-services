#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.oms_simulator
@file    ion/agents/platform/oms/simulator/oms_simulator.py
@author  Carlos Rueda
@brief   OMS simulator
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.oms.oms_client import OmsClient
from ion.agents.platform.util.network import NNode

import yaml
import time

import logging

log = logging.getLogger('oms_simulator')


VALID_PORT_ATTRIBUTES = [
    'maxCurrentDraw', 'initCurrent', 'dataThroughput', 'instrumentType'
]


INVALID_PLATFORM_ID_RESPONSE = 'INVALID-PLATFORM-ID'
INVALID_ATTRIBUTE_NAME_RESPONSE = ('INVALID-ATTRIBUTE-NAME', '')

class OmsSimulator(OmsClient):
    """
    Implementation of OmsClient for testing purposes.
    Status: preliminary.
    """

    def __init__(self, yaml_filename='ion/agents/platform/oms/simulator/network.yml'):

        pyobj = yaml.load(file(yaml_filename))
        assert 'network' in pyobj
        self._build_network(pyobj)

    def _build_network(self, pyobj):
        """
        Constructs:
          - self._idp: {platform_id : NNode} map
          - self._dummy_root: The "dummy" root node; its children are the actual roots.
        """
        def create_node(platform_id):
            assert not platform_id in self._idp
            pn = NNode(platform_id)
            self._idp[platform_id] = pn
            return pn

        def build_node(platObj, parent_node):
            assert 'platform_id' in platObj
            platform_id = platObj['platform_id']
            ports = platObj['ports'] if 'ports' in platObj else []
            attrs = platObj['attrs'] if 'attrs' in platObj else []
            pn = create_node(platform_id)
            parent_node.add_subplatform(pn)
            pn.set_ports(ports)
            pn.set_attributes(attrs)
            if 'subplatforms' in platObj:
                for subplat in platObj['subplatforms']:
                    subplat_id = subplat['platform_id']
                    if subplat_id in pn.subplatforms:
                        raise Exception('%s: duplicate subplatform ID for parent %s' % (
                            subplat_id, platform_id))
                    build_node(subplat, pn)
            return pn

        self._idp = {}
        self._dummy_root = create_node(platform_id='')

        for platObj in pyobj["network"]:
            assert 'platform_id' in platObj
            build_node(platObj, self._dummy_root)

    def ping(self):
        return "pong"

    def getPlatformMap(self):
        """
        Returns platform map. This is the network object model in the OMS.

        @retval [(platform_id, parent_platform_id), ...]
        """
        return self._dummy_root.get_map([])

    def getPlatformAttributeNames(self, platform_id):
        """
        Returns the names of the attributes assocciated to a given platform.

        @param platform_id Platform ID
        @retval [attrName, ...]
        """
        return list(self._idp[platform_id].attrs.iterkeys())

    def dump(self):
        """indented string representation"""
        return self._dummy_root.dump()

    def getPlatformAttributeValues(self, platAttrMap, from_time):
        """
        Returns the values for specific attributes associated with a given set
        of platforms since a given time.

        @param platAttrMap {platform_id: [attrName, ...], ...} dict indexed by
                           platform ID indicating the desired attributes per
                           platform.
        @param from_time NTP v4 compliant string; time from which the values
                         are requested. For a given attribute, its value is
                         reported only if from_time < ts, where ts is the
                         timestamp of the value.

        @retval {platform_id: {attrName : [(attrValue, timestamp), ...], ...}, ...}
                dict indexed by platform ID with (value, timestamp) pairs for
                each attribute. Timestamps are NTP v4 compliant strings
        """
        retval = {}
        timestamp = time.time()
        for platform_id, attributeNames in platAttrMap.iteritems():
            if platform_id in self._idp:
                attrs = self._idp[platform_id].attrs
                vals = {}
                for attrName in attributeNames:
                    if attrName in attrs:
                        attr = attrs[attrName]
                        val = attr._value
                        #
                        # TODO determine how to handle attribute with no value:
                        # should it be reported back with some special
                        # "no-value" mark? just do not report it?
                        # For now, we only report attributes with actual value.
                        #
                        if val is None:
                            continue

                        if from_time < timestamp:
                            vals[attrName] = (val, timestamp)
                    else:
                        vals[attrName] = INVALID_ATTRIBUTE_NAME_RESPONSE
                retval[platform_id] = vals
            else:
                retval[platform_id] = INVALID_PLATFORM_ID_RESPONSE

        return retval

    def getPlatformAttributeInfo(self, platAttrMap):
        """
        Returns information for specific attributes associated with a given set of platforms.

        @param platAttrMap {platform_id: [attrName, ...], ...} dict indexed by
                           platform ID indicating the desired attributes per
                           platform.

        @retval {platform_id: {attrName : info, ...}, ...}
                dict indexed by platform ID with info dictionary for each attribute.
                info = {'units': val, 'monitorCycleSeconds': val, 'OID' : val }
        """
        retval = {}
        for platform_id, attributeNames in platAttrMap.iteritems():
            if platform_id in self._idp:
                attrs = self._idp[platform_id].attrs
                ret_infos = {}
                for attrName in attributeNames:
                    if attrName in attrs:
                        attr = attrs[attrName]
                        ret_infos[attrName] = attr.defn
                    else:
                        ret_infos[attrName] = "INVALID-ATTRIBUTE-NAME"
                retval[platform_id] = ret_infos
            else:
                retval[platform_id] = "INVALID-PLATFORM-ID"

        return retval

    def getPlatformPorts(self, platform_id):
        assert platform_id in self._idp
        return list(self._idp[platform_id].ports.iterkeys())

    def getPortInfo(self, platform_id, port_id):
        assert platform_id in self._idp
#        ports = self._idp[platform_id].ports
#        assert port_id in ports
#        return ports[port_id]

        port_comms = self._idp[platform_id].get_port(port_id).comms
        return port_comms

    def setUpPort(self, platform_id, port_id, attributes):
        assert platform_id in self._idp
        port_attrs = self._idp[platform_id].get_port(port_id).attrs

        # retval will contain the attributes that were set
        retval = {}
        for key, val in attributes.iteritems():
            if key in VALID_PORT_ATTRIBUTES:
                # TODO validate the value
                port_attrs[key] = val
                retval[key] = val
            else:
                log.warn("setUpPort called with unrecognized attribute: %s"% key)

        return retval

    def turnOnPort(self, platform_id, port_id):
        assert platform_id in self._idp
        port = self._idp[platform_id].get_port(port_id)
        if port._on:
            log.warn("port %s in platform %s already turned on." % (port_id, platform_id))
        else:
            port._on = True
            log.info("port %s in platform %s turned on." % (port_id, platform_id))
        return port._on

    def turnOffPort(self, platform_id, port_id):
        assert platform_id in self._idp
        port = self._idp[platform_id].get_port(port_id)
        if not port._on:
            log.warn("port %s in platform %s already turned off." % (port_id, platform_id))
        else:
            port._on = False
            log.info("port %s in platform %s turned off." % (port_id, platform_id))
        return port._on
