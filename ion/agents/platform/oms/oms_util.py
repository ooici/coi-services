#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_util
@file    ion/agents/platform/oms/oms_util.py
@author  Carlos Rueda
@brief   RSN OMS Client based utilities
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.util.network import NetworkDefinition
from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.util.network import Attr, Port


class RsnOmsUtil(object):
    """
    RSN OMS Client based utilities.
    """

    @staticmethod
    def build_network_definition(rsn_oms):
        """
        Creates and returns a NetworkDefinition object reflecting the platform
        network definition reported by the RSN OMS Client object.
        The returned object will have as root the NNode corresponding to the
        actual root of the whole newtork. You can use the `nodes` property to
        access any node.

        @param rsn_oms RSN OMS Client object.
        @return NetworkDefinition object
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("build_network_definition. rsn_oms class: %s",
                      rsn_oms.__class__.__name__)

        # platform types:
        platform_types = rsn_oms.config.getPlatformTypes()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("got platform_types %s", str(platform_types))

        # platform map:
        map = rsn_oms.config.getPlatformMap()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("got platform map %s", str(map))

        # build topology:
        nodes = NetworkUtil.create_node_network(map)
        platform_id = rsn_oms.getRootPlatformID()
        assert platform_id

        nnode = nodes[platform_id]

        # now, populate the attributes and ports for the platforms

        def build_attributes_and_ports(nnode):
            """
            Recursive routine to call set_attributes and set_ports on each nnode.
            """
            set_attributes(nnode)
            set_ports(nnode)

            for sub_platform_id, sub_nnode in nnode.subplatforms.iteritems():
                build_attributes_and_ports(sub_nnode)

        def set_attributes(nnode):
            platform_id = nnode.platform_id
            attr_infos = rsn_oms.getPlatformAttributes(platform_id)
            if not isinstance(attr_infos, dict):
                log.warn("%r: getPlatformAttributes returned: %s",
                         platform_id, attr_infos)
                return

            if log.isEnabledFor(logging.DEBUG):
                log.debug("%r: attr_infos: %s", platform_id, attr_infos)

            assert platform_id in attr_infos

            ret_infos = attr_infos[platform_id]
            for attrName, attr_defn in ret_infos.iteritems():
                attr = Attr(attrName, attr_defn)
                nnode.add_attribute(attr)

        def set_ports(nnode):
            platform_id = nnode.platform_id
            port_infos = rsn_oms.getPlatformPorts(platform_id)
            if not isinstance(port_infos, dict):
                log.warn("%r: getPlatformPorts returned: %s",
                         platform_id, port_infos)
                return

            if log.isEnabledFor(logging.DEBUG):
                log.debug("%r: port_infos: %s", platform_id, port_infos)

            assert platform_id in port_infos
            ports = port_infos[platform_id]
            for port_id, dic in ports.iteritems():
                port = Port(port_id, dic['comms']['ip'])
                nnode.add_port(port)

        # call the recursive routine
        build_attributes_and_ports(nnode)

        # we got our whole network including platform attributes and ports.

        # prepare the dummy_root:
        assert '' in nodes, "platform map from RSN OMS expected to have entry with '' as parent"
        dummy_root = nodes['']

        # and finally create and return NetworkDefinition:
        ndef = NetworkDefinition()
        ndef._platform_types = platform_types
        ndef._nodes = nodes
        ndef._dummy_root = dummy_root
        return ndef
