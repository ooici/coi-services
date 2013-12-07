#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.oms_util
@file    ion/agents/platform/rsn/oms_util.py
@author  Carlos Rueda
@brief   RSN OMS Client based utilities
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.util.network import NetworkDefinition
from ion.agents.platform.util.network_util import NetworkUtil
from ion.agents.platform.util.network import AttrNode, PortNode
from ion.agents.platform.util.network import InstrumentNode
from ion.agents.platform.exceptions import PlatformDriverException


class RsnOmsUtil(object):
    """
    RSN OMS Client based utilities.
    """

    @staticmethod
    def build_network_definition(rsn_oms):
        """
        Creates and returns a NetworkDefinition object reflecting the platform
        network definition reported by the RSN OMS Client object.
        The returned object will have as root the PlatformNode corresponding to the
        actual root of the whole newtork. You can use the `pnodes` property to
        access any node.

        @param rsn_oms RSN OMS Client object.
        @return NetworkDefinition object
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("build_network_definition. rsn_oms class: %s",
                      rsn_oms.__class__.__name__)

        # platform types:
        platform_types = rsn_oms.config.get_platform_types()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("got platform_types %s", str(platform_types))

        # platform map:
        map = rsn_oms.config.get_platform_map()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("got platform map %s", str(map))

        # build topology:
        pnodes = NetworkUtil.create_node_network(map)
        dummy_root = pnodes['']
        root_pnode = pnodes[dummy_root.subplatforms.keys()[0]]
        if log.isEnabledFor(logging.DEBUG):
            log.debug("topology's root platform_id=%r", root_pnode.platform_id)

        # now, populate the attributes and ports for the platforms

        def build_attributes_and_ports(pnode):
            """
            Recursive routine to call set_attributes and set_ports on each pnode.
            """
            set_attributes(pnode)
            set_ports(pnode)

            for sub_platform_id, sub_pnode in pnode.subplatforms.iteritems():
                build_attributes_and_ports(sub_pnode)

        def set_attributes(pnode):
            platform_id = pnode.platform_id
            attr_infos = rsn_oms.attr.get_platform_attributes(platform_id)
            if not isinstance(attr_infos, dict):
                raise PlatformDriverException(
                    "%r: get_platform_attributes returned: %s" % (
                    platform_id, attr_infos))

            if log.isEnabledFor(logging.TRACE):
                log.trace("%r: attr_infos: %s", platform_id, attr_infos)

            if not platform_id in attr_infos:
                raise PlatformDriverException(
                    "%r: get_platform_attributes response does not "
                    "include entry for platform_id: %s" %(
                    platform_id, attr_infos))

            ret_infos = attr_infos[platform_id]
            for attrName, attr_defn in ret_infos.iteritems():
                attr = AttrNode(attrName, attr_defn)
                pnode.add_attribute(attr)

        def set_ports(pnode):
            platform_id = pnode.platform_id
            port_infos = rsn_oms.port.get_platform_ports(platform_id)
            if not isinstance(port_infos, dict):
                raise PlatformDriverException(
                    "%r: get_platform_ports response is not a dict: %s" % (
                    platform_id, port_infos))

            if log.isEnabledFor(logging.TRACE):
                log.trace("%r: port_infos: %s", platform_id, port_infos)

            if not platform_id in port_infos:
                raise PlatformDriverException(
                    "%r: get_platform_ports response does not include "
                    "platform_id: %s" % (platform_id, port_infos))

            ports = port_infos[platform_id]

            if not isinstance(ports, dict):
                raise PlatformDriverException(
                    "%r: get_platform_ports: entry for platform_id is "
                    "not a dict: %s" % (platform_id, ports))

            for port_id, dic in ports.iteritems():
                port = PortNode(port_id)
                port.set_state(dic['state'])
                pnode.add_port(port)

                # add connected instruments:
                instrs_res = rsn_oms.instr.get_connected_instruments(platform_id, port_id)
                if not isinstance(instrs_res, dict):
                    log.warn("%r: port_id=%r: get_connected_instruments "
                             "response is not a dict: %s" % (platform_id, port_id, instrs_res))
                    continue

                if log.isEnabledFor(logging.TRACE):
                    log.trace("%r: port_id=%r: get_connected_instruments "
                              "returned: %s" % (platform_id, port_id, instrs_res))

                if not platform_id in instrs_res:
                    raise PlatformDriverException(
                        "%r: port_id=%r: get_connected_instruments response"
                        "does not have entry for platform_id: %s" % (
                        platform_id, ports))

                if not port_id in instrs_res[platform_id]:
                    raise PlatformDriverException(
                        "%r: port_id=%r: get_connected_instruments response "
                        "for platform_id does not have entry for port_id: %s" % (
                        platform_id, port_id, instrs_res[platform_id]))

                instr = instrs_res[platform_id][port_id]
                for instrument_id, attrs in instr.iteritems():
                    port.add_instrument(InstrumentNode(instrument_id, attrs))

        # call the recursive routine
        build_attributes_and_ports(root_pnode)

        # we got our whole network including platform attributes and ports.

        # and finally create and return NetworkDefinition:
        ndef = NetworkDefinition()
        ndef._platform_types = platform_types
        ndef._pnodes = pnodes
        ndef._dummy_root = dummy_root
        return ndef
