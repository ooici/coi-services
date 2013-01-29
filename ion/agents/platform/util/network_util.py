#!/usr/bin/env python

"""
@package ion.agents.platform.util.network_util
@file    ion/agents/platform/util/network_util.py
@author  Carlos Rueda
@brief   Utilities related with platform network serialization/deserialization.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.util.network import NNode
from ion.agents.platform.util.network import NetworkDefinition

# serialization/deserialization based on YAML
import yaml


class NetworkUtil(object):
    """
    Various utilities including creation of NNode and NetworkDefinition objects.
    """

    @staticmethod
    def create_node_network(map):
        """
        Creates a node network according to the given map (this map is
        the format used by the CI-OMS interface to represent the topology).

        @param map [(platform_id, parent_platform_id), ...]

        @return { platform_id: NNode }
        """
        nodes = {}
        for platform_id, parent_platform_id in map:
            if parent_platform_id is None:
                parent_platform_id = ''

            if not parent_platform_id in nodes:
                nodes[parent_platform_id] = NNode(parent_platform_id)

            if not platform_id in nodes:
                nodes[platform_id] = NNode(platform_id)

            nodes[parent_platform_id].add_subplatform(nodes[platform_id])

        return nodes

    @staticmethod
    def deserialize_network_definition(ser):
        """
        Creates a NetworkDefinition object by deserializing the given argument.

        @param ser representation of the given serialization
        @return A NetworkDefinition object
        """

        ndef = NetworkDefinition()

        def _get_platform_types(pyobj):
            """
            Constructs:
              - ndef._platform_types, {platform_type : description} dict
            """
            assert 'platform_types' in pyobj
            ndef._platform_types = {}
            for ptypeObj in pyobj["platform_types"]:
                assert 'platform_type' in ptypeObj
                assert 'description' in ptypeObj
                platform_type = ptypeObj['platform_type']
                description = ptypeObj['description']
                ndef._platform_types[platform_type] = description

        def _build_network(pyobj):
            """
            Constructs:
              - ndef._nodes: {platform_id : NNode} dict
            """
            assert 'network' in pyobj

            def create_node(platform_id, platform_types=None):
                assert not platform_id in ndef._nodes
                pn = NNode(platform_id, platform_types)
                ndef._nodes[platform_id] = pn
                return pn

            def build_node(platObj, parent_node):
                assert 'platform_id' in platObj
                assert 'platform_types' in platObj
                platform_id = platObj['platform_id']
                platform_types = platObj['platform_types']
                for platform_type in platform_types:
                    assert platform_type in ndef._platform_types
                ports = platObj['ports'] if 'ports' in platObj else []
                attrs = platObj['attrs'] if 'attrs' in platObj else []
                pn = create_node(platform_id, platform_types)
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

            ndef._nodes = {}

            ndef._dummy_root = create_node(platform_id='')

            for platObj in pyobj["network"]:
                build_node(platObj, ndef._dummy_root)

        pyobj = yaml.load(ser)
        _get_platform_types(pyobj)
        _build_network(pyobj)

        return ndef

    @staticmethod
    def serialize_network_definition(ndef):
        """
        Returns a serialization of the given a NetworkDefinition object.

        @param ndef NetworkDefinition object
        @return string with the serialization
        """
        ser = "\nplatform_types:\n"
        for platform_type, description in ndef.platform_types.iteritems():
            ser += "  - platform_type: %s\n" % platform_type
            ser += "    description: %s\n" % description
        ser += "\n%s" % NetworkUtil.serialize_node(ndef.root)

        return ser

    @staticmethod
    def serialize_node(nnode, level=0):
        """
        Returns a serialization of the given a NNode object.

        @param nnode The NNode to serialize
        @param level Indentation level (0 by default)
        @return string with the serialization
        """

        result = ""
        next_level = level
        if nnode.platform_id:
            pid = nnode.platform_id
            lines = []
            if level == 0:
                lines.append('network:')

            lines.append('- platform_id: %s' % pid)
            lines.append('  platform_types: %s' % nnode.platform_types)

            # attributes:
            if len(nnode.attrs):
                lines.append('  attrs:')
                for attr_id, attr in nnode.attrs.iteritems():
                    lines.append('  - attr_id: %s'   % attr_id)
                    for k, v in attr.defn.iteritems():
                        if k != "attr_id":
                            lines.append('    %s: %s'      % (k ,v))

            # ports
            if len(nnode.ports):
                lines.append('  ports:')
                for port_id, port in nnode.ports.iteritems():
                    port_ip = '%s_IP' % port_id
                    lines.append('  - port_id: %s' % port_id)
                    lines.append('    ip: %s '     % port_ip)

            if nnode.subplatforms:
                lines.append('  subplatforms:')

            nl = "\n" + ("  " * level)
            result += nl + nl.join(lines)
            next_level = level + 1

        if nnode.subplatforms:
            for sub_platform in nnode.subplatforms.itervalues():
                result += NetworkUtil.serialize_node(sub_platform, next_level)

        return result
