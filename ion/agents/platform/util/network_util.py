#!/usr/bin/env python

"""
@package ion.agents.platform.util.network_util
@file    ion/agents/platform/util/network_util.py
@author  Carlos Rueda
@brief   Utilities related with platform network definition
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.util.network import PlatformNode
from ion.agents.platform.util.network import AttrNode
from ion.agents.platform.util.network import PortNode
from ion.agents.platform.util.network import InstrumentNode
from ion.agents.platform.util.network import NetworkDefinition
from ion.agents.platform.exceptions import PlatformDefinitionException

import yaml
from collections import OrderedDict


class NetworkUtil(object):
    """
    Various utilities including creation of PlatformNode and NetworkDefinition objects.
    """

    @staticmethod
    def deserialize_network_definition(ser):
        """
        Creates a NetworkDefinition object by deserializing the given argument.

        @param ser representation of the given serialization
        @return A NetworkDefinition object
        """

        ndef = NetworkDefinition()

        def _build_network(pyobj):
            """
            Constructs:
              - ndef._pnodes: {platform_id : PlatformNode} dict
            """

            def create_node(platform_id):
                _require(not platform_id in ndef.pnodes)
                pn = PlatformNode(platform_id)
                ndef.pnodes[platform_id] = pn
                return pn

            def build_and_add_ports_to_node(ports, pn):
                for port_info in ports:
                    _require('port_id' in port_info)
                    port_id = port_info['port_id']
                    port = PortNode(port_id)
                    port.set_state(port_info.get('state', None))
                    if 'instruments' in port_info:
                        for instrument in port_info['instruments']:
                            instrument_id = instrument['instrument_id']
                            _require(not instrument_id in port.instrument_ids,
                                     'port_id=%r: duplicate instrument_id=%r' % (
                                     port_id, instrument_id))
                            port.add_instrument_id(instrument_id)
                    pn.add_port(port)

            def build_and_add_attrs_to_node(attrs, pn):
                for attr_defn in attrs:
                    attr_id = _get_attr_id(attr_defn)
                    _require('monitor_cycle_seconds' in attr_defn)
                    _require('units' in attr_defn)
                    if isinstance(attr_defn, OrderedDict):
                        attr_defn = dict(attr_defn)
                    pn.add_attribute(AttrNode(attr_id, attr_defn))

            def build_node(platObj, parent_node):
                _require('platform_id' in platObj)
                platform_id = platObj['platform_id']
                ports = platObj['ports'] if 'ports' in platObj else []
                attrs = platObj['attrs'] if 'attrs' in platObj else []
                pn = create_node(platform_id)
                parent_node.add_subplatform(pn)
                build_and_add_ports_to_node(ports, pn)
                build_and_add_attrs_to_node(attrs, pn)
                if 'subplatforms' in platObj:
                    for subplat in platObj['subplatforms']:
                        subplat_id = subplat['platform_id']
                        _require(not subplat_id in pn.subplatforms,
                                 '%s: duplicate subplatform ID for parent %s' % (
                                 subplat_id, platform_id))
                        build_node(subplat, pn)
                return pn

            ndef._pnodes = {}

            ndef._dummy_root = create_node(platform_id='')

            _require('network' in pyobj, "'network' undefined")
            for platObj in pyobj["network"]:
                build_node(platObj, ndef._dummy_root)

        pyobj = yaml.load(ser)
        _build_network(pyobj)

        return ndef

    @staticmethod
    def serialize_network_definition(ndef):
        """
        Returns a serialization of the given a NetworkDefinition object.

        @param ndef NetworkDefinition object
        @return string with the serialization
        """
        ser = "\n%s\n%s" % ("# (generated from PlatformNode object)",
                            NetworkUtil.serialize_pnode(ndef.root))

        return ser

    @staticmethod
    def serialize_pnode(pnode, level=0):
        """
        Returns a serialization of the given a PlatformNode object.

        @param pnode The PlatformNode to serialize
        @param level Indentation level (0 by default)
        @return string with the serialization
        """

        result = ""
        next_level = level
        if pnode.platform_id:
            pid = pnode.platform_id
            lines = []
            if level == 0:
                lines.append('network:')

            lines.append('- platform_id: %s' % pid)

            # attributes:
            if len(pnode.attrs):
                lines.append('  attrs:')
                for attr_id, attr in pnode.attrs.iteritems():
                    lines.append('  - attr_id: %s' % attr_id)
                    for k, v in attr.defn.iteritems():
                        if k != "attr_id":
                            lines.append('    %s: %s' % (k, v))

            # ports
            if len(pnode.ports):
                lines.append('  ports:')
                for port_id, port in pnode.ports.iteritems():
                    lines.append('  - port_id: %s' % port_id)

                    # instruments
                    if len(port.instrument_ids):
                        lines.append('    instruments:')
                        for instrument_id in port.instrument_ids:
                            lines.append('    - instrument_id: %s' % instrument_id)

            if pnode.subplatforms:
                lines.append('  subplatforms:')

            nl = "\n" + ("  " * level)
            result += nl + nl.join(lines)
            next_level = level + 1

        if pnode.subplatforms:
            for sub_platform in pnode.subplatforms.itervalues():
                result += NetworkUtil.serialize_pnode(sub_platform, next_level)

        return result

    @staticmethod
    def _dump_pnode(pnode, indent_level=0, only_topology=False,
                    include_subplatforms=True):  # pragma: no cover
        """
        **Developer routine**
        Indented string representation mainly for logging purposes.

        @param indent_level To create an indented string. 0 by default.
        @param only_topology True to only print the topology; False to also
               include attributes and ports. False by default.
        @param include_subplatforms True to also dump the subplatforms (with
               incremented indentation level). True by default.
        """
        s = ""
        indent = "    " * indent_level
        if pnode.platform_id:
            if only_topology:
                s = "%s%s\n" % (indent, pnode.platform_id)
            else:
                s = "%s%s\n" % (indent, str(pnode).replace('\n', '\n%s' % indent))
            indent_level += 1

        if include_subplatforms:
            for sub_platform in pnode.subplatforms.itervalues():
                s += NetworkUtil._dump_pnode(sub_platform, indent_level, only_topology)

        return s

    @staticmethod
    def _gen_diagram(pnode, style="dot", root=True):  # pragma: no cover
        """
        **Developer routine**
        String representation that can be processed by Graphviz tools or
        plantuml.

        @param pnode  PlatformNode
        @param style  one of "dot", "plantuml" (by default, "dot")
        @param root   True (the default) to include appropriate preamble
        """

        # for plantuml, use a simple "activity" diagram. The (*) will be for
        # the the node with platform_id == '' (aka the "dummy root")

        arrow = "-->" if style == "plantuml" else "->"
        body = ""
        if style == "plantuml" and pnode.platform_id == '':
            parent_str = "(*)"
        else:
            parent_str = pnode.platform_id

        if parent_str:
            for sub_platform in pnode.subplatforms.itervalues():
                body += '\t"%s" %s "%s"\n' % (parent_str,
                                          arrow,
                                          sub_platform.platform_id)

        for sub_platform in pnode.subplatforms.itervalues():
            body += NetworkUtil._gen_diagram(sub_platform, style=style, root=False)

        result = body
        if root:
            if style == "dot":
                result = 'digraph G {\n'
                if pnode.platform_id:
                    result += '\t"%s"\n' % pnode.platform_id
                result += '%s}\n' % body
            elif style == "plantuml":
                result = "%s\n" % body

        return result

    @staticmethod
    def _gen_and_open_diagram(pnode):  # pragma: no cover
        """
        **Developer routine**
        Convenience method for testing/debugging.
        Generates a yaml, a dot diagram and corresponding PNG using 'dot' and
        also opens the PNG using 'open' OS commands. All errors are simply ignored.
        """
        try:
            import os, tempfile, subprocess
            if os.getenv("GEN_DIAG", None) is None:
                return

            name = pnode.platform_id
            base_name = '%s/%s' % (tempfile.gettempdir(), name)
            yml_name = '%s.yml' % base_name
            dot_name = '%s.dot' % base_name
            png_name = '%s.png' % base_name
            print 'generating yml %r' % yml_name
            file(yml_name, 'w').write(NetworkUtil.serialize_pnode(pnode))
            print 'generating diagram %r' % dot_name
            file(dot_name, 'w').write(NetworkUtil._gen_diagram(pnode, style="dot"))
            dot_cmd = 'dot -Tpng %s -o %s' % (dot_name, png_name)
            print 'running: %s' % dot_cmd
            subprocess.call(dot_cmd.split())
            print 'opening %r' % png_name
            open_cmd = 'open %s' % png_name
            subprocess.call(open_cmd.split())
        except Exception, e:
            print "error generating or opening diagram: %s" % str(e)

    @staticmethod
    def create_network_definition_from_ci_config(CFG):
        """
        Creates a NetworkDefinition object by traversing the given CI agent
        configuration dictionary.

        @param CFG CI agent configuration
        @return A NetworkDefinition object

        @raise PlatformDefinitionException device_type is not 'PlatformDevice'
        """

        # verify CFG corresponds to PlatformDevice:
        device_type = CFG.get("device_type", None)
        _require('PlatformDevice' == device_type,
                 "Expecting device_type to be 'PlatformDevice'. Got %r" % device_type)

        ndef = NetworkDefinition()
        ndef._pnodes = {}

        def create_platform_node(platform_id, CFG=None):
            _require(not platform_id in ndef.pnodes,
                     "create_platform_node(): platform_id %r not in ndef.pnodes" % platform_id)
            pn = PlatformNode(platform_id, CFG)
            ndef.pnodes[platform_id] = pn
            return pn

        ndef._dummy_root = create_platform_node(platform_id='')

        def _add_attrs_to_platform_node(attrs, pn):
            for attr_defn in attrs:
                attr_id = _get_attr_id(attr_defn)
                _require('monitor_cycle_seconds' in attr_defn,
                         "_add_attrs_to_platform_node(): 'monitor_cycle_seconds' not in attr_defn")
                _require('units' in attr_defn,
                         "_add_attrs_to_platform_node(): 'units' not in attr_defn")
                pn.add_attribute(AttrNode(attr_id, attr_defn))

        def _add_ports_to_platform_node(ports, pn):
            for port_info in ports:
                _require('port_id' in port_info,
                         "_add_ports_to_platform_node(): 'port_id' not in port_info")
                port_id = port_info['port_id']
                port = PortNode(port_id)
                pn.add_port(port)

        def build_platform_node(CFG, parent_node):
            platform_config = CFG.get('platform_config', {})
            platform_id     = platform_config.get('platform_id', None)

            driver_config  = CFG.get('driver_config', {})
            attributes     = driver_config.get('attributes', {})
            ports          = driver_config.get('ports', {})

            _require(platform_id, "missing CFG.platform_config.platform_id")

            _require(driver_config, "missing CFG.driver_config")

            pn = create_platform_node(platform_id, CFG)
            parent_node.add_subplatform(pn)

            # attributes:
            _add_attrs_to_platform_node(attributes.itervalues(), pn)

            # ports:
            # TODO(OOIION-1495) the following was commented out,
            # but we need to capture the ports, at least under the current logic.
            # remove until network checkpoint needs are defined.
            # port info can be retrieve from active deployment
            _add_ports_to_platform_node(ports.itervalues(), pn)

            # children:
            for child_CFG in CFG.get("children", {}).itervalues():
                device_type = child_CFG.get("device_type", None)
                if device_type == 'PlatformDevice':
                    build_platform_node(child_CFG, pn)

                elif device_type == 'InstrumentDevice':
                    build_instrument_node(child_CFG, pn)

            return pn

        def build_instrument_node(CFG, parent_node):
            # use CFG.agent.resource_id as the instrument_id:
            agent = CFG.get('agent', {})
            instrument_id = agent.get('resource_id', None)

            _require(instrument_id, "missing CFG.agent.resource_id for instrument")

            inn = InstrumentNode(instrument_id, CFG=CFG)
            parent_node.add_instrument(inn)

        # Now, build the whole network:
        build_platform_node(CFG, ndef._dummy_root)

        return ndef


def _get_attr_id(attr_defn):
    if 'attr_id' in attr_defn:
        attr_id = attr_defn['attr_id']
    elif 'attr_name' in attr_defn and 'attr_instance' in attr_defn:
        attr_id = "%s|%s" % (attr_defn['attr_name'], attr_defn['attr_instance'])
    else:
        raise PlatformDefinitionException(
            "Attribute definition does now include 'attr_name' nor 'attr_instance'. "
            "attr_defn = %s" % attr_defn)

    return attr_id


def _require(cond, msg=""):
    if not cond:
        raise PlatformDefinitionException(msg)
