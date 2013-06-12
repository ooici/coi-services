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

# serialization/deserialization based on YAML
import yaml


class NetworkUtil(object):
    """
    Various utilities including creation of PlatformNode and NetworkDefinition objects.
    """

    @staticmethod
    def create_node_network(network_map):
        """
        Creates a node network according to the given map (this map is
        the format used by the CI-OMS interface to represent the topology).
        Various verifications are performed here resulting in an exception
        being thrown if the definition is invalid:
         - no duplicate platform_id
         - dummy root (with id '') is present
         - only one regular root node.

        @param network_map [(platform_id, parent_platform_id), ...]

        @return { platform_id: PlatformNode }

        @raise PlatformDefinitionException
        """
        pnodes = {}
        for platform_id, parent_platform_id in network_map:
            if not platform_id:
                raise PlatformDefinitionException(
                    "platform_id in tuple can not be %r" % platform_id)

            if parent_platform_id is None:
                parent_platform_id = ''

            if parent_platform_id in pnodes:
                parent = pnodes[parent_platform_id]
            else:
                parent = pnodes[parent_platform_id] = PlatformNode(parent_platform_id)

            if platform_id in pnodes:
                platform = pnodes[platform_id]
                previous_parent = platform.parent
            else:
                platform = pnodes[platform_id] = PlatformNode(platform_id)
                previous_parent = None

            if previous_parent is not None and previous_parent.platform_id != parent_platform_id:
                raise PlatformDefinitionException(
                    "Duplicate tuple for platform_id=%r but different "
                    "parent_platform_ids: %r and %r" % (
                        platform_id,
                        platform.parent.platform_id, parent_platform_id))

            if platform_id not in parent._subplatforms:
                parent.add_subplatform(platform)

        if not '' in pnodes:
            raise PlatformDefinitionException("Expecting dummy root in node network dict")
        dummy_root = pnodes['']
        if len(dummy_root.subplatforms) != 1:
            raise PlatformDefinitionException(
                "Expecting a single root in node network dict, but got %s" % (
                dummy_root.subplatforms))

        return pnodes

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
              - ndef._pnodes: {platform_id : PlatformNode} dict
            """
            assert 'network' in pyobj

            def create_node(platform_id, platform_types=None):
                assert not platform_id in ndef.pnodes
                pn = PlatformNode(platform_id, platform_types)
                ndef.pnodes[platform_id] = pn
                return pn

            def build_and_add_ports_to_node(ports, pn):
                for port_info in ports:
                    assert 'port_id' in port_info
                    assert 'network' in port_info
                    port_id = port_info['port_id']
                    network = port_info['network']
                    port = PortNode(port_id, network)
                    port.set_state(port_info.get('state', None))
                    if 'instruments' in port_info:
                        for instrument in port_info['instruments']:
                            instrument_id = instrument['instrument_id']
                            if instrument_id in port.instruments:
                                raise Exception('port_id=%r: duplicate instrument ID %r' % (
                                    port_id, instrument_id))
                            port.add_instrument(InstrumentNode(instrument_id))
                    pn.add_port(port)

            def build_and_add_attrs_to_node(attrs, pn):
                for attr_defn in attrs:
                    assert 'attr_id' in attr_defn
                    assert 'monitor_cycle_seconds' in attr_defn
                    assert 'units' in attr_defn
                    attr_id = attr_defn['attr_id']
                    pn.add_attribute(AttrNode(attr_id, attr_defn))

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
                build_and_add_ports_to_node(ports, pn)
                build_and_add_attrs_to_node(attrs, pn)
                if 'subplatforms' in platObj:
                    for subplat in platObj['subplatforms']:
                        subplat_id = subplat['platform_id']
                        if subplat_id in pn.subplatforms:
                            raise Exception('%s: duplicate subplatform ID for parent %s' % (
                                subplat_id, platform_id))
                        build_node(subplat, pn)
                return pn

            ndef._pnodes = {}

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
        ser += "\n%s" % NetworkUtil.serialize_pnode(ndef.root)

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
            lines.append('  platform_types: %s' % pnode.platform_types)

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
                    lines.append('    network: %s' % port.network)

                    # instruments
                    if len(port.instruments):
                        lines.append('    instruments:')
                        for instrument_id, instrument in port.instruments.iteritems():
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
    def _gen_open_diagram(pnode):  # pragma: no cover
        """
        **Developer routine**
        Convenience method for testing/debugging.
        Does nothing if the environment variable GEN_DIAG is not defined.
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
    def _gen_yaml(pnode, level=0):  # pragma: no cover
        """
        **Developer routine**
        This is old - can be deleted.
        Partial string representation of the given PlatformNode in yaml.
        *NOTE*: Very ad hoc, just to help capture some of the real info from
        the RSN OMS interface into network.yml (the file used by the simulator)
        along with values for testing purposes.
        """
        # TODO delete this method

        result = ""
        next_level = level
        if pnode.platform_id:
            pid = pnode.platform_id
            lines = []
            if level == 0:
                lines.append('network:')

            lines.append('- platform_id: %s' % pid)
            lines.append('  platform_types: []')

            lines.append('  attrs:')
            write_attr = False
            for i in range(2):
                read_write = "write" if write_attr else "read"
                write_attr = not write_attr

                # attr_id here is the "ref_id" in the CI-OMS interface spec
                attr_id = '%s_attr_%d' % (pid, i + 1)

                lines.append('  - attr_id: %s' % attr_id)
                lines.append('    type: int')
                lines.append('    units: xyz')
                lines.append('    min_val: -2')
                lines.append('    max_val: 10')
                lines.append('    read_write: %s' % read_write)
                lines.append('    group: power')
                lines.append('    monitor_cycle_seconds: 5')

            lines.append('  ports:')
            for i in range(2):
                port_id = '%s_port_%d' % (pid, i + 1)
                lines.append('  - port_id: %s' % port_id)
                lines.append('    ip: %s_IP' % port_id)

            if pnode.subplatforms:
                lines.append('  subplatforms:')

            nl = "\n" + ("  " * level)
            result += nl + nl.join(lines)
            next_level = level + 1

        if pnode.subplatforms:
            for sub_platform in pnode.subplatforms.itervalues():
                result += NetworkUtil._gen_yaml(sub_platform, next_level)

        return result

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
        if 'PlatformDevice' != device_type:
            raise PlatformDefinitionException("Expecting device_type to be "
                                              "'PlatformDevice'. Got %r" % device_type)

        ndef = NetworkDefinition()
        ndef._pnodes = {}

        def create_platform_node(platform_id, platform_types=None, CFG=None):
            assert not platform_id in ndef.pnodes
            pn = PlatformNode(platform_id, platform_types, CFG)
            ndef.pnodes[platform_id] = pn
            return pn

        ndef._dummy_root = create_platform_node(platform_id='')

        def _get_platform_types(CFG):
            """
            Constructs:
              - ndef._platform_types, {platform_type : description} dict
            """
            ndef._platform_types = {}
            #
            # TODO implement once this information is provided in the CI config

        _get_platform_types(CFG)

        def _add_attrs_to_platform_node(attrs, pn):
            for attr_defn in attrs:
                assert 'attr_id' in attr_defn
                assert 'monitor_cycle_seconds' in attr_defn
                assert 'units' in attr_defn
                attr_id = attr_defn['attr_id']
                pn.add_attribute(AttrNode(attr_id, attr_defn))

        def _add_ports_to_platform_node(ports, pn):
            for port_info in ports:
                assert 'port_id' in port_info
                assert 'network' in port_info
                port_id = port_info['port_id']
                network = port_info['network']
                port = PortNode(port_id, network)
                pn.add_port(port)

        def build_platform_node(CFG, parent_node):
            platform_config = CFG.get('platform_config', {})
            platform_id     = platform_config.get('platform_id', None)
            platform_types  = platform_config.get('platform_types', [])

            driver_config  = CFG.get('driver_config', {})
            attributes     = driver_config.get('attributes', {})
            ports          = driver_config.get('ports', {})

            if not platform_id:
                raise PlatformDefinitionException("missing CFG.platform_config.platform_id")

            if not driver_config:
                raise PlatformDefinitionException("missing CFG.driver_config")

            for platform_type in platform_types:
                if not platform_type in ndef._platform_types:
                    raise PlatformDefinitionException(
                        "%r not in defined platform types: %s" %(
                        platform_type, ndef._platform_types))

            pn = create_platform_node(platform_id, platform_types, CFG)
            parent_node.add_subplatform(pn)

            # attributes:
            _add_attrs_to_platform_node(attributes.itervalues(), pn)

            # ports:
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

            if not instrument_id:
                raise PlatformDefinitionException("missing CFG.agent.resource_id for instrument")

            inn = InstrumentNode(instrument_id, CFG=CFG)
            parent_node.add_instrument(inn)

        # Now, build the whole network:
        build_platform_node(CFG, ndef._dummy_root)

        return ndef
