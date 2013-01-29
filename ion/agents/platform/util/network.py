#!/usr/bin/env python

"""
@package ion.agents.platform.util.network
@file    ion/agents/platform/util/network.py
@author  Carlos Rueda
@brief   Supporting elements for representation of a platform network
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


# NOTE: No use of any pyon stuff in this module mainly to facilitate use by
# simulator, which uses a regular threading.Thread when run as a separate
# process, so we avoid gevent monkey-patching issues.


class Attr(object):
    """
    Represents a platform attribute.
    """
    def __init__(self, attr_id, defn):
        self._attr_id = attr_id
        self._defn = defn

        self._writable = 'read_write' in defn and defn['read_write'].lower().find("write") >= 0

        self._value = defn['value'] if 'value' in defn else None

    def __repr__(self):
        return "Attr{id=%s, defn=%s, value=%s}" % (
            self.attr_id, self.defn, self.value)

    @property
    def attr_id(self):
        return self._attr_id

    @property
    def defn(self):
        return self._defn

    @property
    def writable(self):
        return self._writable

    @property
    def value(self):
        return self._value

    def diff(self, other):
        """
        Returns None if the two attributes are the same.
        Otherwise, returns a message describing the first difference.
        """
        if self.attr_id != other.attr_id:
            return "Attribute IDs are different: %r != %r" % (
                self.attr_id, other.attr_id)

        if self.defn != other.defn:
            return "Attribute definitions are different: %r != %r" % (
                self.defn, other.defn)

        return None


class Port(object):
    """
    Represents a platform port.
    """
    def __init__(self, port_id, ip):
        self._port_id = port_id
        self._comms = {'ip': ip}
        self._attrs = {}

        self._on = False

    def __repr__(self):
        return "Port{id=%s, comms=%s, attrs=%s}" % (
            self._port_id, self._comms, self._attrs)

    @property
    def port_id(self):
        return self._port_id

    @property
    def comms(self):
        return self._comms

    @property
    def attrs(self):
        return self._attrs

    def diff(self, other):
        """
        Returns None if the two ports are the same.
        Otherwise, returns a message describing the first difference.
        """
        if self.port_id != other.port_id:
            return "Port IDs are different: %r != %r" % (
                self.port_id, other.port_id)

        if self.comms != other.comms:
            return "Port comms are different: %r != %r" % (
                self.comms, other.comms)

        if self.attrs != other.attrs:
            return "Port attributes are different: %r != %r" % (
                self.attrs, other.attrs)

        return None


class NNode(object):
    """
    Platform node for purposes of representing the network.

    self._platform_id
    self._platform_types = [type, ...]
    self._attrs = { attr_id: Attr, ... }
    self._ports = { port_id: Port, ... }
    self._subplatforms = { platform_id: NNode, ...}
    self._parent = None | NNode

    """

    def __init__(self, platform_id, platform_types=None):
        self._platform_id = platform_id
        self._platform_types = platform_types or []
        self._name = None
        self._ports = {}
        self._attrs = {}
        self._subplatforms = {}
        self._parent = None

    def set_name(self, name):
        self._name = name

    def add_port(self, port):
        if port.port_id in self._ports:
            raise Exception('%s: duplicate port ID' % port.port_id)
        self._ports[port.port_id] = port

    def set_ports(self, ports):
        self._ports = {}
        for port_info in ports:
            assert 'port_id' in port_info
            assert 'ip' in port_info
            port_id = port_info['port_id']
            port_ip = port_info['ip']
            self.add_port(Port(port_id, port_ip))

    def add_attribute(self, attr):
        if attr.attr_id in self._attrs:
            raise Exception('%s: duplicate attribute ID' % attr.attr_id)
        self._attrs[attr.attr_id] = attr

    def set_attributes(self, attributes):
        self._attrs = {}
        for attr_defn in attributes:
            assert 'attr_id' in attr_defn
            assert 'monitorCycleSeconds' in attr_defn
            assert 'units' in attr_defn
            attr_id = attr_defn['attr_id']
            self.add_attribute(Attr(attr_id, attr_defn))

    @property
    def platform_id(self):
        return self._platform_id

    @property
    def platform_types(self):
        return self._platform_types

    @property
    def name(self):
        return self._name

    @property
    def ports(self):
        return self._ports

    @property
    def attrs(self):
        return self._attrs

    def get_port(self, port_id):
        assert port_id in self._ports
        return self._ports[port_id]

    @property
    def parent(self):
        return self._parent

    @property
    def subplatforms(self):
        return self._subplatforms

    def add_subplatform(self, pn):
        if pn.platform_id in self._subplatforms:
            raise Exception('%s: duplicate subplatform ID' % pn.platform_id)
        self._subplatforms[pn.platform_id] = pn
        pn._parent = self

    def __str__(self):
        s = "<%s" % self.platform_id
        if self.name:
            s += "/name=%s" % self.name
        s += "/types=%s" % self.platform_types
        s += ">\n"
        s += "ports=%s\n"      % list(self.ports.itervalues())
        s += "attrs=%s\n"      % list(self.attrs.itervalues())
        return s

    def get_map(self, pairs):
        """
        Helper for getting the list of (platform_id, parent_platform_id) pairs.
        """
        if self._parent:
            pairs.append((self.platform_id, self.parent.platform_id))
        for sub_platform in self.subplatforms.itervalues():
            sub_platform.get_map(pairs)
        return pairs

    def dump(self, indent_level=0, only_topology=False):
        """
        Indented string representation.
        """
        s = ""
        if self.platform_id:
            indent = "    " * indent_level
            if only_topology:
                s = "%s%s\n" % (indent, self.platform_id)
            else:
                s = "%s%s\n" % (indent, str(self).replace('\n', '\n%s' % indent))
            indent_level += 1

        for sub_platform in self.subplatforms.itervalues():
            s += sub_platform.dump(indent_level, only_topology)

        return s

    def diagram(self, style="dot", root=True):  # pragma: no cover
        """
        **Developer routine**

        String representation that can be processed by Graphviz tools or
        plantuml.

        @param root True (the default) to include appropriate preamble
        @param style one of "dot", "plantuml" (by default, "dot")
        """

        # for plantuml, use a simple "activity" diagram. The (*) will be for
        # the the node with platform_id == '' (aka the "dummy root")

        arrow = "-->" if style == "plantuml" else "->"
        body = ""
        if style == "plantuml" and self.platform_id == '':
            parent_str = "(*)"
        else:
            parent_str = self.platform_id

        if parent_str:
            for sub_platform in self.subplatforms.itervalues():
                body += '\t"%s" %s "%s"\n' % (parent_str,
                                          arrow,
                                          sub_platform.platform_id)

        for sub_platform in self.subplatforms.itervalues():
            body += sub_platform.diagram(style=style, root=False)

        result = body
        if root:
            if style == "dot":
                result = 'digraph G {\n'
                if self.platform_id:
                    result += '\t"%s"\n' % self.platform_id
                result += '%s}\n' % body
            elif style == "plantuml":
                result = "%s\n" % body

        return result

    def yaml(self, level=0):  # pragma: no cover
        """
        **Developer routine**

        Partial string representation in yaml.
        *NOTE*: Very ad hoc, just to help capture some of the real info from
        the RSN OMS interface into network.yml (the file used by the simulator)
        along with values for testing purposes.
        """

        result = ""
        next_level = level
        if self.platform_id:
            pid = self.platform_id
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
                lines.append('    monitorCycleSeconds: 5')

            lines.append('  ports:')
            for i in range(2):
                port_id = '%s_port_%d' % (pid, i + 1)
                lines.append('  - port_id: %s' % port_id)
                lines.append('    ip: %s_IP' % port_id)

            if self.subplatforms:
                lines.append('  subplatforms:')

            nl = "\n" + ("  " * level)
            result += nl + nl.join(lines)
            next_level = level + 1

        if self.subplatforms:
            for sub_platform in self.subplatforms.itervalues():
                result += sub_platform.yaml(next_level)

        return result

    def diff(self, other):
        """
        Returns None if the two nodes represent the same topology and same
        attributes and ports.
        Otherwise, returns a message describing the first difference.
        """
        if self.platform_id != other.platform_id:
            return "platform IDs are different: %r != %r" % (
                self.platform_id, other.platform_id)

        if self.name != other.name:
            return "platform names are different: %r != %r" % (
                self.name, other.name)

        # compare parents:
        if (self.parent is None) != (other.parent is None):
            return "platform parents are different: %r != %r" % (
                self.parent, other.parent)
        if self.parent is not None and self.parent.platform_id != other.parent.platform_id:
            return "platform parents are different: %r != %r" % (
                self.parent.platform_id, other.parent.platform_id)

        # compare attributes:
        attr_ids = set(self.attrs.iterkeys())
        other_attr_ids = set(other.attrs.iterkeys())
        if attr_ids != other_attr_ids:
            return "platform_id=%r: attribute IDs are different: %r != %r" % (
                self.platform_id, attr_ids, other_attr_ids)
        for attr_id, attr in self.attrs.iteritems():
            other_attr = other.attrs[attr_id]
            diff = attr.diff(other_attr)
            if diff:
                return diff

        # compare ports:
        port_ids = set(self.ports.iterkeys())
        other_port_ids = set(other.ports.iterkeys())
        if port_ids != other_port_ids:
            return "platform_id=%r: port IDs are different: %r != %r" % (
                self.platform_id, port_ids, other_port_ids)
        for port_id, port in self.ports.iteritems():
            other_port = other.ports[port_id]
            diff = port.diff(other_port)
            if diff:
                return diff

        # compare sub-platforms:
        subplatform_ids = set(self.subplatforms.iterkeys())
        other_subplatform_ids = set(other.subplatforms.iterkeys())
        if subplatform_ids != other_subplatform_ids:
            return "platform_id=%r: subplatform IDs are different: %r != %r" % (
                    self.platform_id,
                    subplatform_ids, other_subplatform_ids)

        for platform_id, node in self.subplatforms.iteritems():
            other_node = other.subplatforms[platform_id]
            diff = node.diff(other_node)
            if diff:
                return diff

        return None


class NetworkDefinition(object):
    """
    Represents a platform network definition in terms of platform types and
    topology, including attributes and ports associated with the platforms.

    See NetworkUtil for serialization/deserialization of objects of this type
    and other associated utilities.
    """

    def __init__(self):
        self._platform_types = {}
        self._nodes = {}

        # _dummy_root is a dummy NNode having as children the actual roots in
        # the network.
        self._dummy_root = None

    @property
    def platform_types(self):
        """
        Returns the platform types in the network.

        @return {platform_type : description} dict
        """
        return self._platform_types

    @property
    def nodes(self):
        """
        Returns a dict of all NNodes in the network indexed by the platform ID.

        @return {platform_id : NNode} map
        """
        return self._nodes

    @property
    def root(self):
        """
        Returns the root NNode. Can be None if there is no such root or there
        are multiple root nodes. The expected normal situation is to have
        single root.

        @return the root NNode.
        """
        root = None
        if self._dummy_root and len(self._dummy_root.subplatforms) == 1:
            root = self._dummy_root.subplatforms.values()[0]
        return root

    def diff(self, other):
        """
        Returns None if the two objects represent the same network definition.
        Otherwise, returns a message describing the first difference.
        """

        # compare platform_type definitions:
        if set(self.platform_types.items()) != set(other.platform_types.items()):
            return "platform types are different: %r != %r" % (
                self.platform_types, other.platform_types)

        # compare topology
        if (self.root is None) != (other.root is None):
            return "roots are different: %r != %r" % (
                self.root, other.root)
        if self.root is not None:
            return self.root.diff(other.root)
        else:
            return None
