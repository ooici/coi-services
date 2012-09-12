#!/usr/bin/env python

"""
@package ion.agents.platform.util.network
@file    ion/agents/platform/util/network.py
@author  Carlos Rueda
@brief   Supporting elements for representation of a platform network
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


# NOTE: No use of any pyon stuff mainly to facilitate use by simulator, which
# uses a regular threading.Thread, so we avoid gevent monkey-patching issues.

class Attr(object):
    """
    An attribute
    """
    def __init__(self, attr_id, defn):
        self._attr_id = attr_id
        self._defn = defn

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
    def value(self):
        return self._value


class Port(object):
    """
    A port in a platform node.
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

    def diagram(self, style="dot", root=True):
        """
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

    @staticmethod
    def create_network(map):
        """
        Creates a node network according to the given map.

        @param map [(platform_id, parent_platform_id), ...]

        @retval { platform_id: NNode }
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

    def diff_topology(self, other):
        """
        Returns None if the other node represents the same topology as this
        node. Otherwise, returns a message describing the first difference.
        """
        if self.platform_id != other.platform_id:
            return "platform IDs are different: %r != %r" % (
                self.platform_id, other.platform_id)

        subplatform_ids = set(self.subplatforms.iterkeys())
        other_subplatform_ids = set(other.subplatforms.iterkeys())
        if subplatform_ids != other_subplatform_ids:
            return "subplatform IDs are different: %r != %r" % (
                    subplatform_ids, other_subplatform_ids)

        for platform_id, node in self.subplatforms.iteritems():
            other_node = other.subplatforms[platform_id]
            diff = node.diff_topology(other_node)
            if diff:
                return diff

        return None
