#!/usr/bin/env python

"""
@package ion.agents.platform.util.network
@file    ion/agents/platform/util/network.py
@author  Carlos Rueda
@brief   Supporting elements for representation of a platform network
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


# NOTE: No use of any pyon stuff in this module mainly to also facilitate use by
# simulator, which uses a regular threading.Thread when run as a separate
# process, so we avoid gevent monkey-patching issues.

import hashlib


class NDefBase(object):
    """
    A convenient base class for the components of a network definition
    """
    def __init__(self):
        self._checksum = None

    def diff(self, other):
        """
        Returns None if this and the other object are the same.
        Otherwise, returns a message describing the first difference.
        """
        raise NotImplementedError()  #pragma: no cover

    @property
    def checksum(self):
        """
        Gets the last value computed by compute_checksum,
        which is called if it has not been called yet.

        @return SHA1 hash value as string of hexadecimal digits.
        """
        if not self._checksum:
            self._checksum = self.compute_checksum()
        return self._checksum

    def compute_checksum(self):
        """
        Computes the checksum for this object, updating the cached value for
        future calls to checksum().
        @return SHA1 hash value as string of hexadecimal digits
        """
        self._checksum = self._compute_checksum()
        return self._checksum
        
    def _compute_checksum(self):
        """
        Subclasses implement this method to compute the checksum for
        this object.
        @return SHA1 hash value as string of hexadecimal digits
        """
        raise NotImplementedError()  #pragma: no cover


class AttrDef(NDefBase):
    """
    Represents the definition of a platform attribute.
    """
    def __init__(self, attr_id, defn):
        NDefBase.__init__(self)
        self._attr_id = attr_id
        self._defn = defn

        self._writable = 'read_write' in defn and defn['read_write'].lower().find("write") >= 0

    def __repr__(self):
        return "AttrDef{id=%s, defn=%s}" % (self.attr_id, self.defn)

    @property
    def attr_id(self):
        return self._attr_id

    @property
    def defn(self):
        return self._defn

    @property
    def writable(self):
        return self._writable

    def diff(self, other):
        if self.attr_id != other.attr_id:
            return "Attribute IDs are different: %r != %r" % (
                self.attr_id, other.attr_id)

        if self.defn != other.defn:
            return "Attribute definitions are different: %r != %r" % (
                self.defn, other.defn)

        return None

    def _compute_checksum(self):
        hash_obj = hashlib.sha1()
    
        hash_obj.update("attribute_id=%s" % self.attr_id)
    
        # properties:
        hash_obj.update("attribute_properties:")
        for key in sorted(self.defn.keys()):
            val = self.defn[key]
            hash_obj.update("%s=%s;" % (key, val))
    
        return hash_obj.hexdigest()
        

class PortDef(NDefBase):
    """
    Represents the definition of a platform port.

    self._port_id
    self._network = value of the network associated to the port, eg., "10.30.78.x"
    self._instruments = { instrument_id: InstrumentDef, ... }

    """
    def __init__(self, port_id, network):
        NDefBase.__init__(self)
        self._port_id = port_id
        self._network = network
        self._instruments = {}

    def __repr__(self):
        return "PortDef{id=%s, network=%s}" % (
            self._port_id, self._network)

    @property
    def port_id(self):
        return self._port_id

    @property
    def network(self):
        return self._network

    @property
    def instruments(self):
        """
        Instruments of this port.
        """
        return self._instruments

    def add_instrument(self, instrument):
        if instrument.instrument_id in self._instruments:
            raise Exception('%s: duplicate instrument ID' % instrument.instrument_id)
        self._instruments[instrument.instrument_id] = instrument

    def diff(self, other):
        """
        Returns None if the two ports are the same.
        Otherwise, returns a message describing the first difference.
        """
        if self.port_id != other.port_id:
            return "Port IDs are different: %r != %r" % (
                self.port_id, other.port_id)

        if self.network != other.network:
            return "Port network values are different: %r != %r" % (
                self.network, other.network)

        # compare instruments:
        instrument_ids = set(self.instruments.iterkeys())
        other_instrument_ids = set(other.instruments.iterkeys())
        if instrument_ids != other_instrument_ids:
            return "port_id=%r: instrument IDs are different: %r != %r" % (
                self.port_id, instrument_ids, other_instrument_ids)
        for instrument_id, instrument in self.instruments.iteritems():
            other_instrument = other.instruments[instrument_id]
            diff = instrument.diff(other_instrument)
            if diff:
                return diff

        return None

    def _compute_checksum(self):
        hash_obj = hashlib.sha1()

        # id:
        hash_obj.update("port_id=%s;" % self.port_id)

        # network:
        hash_obj.update("port_network:")

        # instruments:
        hash_obj.update("port_instruments:")
        for key in sorted(self.instruments.keys()):
            instrument = self.instruments[key]
            hash_obj.update(instrument.compute_checksum())

        return hash_obj.hexdigest()


class InstrumentDef(NDefBase):
    """
    Represents the definition of an instrument.
    """
    def __init__(self, instrument_id):
        NDefBase.__init__(self)
        self._instrument_id = instrument_id
        self._attrs = {}

    def __repr__(self):
        return "InstrumentDef{id=%s, attrs=%s}" % (
            self.instrument_id, self.attrs)

    @property
    def instrument_id(self):
        return self._instrument_id

    @property
    def attrs(self):
        """
        Attributes of this instrument.
        """
        return self._attrs

    def diff(self, other):
        """
        Returns None if the two instruments are the same.
        Otherwise, returns a message describing the first difference.
        """
        if self.instrument_id != other.instrument_id:
            return "Instrument IDs are different: %r != %r" % (
                self.instrument_id, other.instrument_id)

        if self.attrs != other.attrs:
            return "Instrument attributes are different: %r != %r" % (
                self.attrs, other.attrs)

        return None

    def _compute_checksum(self):
        hash_obj = hashlib.sha1()

        hash_obj.update("instrument_id=%s;" % self.instrument_id)
        hash_obj.update("instrument_attributes:")
        for key in sorted(self.attrs.keys()):
            val = self.attrs[key]
            hash_obj.update("%s=%s;" % (key, val))

        return hash_obj.hexdigest()


class NNode(NDefBase):
    """
    Platform node for purposes of representing the network.

    self._platform_id
    self._platform_types = [type, ...]
    self._attrs = { attr_id: AttrDef, ... }
    self._ports = { port_id: PortDef, ... }
    self._subplatforms = { platform_id: NNode, ...}
    self._parent = None | NNode

    """

    def __init__(self, platform_id, platform_types=None):
        NDefBase.__init__(self)
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

    def add_attribute(self, attr):
        if attr.attr_id in self._attrs:
            raise Exception('%s: duplicate attribute ID' % attr.attr_id)
        self._attrs[attr.attr_id] = attr

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
        s += "ports=%s\n"         % list(self.ports.itervalues())
        s += "attrs=%s\n"         % list(self.attrs.itervalues())
        s += "#subplatforms=%d\n" % len(self.subplatforms)
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

    def dump(self, indent_level=0, only_topology=False,
             include_subplatforms=True):
        """
        Indented string representation mainly for logging purposes.

        @param indent_level To create an indented string. 0 by default.
        @param only_topology True to only print the topology; False to also
               include attributes and ports. False by default.
        @param include_subplatforms True to also dump the subplatforms (with
               incremented indentation level). True by default.
        """
        s = ""
        indent = "    " * indent_level
        if self.platform_id:
            if only_topology:
                s = "%s%s\n" % (indent, self.platform_id)
            else:
                s = "%s%s\n" % (indent, str(self).replace('\n', '\n%s' % indent))
            indent_level += 1

        if include_subplatforms:
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

        if self.platform_types != other.platform_types:
            return "platform types are different: %r != %r" % (
                self.platform_types, other.platform_types)

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

    def _compute_checksum(self):
        hash_obj = hashlib.sha1()

        # update with checksum of sub-platforms:
        hash_obj.update("subplatforms:")
        for key in sorted(self.subplatforms.keys()):
            subplatform = self.subplatforms[key]
            hash_obj.update(subplatform.compute_checksum())

        # now, with info about the platform itself.

        # id:
        hash_obj.update("platform_id=%s;" % self.platform_id)

        # platform_types:
        hash_obj.update("platform_types:")
        for platform_type in sorted(self.platform_types):
            hash_obj.update("%s;" % platform_type)

        # attributes:
        hash_obj.update("platform_attributes:")
        for key in sorted(self.attrs.keys()):
            attr = self.attrs[key]
            hash_obj.update(attr.compute_checksum())

        # ports:
        hash_obj.update("platform_ports:")
        for key in sorted(self.ports.keys()):
            port = self.ports[key]
            hash_obj.update(port.compute_checksum())

        return hash_obj.hexdigest()


class NetworkDefinition(NDefBase):
    """
    Represents a platform network definition in terms of platform types and
    topology, including attributes and ports associated with the platforms.

    See NetworkUtil for serialization/deserialization of objects of this type
    and other associated utilities.
    """

    def __init__(self):
        NDefBase.__init__(self)
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

    def _compute_checksum(self):
        hash_obj = hashlib.sha1()

        # platform_types:
        hash_obj.update("platform_types:")
        for key in sorted(self.platform_types.keys()):
            platform_type = self.platform_types[key]
            hash_obj.update("%s=%s;" % (key, platform_type))

        # root NNode:
        hash_obj.update("root_platform=%s;" % self.root.compute_checksum())

        return hash_obj.hexdigest()
