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


class BaseNode(object):
    """
    A convenient base class for the components of a platform network.
    """
    def __init__(self):
        # cached value for the checksum property.
        self._checksum = None

    def diff(self, other):
        """
        Returns None if this and the other object are the same.
        Otherwise, returns a message describing the first difference.
        """
        raise NotImplementedError()  # pragma: no cover

    @property
    def checksum(self):
        """
        Gets the last value computed by compute_checksum, if any.
        If no such value is available (for example, compute_checksum hasn't
        been called yet, or the cached checksum has been invalidated), then
        this method calls compute_checksum to obtain the checksum.

        @note Unless client code has good control about the changes done on
        instances of this class (including changes in children nodes), that is,
        by making sure to invalidate the corresponding cached values upon any
        changes, the use of this property is *not* recommended; instead call
        compute_checksum, which always compute the checksum based on the
        current state of the node and its children.

        @return SHA1 hash value as string of hexadecimal digits.
        """
        if not self._checksum:
            self._checksum = self.compute_checksum()
        return self._checksum

    def compute_checksum(self):
        """
        Computes the checksum for this object, updating the cached value for
        future calls to the checksum property. Subclasses do not need
        overwrite this method.

        @return SHA1 hash value as string of hexadecimal digits
        """
        self._checksum = self._compute_checksum()
        return self._checksum
        
    def _compute_checksum(self):
        """
        Subclasses implement this method to compute the checksum for
        this object. For any checksum computation of subcomponents,
        the implementation should call compute_checksum on the subcomponent.

        @return SHA1 hash value as string of hexadecimal digits
        """
        raise NotImplementedError()  # pragma: no cover


class AttrNode(BaseNode):
    """
    Represents a platform attribute.
    """
    def __init__(self, attr_id, defn):
        """
        OOIION-1551:
        First, get attr_name and attr_instance from the given attr_id (this is
        the preferred mechanism as it's expected to be of the form
        "<name>|<instance>") or from properties in defn, and resorting to
        the given attr_id for the name, and "0" for the instance.
        Finally, the store attr_id is composed from the name and instance as
        captured above.
        """
        BaseNode.__init__(self)
        idx = attr_id.rfind('|')
        if idx >= 0:
            self._attr_name     = attr_id[:idx]
            self._attr_instance = attr_id[idx + 1:]
        else:
            self._attr_name     = defn.get('attr_name', attr_id)
            self._attr_instance = defn.get('attr_instance', "0")

        self._attr_id = "%s|%s" % (self._attr_name, self._attr_instance)
        defn['attr_id'] = self._attr_id
        self._defn = defn

    def __repr__(self):
        return "AttrNode{id=%s, defn=%s}" % (self.attr_id, self.defn)

    @property
    def attr_name(self):
        return self._attr_name

    @property
    def attr_instance(self):
        return self._attr_instance

    @property
    def attr_id(self):
        return self._attr_id

    @property
    def defn(self):
        return self._defn

    @property
    def writable(self):
        return self.defn.get('read_write', '').lower().find("write") >= 0

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
            if key not in ["attr_name", "attr_instance", "attr_id"]:
                val = self.defn[key]
                hash_obj.update("%s=%s;" % (key, val))
    
        return hash_obj.hexdigest()
        

class PortNode(BaseNode):
    """
    Represents a platform port.

    self._port_id
    self._instruments = { instrument_id: InstrumentNode, ... }

    """
    def __init__(self, port_id):
        BaseNode.__init__(self)
        self._port_id = str(port_id)
        self._instruments = {}
        self._state = None

    def __repr__(self):
        return "PortNode{id=%s}" % (
            self._port_id)

    @property
    def port_id(self):
        return self._port_id

    @property
    def state(self):
        return self._state

    def set_state(self, state):
        self._state = state

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

    def remove_instrument(self, instrument_id):
        if instrument_id not in self._instruments:
            raise Exception('%s: Not such instrument ID' % instrument_id)
        del self._instruments[instrument_id]

    def diff(self, other):
        """
        Returns None if the two ports are the same.
        Otherwise, returns a message describing the first difference.
        """
        if self.port_id != other.port_id:
            return "Port IDs are different: %r != %r" % (
                self.port_id, other.port_id)

        if self.state != other.state:
            return "Port state values are different: %r != %r" % (
                self.state, other.state)

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

        # state:
        hash_obj.update("port_state=%s;" % self.state)

        # instruments:
        hash_obj.update("port_instruments:")
        for key in sorted(self.instruments.keys()):
            instrument = self.instruments[key]
            hash_obj.update(instrument.compute_checksum())

        return hash_obj.hexdigest()


class InstrumentNode(BaseNode):
    """
    Represents an instrument in a port.
    Note, also used directly in PlatformNode to capture the configuration for
    instruments.

    self._instrument_id
    self._attrs = { ... }
    self._CFG = dict

    The _CFG element included for convenience to capture the provided
    configuration dict.
    """
    def __init__(self, instrument_id, attrs=None, CFG=None):
        BaseNode.__init__(self)
        self._instrument_id = instrument_id
        self._attrs = attrs or {}
        self._CFG = CFG

    def __repr__(self):
        return "InstrumentNode{id=%s, attrs=%s}" % (
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

    @property
    def CFG(self):
        return self._CFG

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


class PlatformNode(BaseNode):
    """
    Platform node for purposes of representing the network.

    self._platform_id
    self._platform_types = [type, ...]
    self._attrs = { attr_id: AttrNode, ... }
    self._ports = { port_id: PortNode, ... }
    self._subplatforms = { platform_id: PlatformNode, ...}
    self._parent = None | PlatformNode
    self._instruments = { instrument_id: InstrumentNode, ...}
    self._CFG = dict

    The _CFG element included for convenience to capture the provided
    configuration dict in PlatformAgent. See
    create_network_definition_from_ci_config()

    NOTE: because _instruments is only to capture provided instrument
    configuration, this property is NOT taken into account for the
    _compute_checksum operation, which is intended for the current state of
    the attributes and ports (which also include instruments but in the sense
     of being connected to the port).

    """
    #TODO: some separation of configuration vs. state would be convenient.

    def __init__(self, platform_id, platform_types=None, CFG=None):
        BaseNode.__init__(self)
        self._platform_id = platform_id
        self._platform_types = platform_types or []
        self._name = None
        self._ports = {}
        self._attrs = {}
        self._subplatforms = {}
        self._parent = None
        self._instruments = {}
        self._CFG = CFG

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
        return self._ports[port_id]

    @property
    def CFG(self):
        return self._CFG

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

    @property
    def instruments(self):
        """
        Instruments configured for this platform.
        """
        return self._instruments

    def add_instrument(self, instrument):
        if instrument.instrument_id in self._instruments:
            raise Exception('%s: duplicate instrument ID' % instrument.instrument_id)
        self._instruments[instrument.instrument_id] = instrument

    def __str__(self):
        s = "<%s" % self.platform_id
        if self.name:
            s += "/name=%s" % self.name
        s += "/types=%s" % self.platform_types
        s += ">\n"
        s += "ports=%s\n"         % list(self.ports.itervalues())
        s += "attrs=%s\n"         % list(self.attrs.itervalues())
        s += "#subplatforms=%d\n" % len(self.subplatforms)
        s += "#instruments=%d\n"  % len(self.instruments)
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

    def diff(self, other):
        """
        Returns None if the two PlatformNode's represent the same topology and
        same attributes and ports.
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


class NetworkDefinition(BaseNode):
    """
    Represents a platform network definition in terms of platform types and
    topology, including attributes and ports associated with the platforms.

    See NetworkUtil for serialization/deserialization of objects of this type
    and other associated utilities.
    """

    def __init__(self):
        BaseNode.__init__(self)
        self._platform_types = {}
        self._pnodes = {}

        # _dummy_root is a dummy PlatformNode having as children the actual roots in
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
    def pnodes(self):
        """
        Returns a dict of all PlatformNodes in the network indexed by the platform ID.

        @return {platform_id : PlatformNode} map
        """
        return self._pnodes

    @property
    def root(self):
        """
        Returns the root PlatformNode. Can be None if there is no such root or
        there are multiple root PlatformNode's. The expected normal situation
        is to have single root.

        @return the root PlatformNode.
        """
        root = None
        if self._dummy_root and len(self._dummy_root.subplatforms) == 1:
            root = self._dummy_root.subplatforms.values()[0]
        return root

    def get_map(self):
        """
        Helper for getting the list of (platform_id, parent_platform_id) pairs.
        """
        return self._dummy_root.get_map([])

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

        # root PlatformNode:
        hash_obj.update("root_platform=%s;" % self.root.compute_checksum())

        return hash_obj.hexdigest()
