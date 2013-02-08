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
from ion.agents.platform.oms.oms_client import REQUIRED_INSTRUMENT_ATTRIBUTES
from ion.agents.platform.oms.oms_client import NormalResponse
from ion.agents.platform.oms.oms_client import InvalidResponse
from ion.agents.platform.util.network import NNode
from ion.agents.platform.util.network import AttrDef
from ion.agents.platform.util.network import PortDef
from ion.agents.platform.util.network import InstrumentDef

from ion.agents.platform.oms.simulator.oms_events import EventInfo
from ion.agents.platform.oms.simulator.oms_events import EventNotifier
from ion.agents.platform.oms.simulator.oms_events import EventGenerator
from ion.agents.platform.oms.simulator.oms_values import generate_values

import yaml
import time
import ntplib

from ion.agents.platform.oms.simulator.logger import Logger
log = Logger.get_logger()


# search for "dynamic state" for places where we maintain dynamic state
# associated with the platform network.


class OmsSimulator(OmsClient):
    """
    Implementation of OmsClient for testing purposes.
    """

    def __init__(self, yaml_filename='ion/agents/platform/oms/simulator/network.yml'):
        pyobj = yaml.load(file(yaml_filename))

        self._get_platform_types(pyobj)

        self._build_network(pyobj)

        # registered event listeners: {url: [(event_type, reg_time), ...], ...},
        # where reg_time is the NTP time of (latest) registration.
        # NOTE: for simplicity, we don't keep info about unregistered listeners
        self._reg_event_listeners = {}

        self._event_notifier = EventNotifier()
        # EventGenerator only kept while there are listeners registered
        self._event_generator = None

    def _start_event_generator_if_listeners(self):
        if not self._event_generator and len(self._reg_event_listeners):
            self._event_generator = EventGenerator(self._event_notifier)
            self._event_generator.start()
            log.debug("event generator started (%s listeners registered)",
                      len(self._reg_event_listeners))

    def _stop_event_generator_if_no_listeners(self):
        if self._event_generator and not len(self._reg_event_listeners):
            log.debug("event generator stopping (no listeners registered)")
            self._event_generator.stop()
            self._event_generator = None

    def _deactivate_simulator(self):
        """
        Special method only intended to be called for when the simulator is run
        in "embedded" form. See test_oms_simulator for the particular case.
        """
        log.info("_deactivate_simulator called. event_generator=%s; %s listeners registered",
                 self._event_generator, len(self._reg_event_listeners))
        if self._event_generator:
            self._event_generator.stop()
            self._event_generator = None

    def _get_platform_types(self, pyobj):
        """
        Constructs:
          - self._platform_types: {platform_type : description} map
        """
        assert 'platform_types' in pyobj
        self._platform_types = {}
        for ptypeObj in pyobj["platform_types"]:
            assert 'platform_type' in ptypeObj
            assert 'description' in ptypeObj
            platform_type = ptypeObj['platform_type']
            description = ptypeObj['description']
            self._platform_types[platform_type] = description


    def _build_network(self, pyobj):
        """
        Constructs:
          - self._idp: {platform_id : NNode} map
          - self._dummy_root: The "dummy" root node; its children are the actual roots.
        """
        assert 'network' in pyobj
        self._idp = {}
        self._dummy_root = None

        def create_node(platform_id, platform_types=None):
            assert not platform_id in self._idp
            pn = NNode(platform_id, platform_types)
            self._idp[platform_id] = pn
            return pn

        def build_and_add_ports_to_node(ports, pn):
            for port_info in ports:
                assert 'port_id' in port_info
                assert 'ip' in port_info
                port_id = port_info['port_id']
                port_ip = port_info['ip']
                port = PortDef(port_id, port_ip)
                if 'instruments' in port_info:
                    for instrument in port_info['instruments']:
                        instrument_id = instrument['instrument_id']
                        if instrument_id in port.instruments:
                            raise Exception('port_id=%r: duplicate instrument ID %r' % (
                                port_id, instrument_id))
                        port.add_instrument(InstrumentDef(instrument_id))
                pn.add_port(port)

                ######################################################
                # dynamic state (note that these members are NOT part of the
                # PortDef class definition, but added here for convenience):

                # is port turned on?
                port._is_on = False  # is port turned on?

                # the set of instrument_ids added to the port
                port._added_instruments = set()

        def build_and_add_attrs_to_node(attrs, pn):
            for attr_defn in attrs:
                assert 'attr_id' in attr_defn
                assert 'monitorCycleSeconds' in attr_defn
                assert 'units' in attr_defn
                attr_id = attr_defn['attr_id']
                pn.add_attribute(AttrDef(attr_id, attr_defn))

        def build_node(platObj, parent_node):
            assert 'platform_id' in platObj
            assert 'platform_types' in platObj
            platform_id = platObj['platform_id']
            platform_types = platObj['platform_types']
            for platform_type in platform_types:
                assert platform_type in self._platform_types
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

        self._idp.clear()
        self._dummy_root = create_node(platform_id='')

        for platObj in pyobj["network"]:
            build_node(platObj, self._dummy_root)

    def ping(self):
        return "pong"

    def get_platform_map(self):
        return self._dummy_root.get_map([])

    def get_platform_types(self):
        return self._platform_types

    def get_platform_metadata(self, platform_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        nnode = self._idp[platform_id]

        # TODO capture/include appropriate elements
        md = {}
        if nnode.name:
            md['name'] = nnode.name
        if nnode.parent:
            md['parent_platform_id'] = nnode.parent.platform_id
        md['platform_types'] = nnode.platform_types

        return {platform_id: md}

    def get_platform_attributes(self, platform_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        attrs = self._idp[platform_id].attrs
        ret_infos = {}
        for attrName in attrs:
            attr = attrs[attrName]
            ret_infos[attrName] = attr.defn

        return {platform_id: ret_infos}

    def dump(self):
        """string representation of the network"""
        return "platform_types: %s\nnetwork:\n%s" % (
            self._platform_types, self._dummy_root.dump())

    def get_platform_attribute_values(self, platform_id, attrNames, from_time):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        # complete time window until current time:
        to_time = ntplib.system_to_ntp_time(time.time())
        attrs = self._idp[platform_id].attrs
        vals = {}
        for attrName in attrNames:
            if attrName in attrs:
                attr = attrs[attrName]
                values = generate_values(platform_id, attr.attr_id, from_time, to_time)
                vals[attrName] = values
                # Note: values == [] if there are no values.
            else:
                vals[attrName] = InvalidResponse.ATTRIBUTE_NAME

        return {platform_id: vals}

    def set_platform_attribute_values(self, platform_id, input_attrs):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        assert isinstance(input_attrs, list)

        timestamp = ntplib.system_to_ntp_time(time.time())
        attrs = self._idp[platform_id].attrs
        vals = {}
        for (attrName, attrValue) in input_attrs:
            if attrName in attrs:
                attr = attrs[attrName]
                if attr.writable:
                    #
                    # TODO check given attrValue
                    #
                    vals[attrName] = (attrValue, timestamp)
                else:
                    vals[attrName] = InvalidResponse.ATTRIBUTE_NOT_WRITABLE
            else:
                vals[attrName] = InvalidResponse.ATTRIBUTE_NAME

        retval = {platform_id: vals}
        log.debug("set_platform_attribute_values returning: %s", str(retval))
        return retval

    def get_platform_ports(self, platform_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        ports = {}
        for port_id, port in self._idp[platform_id].ports.iteritems():
            ports[port_id] = {'comms': port.comms, 'attrs': port.attrs}

        return {platform_id: ports}

    def connect_instrument(self, platform_id, port_id, instrument_id, attributes):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._idp[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._idp[platform_id].get_port(port_id)

        result = None
        if instrument_id not in port.instruments:
            result = InvalidResponse.INSTRUMENT_ID
        elif instrument_id in port._added_instruments:
            result = InvalidResponse.INSTRUMENT_ALREADY_CONNECTED
        elif port._is_on:
            # TODO: confirm that port must be OFF so instrument can be connected
            result = InvalidResponse.PORT_IS_ON

        if result is None:
            # verify required attributes are provided:
            for key in REQUIRED_INSTRUMENT_ATTRIBUTES:
                if not key in attributes:
                    result = InvalidResponse.MISSING_INSTRUMENT_ATTRIBUTE
                    log.warn("connect_instrument called with missing attribute: %s"% key)
                    break

        if result is None:
            # verify given attributes are recognized:
            for key in attributes.iterkeys():
                if not key in REQUIRED_INSTRUMENT_ATTRIBUTES:
                    result = InvalidResponse.INVALID_INSTRUMENT_ATTRIBUTE
                    log.warn("connect_instrument called with invalid attribute: %s"% key)
                    break

        if result is None:
            # NOTE: values simply accepted without any validation
            attrs = port.instruments[instrument_id].attrs
            result = {}
            for key, val in attributes.iteritems():
                assert key in REQUIRED_INSTRUMENT_ATTRIBUTES
                attrs[key] = val  # set the value of the attribute:
                result[key] = val # in the result, indicate that the value was set

            port._added_instruments.add(instrument_id)

        return {platform_id: {port_id: {instrument_id: result}}}

    def disconnect_instrument(self, platform_id, port_id, instrument_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._idp[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._idp[platform_id].get_port(port_id)

        if instrument_id not in port.instruments:
            result = InvalidResponse.INSTRUMENT_ID
        elif instrument_id not in port._added_instruments:
            result = InvalidResponse.INSTRUMENT_NOT_CONNECTED
        elif port._is_on:
            # TODO: confirm that port must be OFF so instrument can be disconnected
            result = InvalidResponse.PORT_IS_ON
        else:
            port._added_instruments.remove(instrument_id)
            result = NormalResponse.INSTRUMENT_DISCONNECTED

        return {platform_id: {port_id: {instrument_id: result}}}

    def get_connected_instruments(self, platform_id, port_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._idp[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._idp[platform_id].get_port(port_id)

        result = {}
        for instrument_id in port._added_instruments:
            result[instrument_id] = port.instruments[instrument_id].attrs

        return {platform_id: {port_id: result}}

    def turn_on_platform_port(self, platform_id, port_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._idp[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._idp[platform_id].get_port(port_id)
        if port._is_on:
            result = NormalResponse.PORT_ALREADY_ON
            log.warn("port %s in platform %s already turned on." % (port_id, platform_id))
        else:
            port._is_on = True
            result = NormalResponse.PORT_TURNED_ON
            log.info("port %s in platform %s turned on." % (port_id, platform_id))

        return {platform_id: {port_id: result}}

    def turn_off_platform_port(self, platform_id, port_id):
        if platform_id not in self._idp:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._idp[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._idp[platform_id].get_port(port_id)
        if not port._is_on:
            result = NormalResponse.PORT_ALREADY_OFF
            log.warn("port %s in platform %s already turned off." % (port_id, platform_id))
        else:
            port._is_on = False
            result = NormalResponse.PORT_TURNED_OFF
            log.info("port %s in platform %s turned off." % (port_id, platform_id))

        return {platform_id: {port_id: result}}

    def describe_event_types(self, event_type_ids):
        if len(event_type_ids) == 0:
            return EventInfo.EVENT_TYPES

        result = {}
        for k in event_type_ids:
            if not k in EventInfo.EVENT_TYPES:
                result[k] = InvalidResponse.EVENT_TYPE
            else:
                result[k] = EventInfo.EVENT_TYPES[k]

        return result

    def get_events_by_platform_type(self, platform_types):
        if len(platform_types) == 0:
            platform_types = self._platform_types.keys()

        result = {}
        for platform_type in platform_types:
            if not platform_type in self._platform_types:
                result[platform_type] = InvalidResponse.PLATFORM_TYPE
                continue

            result[platform_type] = [v for v in EventInfo.EVENT_TYPES.itervalues() \
                if v['platform_type'] == platform_type]

        return result

    def _validate_event_listener_url(self, url):
        """
        Does a basic, static validation of the url.
        """
        # TODO implement it; for now always returning True
        return True

    def register_event_listener(self, url, event_types):
        log.debug("register_event_listener called: url=%r, event_types=%s",
                 url, str(event_types))

        if not self._validate_event_listener_url(url):
            return {url: InvalidResponse.EVENT_LISTENER_URL}

        if not url in self._reg_event_listeners:
            # create entry for this new url
            existing_pairs = self._reg_event_listeners[url] = []
        else:
            existing_pairs = self._reg_event_listeners[url]

        if len(existing_pairs):
            existing_types, reg_times = zip(*existing_pairs)
        else:
            existing_types = reg_times = []

        if len(event_types) == 0:
            event_types = list(EventInfo.EVENT_TYPES.keys())

        result_list = []
        for event_type in event_types:
            if not event_type in EventInfo.EVENT_TYPES:
                result_list.append((event_type, InvalidResponse.EVENT_TYPE))
                continue

            if event_type in existing_types:
                # already registered:
                reg_time = reg_times[existing_types.index(event_type)]
                result_list.append((event_type, reg_time))
            else:
                #
                # new registration
                #
                reg_time = self._event_notifier.add_listener(url, event_type)
                existing_pairs.append((event_type, reg_time))
                result_list.append((event_type, reg_time))

                log.info("%r registered for event_type=%r", url, event_type)

        self._start_event_generator_if_listeners()

        return {url: result_list}

    def unregister_event_listener(self, url, event_types):
        log.debug("unregister_event_listener called: url=%r, event_types=%s",
                 url, str(event_types))

        if not url in self._reg_event_listeners:
            return {url: InvalidResponse.EVENT_LISTENER_URL}

        existing_pairs = self._reg_event_listeners[url]

        assert len(existing_pairs), "we don't keep any url with empty list"

        existing_types, reg_times = zip(*existing_pairs)

        if len(event_types) == 0:
            event_types = list(EventInfo.EVENT_TYPES.keys())

        result_list = []
        for event_type in event_types:
            if not event_type in EventInfo.EVENT_TYPES:
                result_list.append((event_type, InvalidResponse.EVENT_TYPE))
                continue

            if event_type in existing_types:
                #
                # registered, so remove it
                #
                unreg_time = self._event_notifier.remove_listener(url, event_type)
                idx = existing_types.index(event_type)
                del existing_pairs[idx]
                result_list.append((event_type, unreg_time))

                # update for next iteration (index for next proper removal):
                if len(existing_pairs):
                    existing_types, reg_times = zip(*existing_pairs)
                else:
                    existing_types = reg_times = []

                log.info("%r unregistered for event_type=%r", url, event_type)

            else:
                # not registered, report 0
                unreg_time = 0
                result_list.append((event_type, unreg_time))

        if len(existing_pairs):
            # reflect the updates:
            self._reg_event_listeners[url] = existing_pairs
        else:
            # we don't keep any url with empty list
            del self._reg_event_listeners[url]

        self._stop_event_generator_if_no_listeners()

        return {url: result_list}

    def get_registered_event_listeners(self):
        return self._reg_event_listeners
