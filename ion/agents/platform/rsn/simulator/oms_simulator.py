#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.oms_simulator
@file    ion/agents/platform/rsn/simulator/oms_simulator.py
@author  Carlos Rueda
@brief   OMS simulator
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.agents.platform.rsn.oms_client import CIOMSClient
from ion.agents.platform.rsn.oms_client import REQUIRED_INSTRUMENT_ATTRIBUTES
from ion.agents.platform.responses import NormalResponse, InvalidResponse
from ion.agents.platform.util.network import InstrumentNode
from ion.agents.platform.util.network_util import NetworkUtil

from ion.agents.platform.rsn.simulator.oms_events import EventInfo
from ion.agents.platform.rsn.simulator.oms_events import EventNotifier
from ion.agents.platform.rsn.simulator.oms_events import EventGenerator
from ion.agents.platform.rsn.simulator.oms_values import generate_values

import time
import ntplib

from ion.agents.platform.rsn.simulator.logger import Logger
log = Logger.get_logger()


class CIOMSSimulator(CIOMSClient):
    """
    Implementation of CIOMSClient for testing purposes.
    """

    # see start_raising_exceptions_at
    _exception_time = 0

    @classmethod
    def start_raising_exceptions_at(cls, at_time):
        """
        After the given time (if positive), all methods in any created
        instance of this class will start raising Exceptions. This allows to
        test for the "lost connection" case in the platform agent and driver.
        """
        secs = at_time - time.time()
        log.debug("synthetic exceptions starting in %s secs from now", secs)
        cls._exception_time = at_time

    def __init__(self, yaml_filename='ion/agents/platform/rsn/simulator/network.yml'):
        self._ndef = NetworkUtil.deserialize_network_definition(file(yaml_filename))
        self._platform_types = self._ndef.platform_types
        self._pnodes = self._ndef.pnodes

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

    def _dispatch_synthetic_exception(self):
        """
        Called by all CI_OMS interface methods to dispatch the
        simulation of connection lost.
        """
        if 0 < self._exception_time <= time.time():
            msg = "synthetic exception from CIOMSSimulator"
            log.debug(msg)
            raise Exception(msg)

    def ping(self):
        self._dispatch_synthetic_exception()

        return "pong"

    def get_platform_map(self):
        self._dispatch_synthetic_exception()

        return self._ndef.get_map()

    def get_platform_types(self):
        self._dispatch_synthetic_exception()

        return self._platform_types

    def get_platform_metadata(self, platform_id):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        pnode = self._pnodes[platform_id]

        # TODO capture/include appropriate elements
        md = {}
        if pnode.name:
            md['name'] = pnode.name
        if pnode.parent:
            md['parent_platform_id'] = pnode.parent.platform_id
        md['platform_types'] = pnode.platform_types

        return {platform_id: md}

    def get_platform_attributes(self, platform_id):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        attrs = self._pnodes[platform_id].attrs
        ret_infos = {}
        for attrName in attrs:
            attr = attrs[attrName]
            ret_infos[attrName] = attr.defn

        return {platform_id: ret_infos}

    def get_platform_attribute_values(self, platform_id, req_attrs):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        # complete time window until current time:
        to_time = ntplib.system_to_ntp_time(time.time())
        attrs = self._pnodes[platform_id].attrs
        vals = {}
        for attrName, from_time in req_attrs:
            if attrName in attrs:
                attr = attrs[attrName]
                values = generate_values(platform_id, attr.attr_id, from_time, to_time)
                vals[attrName] = values
                # Note: values == [] if there are no values.
            else:
                vals[attrName] = InvalidResponse.ATTRIBUTE_NAME

        return {platform_id: vals}

    def set_platform_attribute_values(self, platform_id, input_attrs):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        assert isinstance(input_attrs, list)

        timestamp = ntplib.system_to_ntp_time(time.time())
        attrs = self._pnodes[platform_id].attrs
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
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        ports = {}
        for port_id, port in self._pnodes[platform_id].ports.iteritems():
            ports[port_id] = {'network': port.network,
                              'is_on':   port.is_on}

        return {platform_id: ports}

    def connect_instrument(self, platform_id, port_id, instrument_id, attributes):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)

        result = None
        if instrument_id in port.instruments:
            result = InvalidResponse.INSTRUMENT_ALREADY_CONNECTED
        elif port.is_on:
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
            connected_instrument = InstrumentNode(instrument_id)
            port.add_instrument(connected_instrument)
            attrs = connected_instrument.attrs
            result = {}
            for key, val in attributes.iteritems():
                attrs[key] = val  # set the value of the attribute:
                result[key] = val # in the result, indicate that the value was set

        return {platform_id: {port_id: {instrument_id: result}}}

    def disconnect_instrument(self, platform_id, port_id, instrument_id):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)

        if instrument_id not in port.instruments:
            result = InvalidResponse.INSTRUMENT_NOT_CONNECTED
        elif port.is_on:
            # TODO: confirm that port must be OFF so instrument can be disconnected
            result = InvalidResponse.PORT_IS_ON
        else:
            port.remove_instrument(instrument_id)
            result = NormalResponse.INSTRUMENT_DISCONNECTED

        return {platform_id: {port_id: {instrument_id: result}}}

    def get_connected_instruments(self, platform_id, port_id):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)

        result = {}
        for instrument_id in port.instruments:
            result[instrument_id] = port.instruments[instrument_id].attrs

        return {platform_id: {port_id: result}}

    def turn_on_platform_port(self, platform_id, port_id):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)
        if port.is_on:
            result = NormalResponse.PORT_ALREADY_ON
            log.warn("port %s in platform %s already turned on." % (port_id, platform_id))
        else:
            port.set_on(True)
            result = NormalResponse.PORT_TURNED_ON
            log.info("port %s in platform %s turned on." % (port_id, platform_id))

        return {platform_id: {port_id: result}}

    def turn_off_platform_port(self, platform_id, port_id):
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)
        if not port.is_on:
            result = NormalResponse.PORT_ALREADY_OFF
            log.warn("port %s in platform %s already turned off." % (port_id, platform_id))
        else:
            port.set_on(False)
            result = NormalResponse.PORT_TURNED_OFF
            log.info("port %s in platform %s turned off." % (port_id, platform_id))

        return {platform_id: {port_id: result}}

    def describe_event_types(self, event_type_ids):
        self._dispatch_synthetic_exception()

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
        self._dispatch_synthetic_exception()

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
        self._dispatch_synthetic_exception()

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
        self._dispatch_synthetic_exception()

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
        self._dispatch_synthetic_exception()

        return self._reg_event_listeners

    def get_checksum(self, platform_id):
        """
        @note the checksum is always computed, which is fine for the simulator.
        A more realistic and presumably more efficient implementation would
        exploit some caching mechanism along with appropriate invalidation
        upon modifications to the platform information.
        """
        self._dispatch_synthetic_exception()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        pnode = self._pnodes[platform_id]
        checksum = pnode.compute_checksum()

        return {platform_id: checksum}
