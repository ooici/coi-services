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

###########################################################################
# NOTE: several operations related with "event types" were removed/adjusted
# from the CI-OMS interface per April/2013 telecons.
###########################################################################


class CIOMSSimulator(CIOMSClient):
    """
    Implementation of CIOMSClient for testing purposes.
    It adds some methods intended to be used by tests (they are prefixed with
    "x_" and are "public" to make them visible through the xml/rpc mechanism).
    """

    # _raise_exception: see disable() and enable()
    _raise_exception = False

    @classmethod
    def x_disable(cls):
        """
        Makes any subsequent call to any public API operation to raise an
        exception. This allows to test for the "lost connection" case.
        """
        cls._raise_exception = True

    @classmethod
    def x_enable(cls):
        """
        Cancels the effect of disable() (so the simulator continues to
        operate normally).
        """
        cls._raise_exception = False

    def __init__(self, yaml_filename='ion/agents/platform/rsn/simulator/network.yml'):
        self._ndef = NetworkUtil.deserialize_network_definition(file(yaml_filename))
        self._platform_types = self._ndef.platform_types
        self._pnodes = self._ndef.pnodes

        # registered event listeners: {url: reg_time, ...},
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

    def _enter(self):
        """
        Called when entering any of the CI-OMS interface methods.
        """
        self._dispatch_synthetic_exception()

    def _dispatch_synthetic_exception(self):
        """
        Called by all CI_OMS interface methods to dispatch the
        simulation of connection lost.
        """
        if self._raise_exception:
            msg = "(LC) synthetic exception from CIOMSSimulator"
            log.debug(msg)
            raise Exception(msg)

    def ping(self):
        self._enter()

        return "pong"

    def get_platform_map(self):
        self._enter()

        return self._ndef.get_map()

    def get_platform_types(self):
        self._enter()

        return self._platform_types

    def get_platform_metadata(self, platform_id):
        self._enter()

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
        self._enter()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        attrs = self._pnodes[platform_id].attrs
        ret_infos = {}
        for attrName in attrs:
            attr = attrs[attrName]
            ret_infos[attrName] = attr.defn

        return {platform_id: ret_infos}

    def get_platform_attribute_values(self, platform_id, req_attrs):
        self._enter()

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
                vals[attrName] = InvalidResponse.ATTRIBUTE_ID

        return {platform_id: vals}

    def set_platform_attribute_values(self, platform_id, input_attrs):
        self._enter()

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
                vals[attrName] = InvalidResponse.ATTRIBUTE_ID

        retval = {platform_id: vals}
        log.debug("set_platform_attribute_values returning: %s", str(retval))
        return retval

    def get_platform_ports(self, platform_id):
        self._enter()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        ports = {}
        for port_id, port in self._pnodes[platform_id].ports.iteritems():
            ports[port_id] = {'state'  : port.state}

        return {platform_id: ports}

    def connect_instrument(self, platform_id, port_id, instrument_id, attributes):
        self._enter()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)

        result = None
        if instrument_id in port.instruments:
            result = InvalidResponse.INSTRUMENT_ALREADY_CONNECTED
        elif port.state == "ON":
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
        self._enter()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)

        if instrument_id not in port.instruments:
            result = InvalidResponse.INSTRUMENT_NOT_CONNECTED
        elif port.state == "ON":
            # TODO: confirm that port must be OFF so instrument can be disconnected
            result = InvalidResponse.PORT_IS_ON
        else:
            port.remove_instrument(instrument_id)
            result = NormalResponse.INSTRUMENT_DISCONNECTED

        return {platform_id: {port_id: {instrument_id: result}}}

    def get_connected_instruments(self, platform_id, port_id):
        self._enter()

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
        self._enter()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)
        if port.state == "ON":
            result = NormalResponse.PORT_ALREADY_ON
            log.warn("port %s in platform %s already turned on." % (port_id, platform_id))
        else:
            port.set_state("ON")
            result = NormalResponse.PORT_TURNED_ON
            log.info("port %s in platform %s turned on." % (port_id, platform_id))

        return {platform_id: {port_id: result}}

    def turn_off_platform_port(self, platform_id, port_id):
        self._enter()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        if port_id not in self._pnodes[platform_id].ports :
            return {platform_id: {port_id: InvalidResponse.PORT_ID}}

        port = self._pnodes[platform_id].get_port(port_id)
        if port.state == "OFF":
            result = NormalResponse.PORT_ALREADY_OFF
            log.warn("port %s in platform %s already turned off." % (port_id, platform_id))
        else:
            port.set_state("OFF")
            result = NormalResponse.PORT_TURNED_OFF
            log.info("port %s in platform %s turned off." % (port_id, platform_id))

        return {platform_id: {port_id: result}}

    def _validate_event_listener_url(self, url):
        """
        Does a basic, static validation of the url.
        """
        # TODO implement it; for now always returning True
        return True

    def register_event_listener(self, url):
        self._enter()

        # NOTE: event_types was previously a parameter to this operation. To
        # minimize changes in the code, I introduced an 'ALL' event type to
        # be used here explicitly.
        event_type = 'ALL'

        log.debug("register_event_listener called: url=%r", url)

        if not self._validate_event_listener_url(url):
            return {url: InvalidResponse.EVENT_LISTENER_URL}

        if not url in self._reg_event_listeners:
            # create entry for this new url
            reg_time = self._event_notifier.add_listener(url, event_type)
            self._reg_event_listeners[url] = reg_time
            log.info("registered url=%r", url)
        else:
            # already registered:
            reg_time = self._reg_event_listeners[url]

        self._start_event_generator_if_listeners()

        return {url: reg_time}

    def unregister_event_listener(self, url):
        self._enter()

        # NOTE: event_types was previously a parameter to this operation. To
        # minimize changes in the code, I introduced an 'ALL' event type to
        # be used here explicitly.
        event_type = 'ALL'

        log.debug("unregister_event_listener called: url=%r", url)

        if not url in self._reg_event_listeners:
            return {url: 0}

        #
        # registered, so remove it
        #
        unreg_time = self._event_notifier.remove_listener(url, event_type)
        del self._reg_event_listeners[url]

        log.info("unregistered url=%r", url)

        self._stop_event_generator_if_no_listeners()

        return {url: unreg_time}

    def get_registered_event_listeners(self):
        self._enter()

        return self._reg_event_listeners

    def generate_test_event(self, event):
        self._enter()

        if self._event_generator:  # there are listeners registered.
            # copy event and include the additional fields:
            event_instance = event.copy()
            event_instance['test_event'] = True
            timestamp = ntplib.system_to_ntp_time(time.time())
            if 'timestamp' not in event_instance:
                event_instance['timestamp'] = timestamp
            if 'first_time_timestamp' not in event_instance:
                event_instance['first_time_timestamp'] = timestamp
            # simply notify listeners right away
            self._event_notifier.notify(event_instance)
            return True

        else:  # there are *no* listeners registered.
            return False

    def get_checksum(self, platform_id):
        """
        @note the checksum is always computed, which is fine for the simulator.
        A more realistic and presumably more efficient implementation would
        exploit some caching mechanism along with appropriate invalidation
        upon modifications to the platform information.
        """
        self._enter()

        if platform_id not in self._pnodes:
            return {platform_id: InvalidResponse.PLATFORM_ID}

        pnode = self._pnodes[platform_id]
        checksum = pnode.compute_checksum()

        return {platform_id: checksum}
