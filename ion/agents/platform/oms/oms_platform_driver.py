#!/usr/bin/env python

"""
@package ion.agents.platform.oms.oms_platform_driver
@file    ion/agents/platform/oms/oms_platform_driver.py
@author  Carlos Rueda
@brief   Base class for OMS platform drivers.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.public import log
import logging

from ion.agents.platform.platform_driver import PlatformDriver
from ion.agents.platform.exceptions import PlatformException
from ion.agents.platform.exceptions import PlatformDriverException
from ion.agents.platform.exceptions import PlatformConnectionException
from ion.agents.platform.oms.oms_client_factory import OmsClientFactory
from ion.agents.platform.oms.oms_client import InvalidResponse
from ion.agents.platform.oms.oms_event_listener import OmsEventListener

from ion.agents.platform.util import ion_ts_2_ntp, ntp_2_ion_ts


class OmsPlatformDriver(PlatformDriver):
    """
    Base class for OMS platform drivers.
    """

    def __init__(self, platform_id, driver_config, parent_platform_id=None):
        """
        Creates an OmsPlatformDriver instance.

        @param platform_id Corresponding platform ID
        @param driver_config with required 'oms_uri' entry.
        @param parent_platform_id Platform ID of my parent, if any.
                    This is mainly used for diagnostic purposes
        """
        PlatformDriver.__init__(self, platform_id, driver_config, parent_platform_id)

        if not 'oms_uri' in driver_config:
            raise PlatformDriverException(msg="driver_config does not indicate 'oms_uri'")

        oms_uri = driver_config['oms_uri']
        log.debug("%r: creating OmsClient instance with oms_uri=%r",
            self._platform_id, oms_uri)
        self._oms = OmsClientFactory.create_instance(oms_uri)
        log.debug("%r: OmsClient instance created: %s",
            self._platform_id, self._oms)

        # we can instantiate this here as the the actual http server is
        # started via corresponding method.
        self._event_listener = OmsEventListener(self._notify_driver_event)

    def ping(self):
        """
        Verifies communication with external platform returning "PONG" if
        this verification completes OK.

        @retval "PONG" iff all OK.
        @raise PlatformConnectionException Cannot ping external platform or
               got unexpected response.
        """
        log.debug("%r: pinging OMS...", self._platform_id)
        try:
            retval = self._oms.hello.ping()
        except Exception, e:
            raise PlatformConnectionException(msg="Cannot ping %s" % str(e))

        if retval is None or retval.upper() != "PONG":
            raise PlatformConnectionException(msg="Unexpected ping response: %r" % retval)

        return "PONG"

    def _verify_got_nnode(self):
        if not self._nnode:
            raise PlatformDriverException("%r: set_nnode must have been called" % self._platform_id)

    def go_active(self):
        """
        Activates the driver.

        @raise PlatformDriverException set_nnode must have been called prior to this call.
        """

        if log.isEnabledFor(logging.DEBUG):
            # NOTE: The following log.debug DOES NOT show up when running a test
            # with the pycc plugin (--with-pycc)!  (noticed with test_oms_launch).
            log.debug("%r: going active..", self._platform_id)

        self._verify_got_nnode()

        # note, we ping the OMS here regardless of the source for the network
        # definition:
        self.ping()

        self.__gen_diagram()

    def __gen_diagram(self):  # pragma: no cover
        """
        **Developer routine**
        Convenience method for testing/debugging.
        Generates a dot diagram iff the environment variable GEN_DIAG is
        defined and this driver corresponds to the root of the network
        (determined by not having a parent platform ID).
        """
        try:
            import os, tempfile, subprocess
            if os.getenv("GEN_DIAG", None) is None:
                return
            if self._parent_platform_id:
                # I'm not the root of the network
                return

            # I'm the root of the network
            name = self._platform_id
            base_name = '%s/%s' % (tempfile.gettempdir(), name)
            dot_name = '%s.dot' % base_name
            png_name = '%s.png' % base_name
            print 'generating diagram %r' % dot_name
            file(dot_name, 'w').write(self._nnode.diagram(style="dot"))
            print 'generating png %r' % png_name
            dot_cmd = 'dot -Tpng %s -o %s' % (dot_name, png_name)
            subprocess.call(dot_cmd.split())
            print 'opening %r' % png_name
            open_cmd = 'open %s' % png_name
            subprocess.call(open_cmd.split())
        except Exception, e:
            print "error generating or opening diagram: %s" % str(e)

    def get_metadata(self):
        """
        """
        retval = self._oms.get_platform_metadata(self._platform_id)
        log.debug("get_platform_metadata = %s", retval)

        if not self._platform_id in retval:
            raise PlatformException("Unexpected: response does not include "
                                    "requested platform '%s'" % self._platform_id)

        md = retval[self._platform_id]
        return md

    def get_attribute_values(self, attr_names, from_time):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("get_attribute_values: attr_names=%s from_time=%s" % (
                str(attr_names), from_time))

        # OOIION-631 convert the system time from_time to NTP, which is used by
        # the RSN OMS interface:
        ntp_from_time = ion_ts_2_ntp(from_time)
        retval = self._oms.get_platform_attribute_values(self._platform_id, attr_names, ntp_from_time)
        log.debug("get_platform_attribute_values = %s", retval)

        if not self._platform_id in retval:
            raise PlatformException("Unexpected: response does not include "
                                    "requested platform '%s'" % self._platform_id)

        attr_values = retval[self._platform_id]

        # OOIION-631 the reported timestamps are in NTP; do conversion to system time

        if log.isEnabledFor(logging.DEBUG):
            log.debug("get_attribute_values: response before conversion = %s" %
                      str(attr_values))

        conv_attr_values = {}  # the converted dictionary to return
        for attr_name, array in attr_values.iteritems():
            conv_array = []
            for (val, ntp_time) in array:

                if isinstance(ntp_time, (float, int)):
                    # do conversion:
                    sys_ts = ntp_2_ion_ts(ntp_time)
                else:
                    # NO conversion; just keep whatever the returned value is --
                    # normally an error code in str format:
                    sys_ts = ntp_time

                conv_array.append((val, sys_ts))

            conv_attr_values[attr_name] = conv_array

        if log.isEnabledFor(logging.DEBUG):
            log.debug("get_attribute_values: response  after conversion = %s" %
                      str(conv_attr_values))

        return conv_attr_values


    def _validate_set_attribute_values(self, attrs):
        """
        Does some pre-validation of the passed values according to the
        definition of the attributes.

        NOTE: We don't check everything here, just some basics.
        TODO determine appropriate validations at this level.
        Note that the basic checks here follow what the OMS system
        will do if we just send the request directly to it. So,
        need to determine what exactly should be done on the CI side.

        @param attrs

        @return dict of errors for the offending attribute names, if any.
        """
        # TODO determine appropriate validations at this level.

        # get definitions to verify the values against
        attr_defs = self._get_platform_attributes()

        if log.isEnabledFor(logging.DEBUG):
            log.debug("validating passed attributes: %s against defs %s", attrs, attr_defs)

        # to collect errors, if any:
        error_vals = {}
        for attr_name, attr_value in attrs:

            attr_def = attr_defs.get(attr_name, None)

            if log.isEnabledFor(logging.DEBUG):
                log.debug("validating %s against %s", attr_name, str(attr_def))

            if not attr_def:
                error_vals[attr_name] = InvalidResponse.ATTRIBUTE_NAME
                log.warn("Attribute %s not in associated platform %s",
                    attr_name, self._platform_id)
                continue

            type = attr_def.get('type', None)
            units = attr_def.get('units', None)
            min_val = attr_def.get('min_val', None)
            max_val = attr_def.get('max_val', None)
            read_write = attr_def.get('read_write', None)
            group = attr_def.get('group', None)

            if "write" != read_write:
                error_vals[attr_name] = InvalidResponse.ATTRIBUTE_NOT_WRITABLE
                log.warn(
                    "Trying to set read-only attribute %s in platform %s",
                    attr_name, self._platform_id)
                continue

            #
            # TODO the following value-related checks are minimal
            #
            if type in ["float", "int"]:
                if min_val and float(attr_value) < float(min_val):
                    error_vals[attr_name] = InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE
                    log.warn(
                        "Value %s for attribute %s is less than specified minimum "
                        "value %s in associated platform %s",
                        attr_value, attr_name, min_val,
                        self._platform_id)
                    continue

                if max_val and float(attr_value) > float(max_val):
                    error_vals[attr_name] = InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE
                    log.warn(
                        "Value %s for attribute %s is greater than specified maximum "
                        "value %s in associated platform %s",
                        attr_value, attr_name, max_val,
                        self._platform_id)
                    continue

        return error_vals

    def set_attribute_values(self, attrs):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("set_attribute_values: attrs = %s" % str(attrs))

        error_vals = self._validate_set_attribute_values(attrs)
        if len(error_vals) > 0:
            # remove offending attributes for the request below
            attrs_dict = dict(attrs)
            for bad_attr_name in error_vals:
                del attrs_dict[bad_attr_name]

            # no good attributes at all?
            if len(attrs_dict) == 0:
                # just immediately return with the errors:
                return error_vals

            # else: update attrs with the good attributes:
            attrs = attrs_dict.items()

        # ok, now make the request to RSN OMS:
        retval = self._oms.set_platform_attribute_values(self._platform_id, attrs)
        log.debug("set_platform_attribute_values = %s", retval)

        if not self._platform_id in retval:
            raise PlatformException("Unexpected: response does not include "
                                    "requested platform '%s'" % self._platform_id)

        attr_values = retval[self._platform_id]

        # OOIION-631 the reported timestamps are in NTP; see below for
        # conversion to system time.

        if log.isEnabledFor(logging.DEBUG):
            log.debug("set_attribute_values: response before conversion = %s" %
                      str(attr_values))

        # conv_attr_values: the time converted dictionary to return, initialized
        # with the error ones determined above if any:
        conv_attr_values = error_vals

        for attr_name, attr_val_ts in attr_values.iteritems():
            (val, ntp_time) = attr_val_ts

            if isinstance(ntp_time, (float, int)):
                # do conversion:
                sys_ts = ntp_2_ion_ts(ntp_time)
            else:
                # NO conversion; just keep whatever the returned value is --
                # normally an error code in str format:
                sys_ts = ntp_time

            conv_attr_values[attr_name] = (val, sys_ts)

        if log.isEnabledFor(logging.DEBUG):
            log.debug("set_attribute_values: response  after conversion = %s" %
                      str(conv_attr_values))

        return conv_attr_values

    def _verify_platform_id_in_response(self, response):
        """
        Verifies the presence of my platform_id in the response.

        @param response Dictionary returned by _oms

        @retval response[self._platform_id]
        """
        if not self._platform_id in response:
            msg = "unexpected: response does not contain entry for %r" % self._platform_id
            log.error(msg)
            raise PlatformException(msg=msg)

        if response[self._platform_id] == InvalidResponse.PLATFORM_ID:
            msg = "response reports invalid platform_id for %r" % self._platform_id
            log.error(msg)
            raise PlatformException(msg=msg)
        else:
            return response[self._platform_id]

    def _get_platform_attributes(self):
        self._verify_got_nnode()

        attr_info = dict((attr.attr_id, attr.defn) for attr in self._nnode.attrs.itervalues())
        log.debug("%r: _get_platform_attributes attr_info=%s",
              self._platform_id, attr_info)
        return attr_info

    ###############################################
    # Ports:

    def get_ports(self):
        self._verify_got_nnode()

        ports = {}
        for port_id, port in self._nnode.ports.iteritems():
            ports[port_id] = {'comms': port.comms, 'attrs': port.attrs}
        log.debug("%r: get_ports: %s",
              self._platform_id, ports)
        return ports

    def _verify_port_id_in_response(self, port_id, dic):
        """
        Verifies the presence of port_id in the dic.

        @param port_id  The ID to verify
        @param dic Dictionary returned by _oms

        @return dic[port_id]
        """
        if not port_id in dic:
            msg = "unexpected: dic does not contain entry for %r" % port_id
            log.error(msg)
            raise PlatformException(msg=msg)

        if dic[port_id] == InvalidResponse.PORT_ID:
            msg = "%r: response reports invalid port_id for %r" % (
                                 self._platform_id, port_id)
            log.error(msg)
            raise PlatformException(msg=msg)
        else:
            return dic[port_id]

    def _verify_instrument_id_in_response(self, port_id, instrument_id, dic):
        """
        Verifies the presence of instrument_id in the dic.

        @param port_id        Used for error reporting
        @param instrument_id  The ID to verify
        @param dic            Dictionary returned by _oms

        @return dic[instrument_id]
        """
        if not instrument_id in dic:
            msg = "unexpected: dic does not contain entry for %r" % instrument_id
            log.error(msg)
            raise PlatformException(msg=msg)

        if dic[instrument_id] == InvalidResponse.INSTRUMENT_ID:
            msg = "%r: port_id=%r: response reports invalid instrument_id for" \
                  " %r" % (self._platform_id, port_id, instrument_id)
            log.error(msg)
            raise PlatformException(msg=msg)
        else:
            return dic[instrument_id]

    def connect_instrument(self, port_id, instrument_id, attributes):
        log.debug("%r: connect_instrument: port_id=%r instrument_id=%r attributes=%s",
                  self._platform_id, port_id, instrument_id, attributes)

        response = self._oms.connect_instrument(self._platform_id, port_id, instrument_id, attributes)
        log.debug("%r: connect_instrument response: %s",
            self._platform_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        self._verify_port_id_in_response(port_id, dic_plat)

        return dic_plat  # note: return the dic for the platform

    def disconnect_instrument(self, port_id, instrument_id):
        log.debug("%r: disconnect_instrument: port_id=%r instrument_id=%r",
                  self._platform_id, port_id, instrument_id)

        response = self._oms.disconnect_instrument(self._platform_id, port_id, instrument_id)
        log.debug("%r: disconnect_instrument response: %s",
            self._platform_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        port_dic = self._verify_port_id_in_response(port_id, dic_plat)
        self._verify_instrument_id_in_response(port_id, instrument_id, port_dic)

        return dic_plat  # note: return the dic for the platform

    def get_connected_instruments(self, port_id):
        log.debug("%r: get_connected_instruments: port_id=%s",
                  self._platform_id, port_id)

        response = self._oms.get_connected_instruments(self._platform_id, port_id)
        log.debug("%r: port_id=%r: get_connected_instruments response: %s",
            self._platform_id, port_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        port_dic = self._verify_port_id_in_response(port_id, dic_plat)

        return dic_plat  # note: return the dic for the platform

    def turn_on_port(self, port_id):
        log.debug("%r: turning on port: port_id=%s",
                  self._platform_id, port_id)

        response = self._oms.turn_on_platform_port(self._platform_id, port_id)
        log.debug("%r: turn_on_platform_port response: %s",
            self._platform_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        self._verify_port_id_in_response(port_id, dic_plat)

        return dic_plat  # note: return the dic for the platform

    def turn_off_port(self, port_id):
        log.debug("%r: turning off port: port_id=%s",
                  self._platform_id, port_id)

        response = self._oms.turn_off_platform_port(self._platform_id, port_id)
        log.debug("%r: turn_off_platform_port response: %s",
            self._platform_id, response)

        dic_plat = self._verify_platform_id_in_response(response)
        self._verify_port_id_in_response(port_id, dic_plat)

        return dic_plat  # note: return the dic for the platform

    ###############################################
    # Events:

    def _register_event_listener(self, url):
        """
        Registers given url for all event types.
        """
        result = self._oms.register_event_listener(url, [])
        log.info("register_event_listener url=%r returned: %s", url, str(result))

    def _unregister_event_listener(self, url):
        """
        Unregisters given url for all event types.
        """
        result = self._oms.unregister_event_listener(url, [])
        log.info("unregister_event_listener url=%r returned: %s", url, str(result))

    def start_event_dispatch(self, params):
        # start http server:
        self._event_listener.start_http_server()

        # then, register my listener:
        self._register_event_listener(self._event_listener.url)

        return "OK"

    def stop_event_dispatch(self):
        # unregister my listener:
        self._unregister_event_listener(self._event_listener.url)

        # then, stop http server:
        self._event_listener.stop_http_server()

        return "OK"
