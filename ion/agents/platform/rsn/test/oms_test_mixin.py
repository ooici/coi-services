#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.test.oms_test_mixin
@file    ion/agents/platform/rsn/test/oms_test_mixin.py
@author  Carlos Rueda
@brief   A mixin to facilitate test cases for OMS objects following the
         OMS-CI interface.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.rsn.simulator.logger import Logger
log = Logger.get_logger()

from ion.agents.platform.test.helper import HelperTestMixin

from ion.agents.platform.responses import NormalResponse, InvalidResponse

import time
import ntplib
from gevent.pywsgi import WSGIServer
import socket
import yaml


# some bogus IDs
BOGUS_PLATFORM_ID = 'bogus_plat_id'
BOGUS_ATTR_NAMES = ['bogus_attr1|1', 'bogus_attr2|2']
BOGUS_PORT_ID = 'bogus_port_id'
BOGUS_INSTRUMENT_ID = 'bogus_instrument_id'
BOGUS_EVENT_TYPE = "bogus_event_type"


class OmsTestMixin(HelperTestMixin):
    """
    A mixin to facilitate test cases for OMS objects following the OMS-CI interface.
    """
    @classmethod
    def setUpClass(cls):
        HelperTestMixin.setUpClass()

    def test_aa_ping(self):
        response = self.oms.hello.ping()
        self.assertEquals(response, "pong")

    def test_ab_get_platform_map(self):
        platform_map = self.oms.config.get_platform_map()
        log.info("config.get_platform_map() => %s" % platform_map)
        self.assertIsInstance(platform_map, list)
        roots = []
        for pair in platform_map:
            self.assertIsInstance(pair, (tuple, list))
            self.assertEquals(len(pair), 2)
            plat, parent = pair
            if parent == '':
                roots.append(plat)
        self.assertEquals(len(roots), 1)
        self.assertEquals("ShoreStation", roots[0])

    def test_ac_get_platform_types(self):
        retval = self.oms.config.get_platform_types()
        log.info("config.get_platform_types() => %s" % retval)
        self.assertIsInstance(retval, dict)
        for k, v in retval.iteritems():
            self.assertIsInstance(k, str)
            self.assertIsInstance(v, str)

    def test_ad_get_platform_metadata(self):
        platform_id = self.PLATFORM_ID
        retval = self.oms.config.get_platform_metadata(platform_id)
        log.info("config.get_platform_metadata(%r) => %s" % (platform_id, retval))
        md = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(md, dict)
        # TODO: decide on the actual expected metadata.
        # if not 'platform_types' in md:
        #     log.warn("RSN OMS spec: platform_types not included in metadata: %s", md)

    def test_ad_get_platform_metadata_invalid(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.config.get_platform_metadata(platform_id)
        log.info("config.get_platform_metadata(%r) => %s" % (platform_id, retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_af_get_platform_attributes(self):
        platform_id = self.PLATFORM_ID
        retval = self.oms.attr.get_platform_attributes(platform_id)
        log.info("attr.get_platform_attributes(%r) => %s" % (platform_id, retval))
        infos = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(infos, dict)

    def test_ag_get_platform_attributes_invalid(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.attr.get_platform_attributes(platform_id)
        log.info("attr.get_platform_attributes(%r) => %s" % (platform_id, retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_ah_get_platform_attribute_values(self):
        platform_id = self.PLATFORM_ID
        attrNames = self.ATTR_NAMES
        cur_time = ntplib.system_to_ntp_time(time.time())
        from_time = cur_time - 50  # a 50-sec time window
        req_attrs = [(attr_id, from_time) for attr_id in attrNames]
        log.debug("attr.get_platform_attribute_values(%r, %r)" % (platform_id, req_attrs))
        retval = self.oms.attr.get_platform_attribute_values(platform_id, req_attrs)
        log.info("attr.get_platform_attribute_values(%r, %r) => %s" % (platform_id, req_attrs, retval))
        vals = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(vals, dict)
        for attrName in attrNames:
            self._verify_valid_attribute_id(attrName, vals)

    def test_ah_get_platform_attribute_values_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        attrNames = self.ATTR_NAMES
        cur_time = ntplib.system_to_ntp_time(time.time())
        from_time = cur_time - 50  # a 50-sec time window
        req_attrs = [(attr_id, from_time) for attr_id in attrNames]
        log.debug("attr.get_platform_attribute_values(%r, %r)" % (platform_id, req_attrs))
        retval = self.oms.attr.get_platform_attribute_values(platform_id, req_attrs)
        log.info("attr.get_platform_attribute_values(%r, %r) => %s" % (platform_id, req_attrs, retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_ah_get_platform_attribute_values_invalid_attributes(self):
        platform_id = self.PLATFORM_ID
        attrNames = BOGUS_ATTR_NAMES
        cur_time = ntplib.system_to_ntp_time(time.time())
        from_time = cur_time - 50  # a 50-sec time window
        req_attrs = [(attr_id, from_time) for attr_id in attrNames]
        log.debug("attr.get_platform_attribute_values(%r, %r)" % (platform_id, req_attrs))
        retval = self.oms.attr.get_platform_attribute_values(platform_id, req_attrs)
        log.info("attr.get_platform_attribute_values(%r, %r) => %s" % (platform_id, req_attrs, retval))
        vals = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(vals, dict)
        for attrName in attrNames:
            self._verify_invalid_attribute_id(attrName, vals)

    def test_ah_set_platform_attribute_values(self):
        platform_id = self.PLATFORM_ID
        # try for all test attributes, but check below for both those writable
        # and not writable
        attrNames = self.ATTR_NAMES

        def valueFor(attrName):
            # simple string value, ok because there is no strict value check yet
            # TODO more realistic value depending on attribute's type
            return "test_value_for_%s" % attrName

        attrs = [(attrName, valueFor(attrName)) for attrName in attrNames]
        log.debug("attr.set_platform_attribute_values(%r, %r)" % (platform_id, attrs))
        retval = self.oms.attr.set_platform_attribute_values(platform_id, attrs)
        log.info("attr.set_platform_attribute_values(%r, %r) => %s" % (platform_id, attrs, retval))
        vals = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(vals, dict)
        for attrName in attrNames:
            if attrName in self.WRITABLE_ATTR_NAMES:
                self._verify_valid_attribute_id(attrName, vals)
            else:
                self._verify_not_writable_attribute_id(attrName, vals)

    def _get_platform_ports(self, platform_id):
        retval = self.oms.port.get_platform_ports(platform_id)
        log.info("port.get_platform_ports(%r) => %s" % (platform_id, retval))
        ports = self._verify_valid_platform_id(platform_id, retval)
        return ports

    def test_ak_get_platform_ports(self):
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)
        for port_id, info in ports.iteritems():
            self.assertIsInstance(info, dict)
            self.assertIn('state',   info)

    def test_ak_get_platform_ports_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.port.get_platform_ports(platform_id)
        log.info("port.get_platform_ports(%r) => %s" % (platform_id, retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def _get_connected_instruments(self, platform_id, port_id):
        log.debug("instr.get_connected_instruments(%r, %r)" % (platform_id, port_id))
        retval = self.oms.instr.get_connected_instruments(platform_id, port_id)
        log.info("instr.get_connected_instruments(%r, %r) => %s" % (platform_id, port_id, retval))
        ports = self._verify_valid_platform_id(platform_id, retval)
        port_dic = self._verify_valid_port_id(port_id, ports)
        self.assertIsInstance(port_dic, dict)
        return port_dic

    def test_al_get_connected_instruments(self):
        platform_id = self.PLATFORM_ID
        port_id = self.PORT_ID
        self._get_connected_instruments(platform_id, port_id)

    def _connect_instrument(self, platform_id, port_id, instrument_id, attributes):
        log.debug("instr.connect_instrument(%r, %r, %r, %r)" % (platform_id, port_id, instrument_id, attributes))
        retval = self.oms.instr.connect_instrument(platform_id, port_id, instrument_id, attributes)
        log.info("instr.connect_instrument(%r, %r, %r, %r) => %s" % (platform_id, port_id, instrument_id, attributes, retval))
        return retval

    def _connect_instrument_valid(self, platform_id, port_id, instrument_id, attributes):
        retval = self._connect_instrument(platform_id, port_id, instrument_id, attributes)
        ports = self._verify_valid_platform_id(platform_id, retval)
        port_dic = self._verify_valid_port_id(port_id, ports)
        self.assertIsInstance(port_dic, dict)
        instr_val = self._verify_valid_instrument_id(instrument_id, port_dic)
        if isinstance(instr_val, dict):
            for attr_name in attributes:
                self.assertTrue(attr_name in instr_val)
                attr_val = instr_val[attr_name]
                self.assertEquals(attributes[attr_name], attr_val,
                                  "value given %s different from value received %s" % (
                                  attributes[attr_name], attr_val))

    def _disconnect_instrument(self, platform_id, port_id, instrument_id):
        log.debug("instr.disconnect_instrument(%r, %r, %r)" % (platform_id, port_id, instrument_id))
        retval = self.oms.instr.disconnect_instrument(platform_id, port_id, instrument_id)
        log.info("instr.disconnect_instrument(%r, %r, %r) => %s" % (platform_id, port_id, instrument_id, retval))
        return retval

    def _disconnect_instrument_valid(self, platform_id, port_id, instrument_id):
        retval = self._disconnect_instrument(platform_id, port_id, instrument_id)
        ports = self._verify_valid_platform_id(platform_id, retval)
        port_dic = self._verify_valid_port_id(port_id, ports)
        self.assertIsInstance(port_dic, dict)
        self.assertIn(instrument_id, port_dic)
        self._verify_instrument_disconnected(instrument_id, port_dic[instrument_id])

    def test_am_connect_and_disconnect_instrument(self):
        platform_id = self.PLATFORM_ID
        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID

        # connect (if not already connected)
        port_dic = self._get_connected_instruments(platform_id, port_id)
        if not instrument_id in port_dic:
            # TODO proper values
            attributes = {'maxCurrentDraw': 1, 'initCurrent': 2,
                          'dataThroughput': 3, 'instrumentType': 'FOO'}

            self._connect_instrument_valid(platform_id, port_id, instrument_id, attributes)

        # disconnect:
        port_dic = self._get_connected_instruments(platform_id, port_id)
        if instrument_id in port_dic:
            self._disconnect_instrument_valid(platform_id, port_id, instrument_id)

    def test_am_connect_instrument_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID
        attributes = {}
        retval = self._connect_instrument(platform_id, port_id, instrument_id, attributes)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_am_connect_instrument_invalid_port_id(self):
        platform_id = self.PLATFORM_ID
        port_id = BOGUS_PORT_ID
        instrument_id = self.INSTRUMENT_ID
        attributes = {}
        retval = self._connect_instrument(platform_id, port_id, instrument_id, attributes)
        ports = self._verify_valid_platform_id(platform_id, retval)
        self._verify_invalid_port_id(port_id, ports)

    def test_am_connect_instrument_invalid_instrument_id(self):
        platform_id = self.PLATFORM_ID
        port_id = self.PORT_ID
        instrument_id = BOGUS_INSTRUMENT_ID
        attributes = {}
        retval = self._connect_instrument(platform_id, port_id, instrument_id, attributes)
        ports = self._verify_valid_platform_id(platform_id, retval)
        port_dic = self._verify_valid_port_id(port_id, ports)
        self.assertIsInstance(port_dic, dict)
        self._verify_invalid_instrument_id(instrument_id, port_dic)

    def test_an_turn_on_platform_port(self):
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)
        for port_id in ports.iterkeys():
            retval = self.oms.port.turn_on_platform_port(platform_id, port_id)
            log.info("port.turn_on_platform_port(%s,%s) => %s" % (platform_id, port_id, retval))
            portRes = self._verify_valid_platform_id(platform_id, retval)
            res = self._verify_valid_port_id(port_id, portRes)
            self.assertEquals(res, NormalResponse.PORT_TURNED_ON)

    def test_an_turn_on_platform_port_invalid_platform_id(self):
        # use valid id for get_platform_ports
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)

        # use invalid id for turn_on_platform_port
        requested_platform_id = BOGUS_PLATFORM_ID
        for port_id in ports.iterkeys():
            retval = self.oms.port.turn_on_platform_port(requested_platform_id, port_id)
            log.info("port.turn_on_platform_port(%r, %r) => %s" % (requested_platform_id, port_id, retval))
            self._verify_invalid_platform_id(requested_platform_id, retval)

    def test_ao_turn_off_platform_port(self):
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)
        for port_id in ports.iterkeys():
            retval = self.oms.port.turn_off_platform_port(platform_id, port_id)
            log.info("port.turn_off_platform_port(%r, %r) => %s" % (platform_id, port_id, retval))
            portRes = self._verify_valid_platform_id(platform_id, retval)
            res = self._verify_valid_port_id(port_id, portRes)
            self.assertEquals(res, NormalResponse.PORT_TURNED_OFF)

    def test_ao_turn_off_platform_port_invalid_platform_id(self):
        # use valid for get_platform_ports
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)

        # use invalid for turn_off_platform_port
        requested_platform_id = BOGUS_PLATFORM_ID
        for port_id in ports.iterkeys():
            retval = self.oms.port.turn_off_platform_port(requested_platform_id, port_id)
            log.info("port.turn_off_platform_port(%r, %r) => %s" % (requested_platform_id, port_id, retval))
            self._verify_invalid_platform_id(requested_platform_id, retval)

    ###################################################################
    # EVENTS
    ###################################################################

    # _notifications: [event_instance, ...]
    _notifications = []
    _http_server = None

    # Based on the HTTP server launched via start_http_server (if called), or just an ad hoc url.
    _use_fqdn_for_event_listener = False

    # some dummy value ok for tests only dealing with (un)registration:
    _url_for_listener = "http://listener.example.org"

    @classmethod
    def start_http_server(cls, port=5000):
        """
        Starts a server on the localhost to handle the reception of events reported by OMS.
        The received events are kept in the member _notifications, which can be
        consulted directly and it is also returned by stop_http_server.

        @param port   Port to bind the server to. By default, 5000.

        @return  URL that can be used as a listener for registration with OMS.
                 This URL is composed based on the port and socket.getfqdn()
                 if _use_fqdn_for_event_listener is True.
        """
        def application(environ, start_response):
            input = environ['wsgi.input']
            body = "\n".join(input.readlines())
            event_instance = yaml.load(body)
            log.debug('http server received event_instance=%s' % str(event_instance))

            cls._notifications.append(event_instance)

            status = '200 OK'
            headers = [('Content-Type', 'text/plain')]
            start_response(status, headers)
            return status

        cls._notifications = []
        cls._http_server = WSGIServer(('localhost', port), application)
        log.debug("starting http server for receiving event notifications...")
        cls._http_server.start()
        address = cls._http_server.address
        log.info("http server for event listener started on %s:%s" % address)

        if cls._use_fqdn_for_event_listener:
            cls._url_for_listener = "http://%s:%s" % (socket.getfqdn(), address[1])
        else:
            cls._url_for_listener = "http://%s:%s" % address
        log.info("url_for_listener = %s" % cls._url_for_listener)

        return cls._url_for_listener

    @classmethod
    def stop_http_server(cls):
        """
        Stops the http server returning the notifications list,
        which is internally re-initialized.
        """
        if cls._http_server:
            address = cls._http_server.address
            log.info("stopping http server: address: host=%r port=%r" % address)
            cls._http_server.stop()
            cls._http_server = None

        ret = cls._notifications
        cls._notifications = []  # re-initialize
        return ret

    def _get_registered_event_listeners(self):
        listeners = self.oms.event.get_registered_event_listeners()
        log.info("event.get_registered_event_listeners() => %s" % str(listeners))
        self.assertIsInstance(listeners, dict)
        return listeners

    def _register_event_listener(self, url):
        log.info("event.register_event_listener(%r)" % url)
        result = self.oms.event.register_event_listener(url)
        log.info("event.register_event_listener(%r) => %s" % (url, str(result)))
        self.assertIsInstance(result, dict)
        self.assertEquals(len(result), 1)
        self.assertTrue(url in result)

        listeners = self._get_registered_event_listeners()
        self.assertTrue(url in listeners)

        def unregister():
            self._unregister_event_listener(url)
        self.addCleanup(unregister)

        return result[url]

    def _unregister_event_listener(self, url):
        result = self.oms.event.unregister_event_listener(url)
        log.info("event.unregister_event_listener(%r) => %s" % (url, str(result)))
        self.assertIsInstance(result, dict)
        self.assertEquals(len(result), 1)
        self.assertTrue(url in result)

        # check that it's unregistered
        listeners = self._get_registered_event_listeners()
        self.assertTrue(url not in listeners)

        return result[url]

    def test_be_register_and_unregister_event_listener(self):
        url = self._url_for_listener
        self._register_event_listener(url)
        self._unregister_event_listener(url)

    def test_bi_unregister_event_listener_not_registered_url(self):
        url = "http://_never_registered_url"
        res = self._unregister_event_listener(url)
        self.assertEquals(0, res)

    def test_generate_test_event_and_reception(self):
        # 1. start http server for listener:
        url = self.start_http_server()
        self.addCleanup(self.stop_http_server)

        # 2. register listener:
        self._register_event_listener(url)

        # 3. request generation of test event:
        event = {
            'message'      : "fake event triggered from CI",
            'platform_id'  : "some_platform_id",
            'severity'     : "3",
            'group '       : "power",
        }
        log.debug("event.generate_test_event(%r)" % event)
        result = self.oms.event.generate_test_event(event)
        log.info("event.generate_test_event(%r) => %r" % (event, result))
        self.assertEquals(result, True)

        # 4. wait until test event is notified
        max_wait = 30
        log.info("waiting for a max of %d secs for test event to be notified..." % max_wait)
        wait_until = time.time() + max_wait
        got_it = None
        while not got_it and time.time() <= wait_until:
            time.sleep(1)
            for evt in self._notifications:
                if event['message'] == evt['message']:
                    got_it = evt
                    break

        self.assertIsNotNone(got_it, "didn't get expected test event notification within %d " \
                             "secs. (Got %d event notifications.)" % (
                             max_wait, len(self._notifications)))
        log.info("got test event: %s" % got_it)

    def test_get_checksum(self):
        platform_id = self.PLATFORM_ID
        retval = self.oms.config.get_checksum(platform_id)
        log.info("config.get_checksum(%r) => %s" % (platform_id, retval))
