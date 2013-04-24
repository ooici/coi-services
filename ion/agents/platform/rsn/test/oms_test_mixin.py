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
import yaml


# some bogus IDs
BOGUS_PLATFORM_ID = 'bogus_plat_id'
BOGUS_ATTR_NAMES = ['bogus_attr1', 'bogus_attr2']
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
        log.info("get_platform_types = %s" % retval)
        self.assertIsInstance(retval, dict)
        for k, v in retval.iteritems():
            self.assertIsInstance(k, str)
            self.assertIsInstance(v, str)

    def test_ad_get_platform_metadata(self):
        platform_id = self.PLATFORM_ID
        retval = self.oms.config.get_platform_metadata(platform_id)
        log.info("get_platform_metadata(%r) = %s" % (platform_id,  retval))
        md = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(md, dict)
        self.assertTrue('platform_types' in md)

    def test_ad_get_platform_metadata_invalid(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.config.get_platform_metadata(platform_id)
        log.info("get_platform_metadata(%r) = %s" % (platform_id,retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_af_get_platform_attributes(self):
        platform_id = self.PLATFORM_ID
        retval = self.oms.config.get_platform_attributes(platform_id)
        log.info("get_platform_attributes(%r) = %s" % (platform_id, retval))
        infos = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(infos, dict)

    def test_ag_get_platform_attributes_invalid(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.config.get_platform_attributes(platform_id)
        log.info("get_platform_attributes(%r) = %s" % (platform_id, retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_ah_get_platform_attribute_values(self):
        platform_id = self.PLATFORM_ID
        attrNames = self.ATTR_NAMES
        cur_time = ntplib.system_to_ntp_time(time.time())
        from_time = cur_time - 50  # a 50-sec time window
        req_attrs = [(attr_id, from_time) for attr_id in attrNames]
        retval = self.oms.get_platform_attribute_values(platform_id, req_attrs)
        log.info("get_platform_attribute_values = %s" % retval)
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
        retval = self.oms.get_platform_attribute_values(platform_id, req_attrs)
        log.info("get_platform_attribute_values = %s" % retval)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_ah_get_platform_attribute_values_invalid_attributes(self):
        platform_id = self.PLATFORM_ID
        attrNames = BOGUS_ATTR_NAMES
        cur_time = ntplib.system_to_ntp_time(time.time())
        from_time = cur_time - 50  # a 50-sec time window
        req_attrs = [(attr_id, from_time) for attr_id in attrNames]
        retval = self.oms.get_platform_attribute_values(platform_id, req_attrs)
        log.info("get_platform_attribute_values = %s" % retval)
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
        retval = self.oms.set_platform_attribute_values(platform_id, attrs)
        log.info("set_platform_attribute_values = %s" % retval)
        vals = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(vals, dict)
        for attrName in attrNames:
            if attrName in self.WRITABLE_ATTR_NAMES:
                self._verify_valid_attribute_id(attrName, vals)
            else:
                self._verify_not_writable_attribute_id(attrName, vals)

    def _get_platform_ports(self, platform_id):
        retval = self.oms.get_platform_ports(platform_id)
        log.info("get_platform_ports(%r) = %s" % (platform_id, retval))
        ports = self._verify_valid_platform_id(platform_id, retval)
        return ports

    def test_ak_get_platform_ports(self):
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)
        for port_id, info in ports.iteritems():
            self.assertIsInstance(info, dict)
            self.assertTrue('network' in info)
            self.assertTrue('is_on' in info)

    def test_ak_get_platform_ports_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.get_platform_ports(platform_id)
        log.info("get_platform_ports = %s" % retval)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_am_connect_instrument(self):
        platform_id = self.PLATFORM_ID
        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID
        # TODO proper values
        attributes = {'maxCurrentDraw': 1, 'initCurrent': 2,
                      'dataThroughput': 3, 'instrumentType': 'FOO'}
        retval = self.oms.connect_instrument(platform_id, port_id, instrument_id, attributes)
        log.info("connect_instrument = %s" % retval)
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

    def test_am_connect_instrument_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        port_id = self.PORT_ID
        instrument_id = self.INSTRUMENT_ID
        attributes = {}
        retval = self.oms.connect_instrument(platform_id, port_id, instrument_id, attributes)
        log.info("connect_instrument = %s" % retval)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_am_connect_instrument_invalid_port_id(self):
        platform_id = self.PLATFORM_ID
        port_id = BOGUS_PORT_ID
        instrument_id = self.INSTRUMENT_ID
        attributes = {}
        retval = self.oms.connect_instrument(platform_id, port_id, instrument_id, attributes)
        log.info("connect_instrument = %s" % retval)
        ports = self._verify_valid_platform_id(platform_id, retval)
        self._verify_invalid_port_id(port_id, ports)

    def test_am_connect_instrument_invalid_instrument_id(self):
        platform_id = self.PLATFORM_ID
        port_id = self.PORT_ID
        instrument_id = BOGUS_INSTRUMENT_ID
        attributes = {}
        retval = self.oms.connect_instrument(platform_id, port_id, instrument_id, attributes)
        log.info("connect_instrument = %s" % retval)
        ports = self._verify_valid_platform_id(platform_id, retval)
        port_dic = self._verify_valid_port_id(port_id, ports)
        self.assertIsInstance(port_dic, dict)
        self._verify_invalid_instrument_id(instrument_id, port_dic)

    def test_an_turn_on_platform_port(self):
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)
        for port_id in ports.iterkeys():
            retval = self.oms.turn_on_platform_port(platform_id, port_id)
            log.info("turn_on_platform_port(%s,%s) = %s" % (platform_id, port_id, retval))
            portRes = self._verify_valid_platform_id(platform_id, retval)
            res = self._verify_valid_port_id(port_id, portRes)
            self.assertEquals(res, NormalResponse.PORT_TURNED_ON)

    def test_an_turn_on_platform_port_invalid_platform_id(self):
        # use valid for get_platform_ports
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)

        # use invalid for turn_on_platform_port
        requested_platform_id = BOGUS_PLATFORM_ID
        for port_id in ports.iterkeys():
            retval = self.oms.turn_on_platform_port(requested_platform_id, port_id)
            log.info("turn_on_platform_port(%s,%s) = %s" % (requested_platform_id, port_id, retval))
            self._verify_invalid_platform_id(requested_platform_id, retval)

    def test_ao_turn_off_platform_port(self):
        platform_id = self.PLATFORM_ID
        ports = self._get_platform_ports(platform_id)
        for port_id in ports.iterkeys():
            retval = self.oms.turn_off_platform_port(platform_id, port_id)
            log.info("turn_off_platform_port(%s,%s) = %s" % (platform_id, port_id, retval))
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
            retval = self.oms.turn_off_platform_port(requested_platform_id, port_id)
            log.info("turn_off_platform_port(%s,%s) = %s" % (requested_platform_id, port_id, retval))
            self._verify_invalid_platform_id(requested_platform_id, retval)

    ###################################################################
    # EVENTS
    ###################################################################

    # { (url, event_type): [event_instance, ...], ...}
    _notifications = {}
    _http_server = None

    @classmethod
    def start_http_server(cls, host='localhost', port=0):
        """
        A subclass call this to start a server that handles the notification
        of events keeping record of them in the member _notifications, which
        can be consulted directly and it is also returned by stop_http_server.
        """
        def application(environ, start_response):
            input = environ['wsgi.input']
            body = "\n".join(input.readlines())
            event_instance = yaml.load(body)
            log.info('notification received event_instance=%s' % str(event_instance))
            if not 'url' in event_instance:
                log.warn("expecting 'url' entry in notification call")
                return
            if not 'ref_id' in event_instance:
                log.warn("expecting 'ref_id' entry in notification call")
                return

            url = event_instance['url']
            event_type = event_instance['ref_id']

            if (url, event_type) in cls._notifications:
                cls._notifications[(url, event_type)].append(event_instance)
            else:
                cls._notifications[(url, event_type)] = [event_instance]

            status = '200 OK'
            headers = [('Content-Type', 'text/plain')]
            start_response(status, headers)
            return event_type

        cls._notifications = {}
        cls._http_server = WSGIServer((host, port), application)
        log.info("HTTP SERVER: starting http server for receiving event notifications...")
        cls._http_server.start()
        address = cls._http_server.address
        log.info("HTTP SERVER: ... http server started: address: host=%r port=%r" % address)

    @classmethod
    def __get_url(cls):
        """
        Used here to compose the URL for various tests. This URL is based on the
        HTTP server launched via start_http_server (if called), or just an ad hoc url.
        """
        if cls._http_server:
            # use actual URL corresponding to the launched server
            url = "http://%s:%s" % cls._http_server.address
        else:
            # use ad hoc url
            url = "http://localhost:9753"
        return url

    @classmethod
    def stop_http_server(cls):
        """
        Stops the http server returning the notifications dictionary,
        which is internally re-initialized.
        """
        if cls._http_server:
            address = cls._http_server.address
            log.info("HTTP SERVER: stopping http server: address: host=%r port=%r" % address)
            cls._http_server.stop()
            cls._http_server = None

        ret = cls._notifications
        cls._notifications = {}  # re-initialize
        return ret

    def _get_all_event_types(self):
        all_events = self.oms.describe_event_types([])
        self.assertIsInstance(all_events, dict)
        log.info('all_events = %s' % all_events)
        return all_events

    def test_ba_describe_event_types(self):
        all_events = self._get_all_event_types()
        if len(all_events) == 0:
            return

        # get a specific event
        event_type_id = all_events.keys()[0]
        events = self.oms.describe_event_types([event_type_id])
        self.assertIsInstance(events, dict)
        self.assertEquals(len(events), 1)
        self.assertTrue(event_type_id in events)

    def test_bb_describe_event_types_invalid_event_type(self):
        event_type_id = BOGUS_EVENT_TYPE
        events = self.oms.describe_event_types([event_type_id])
        self.assertIsInstance(events, dict)
        self.assertEquals(len(events), 1)
        self.assertTrue(event_type_id in events)
        self.assertEquals(InvalidResponse.EVENT_TYPE, events[event_type_id])

    def test_bc_get_events_by_platform_type(self):
        # get all event types
        all_events = self.oms.get_events_by_platform_type([])
        self.assertIsInstance(all_events, dict)

        log.info('all_events = %s' % all_events)
        if len(all_events) == 0:
            return

        # arbitrarily get first platform type again
        platform_type = all_events.keys()[0]
        events = self.oms.get_events_by_platform_type([platform_type])
        self.assertIsInstance(events, dict)
        self.assertEquals(len(events), 1)
        self.assertTrue(platform_type in events)
        self.assertEquals(all_events[platform_type], events[platform_type])

    def test_bd_get_events_by_platform_type_invalid_platform_type(self):
        platform_type = "bogus_platform_type"
        events = self.oms.get_events_by_platform_type([platform_type])
        self.assertIsInstance(events, dict)
        self.assertEquals(len(events), 1)
        self.assertTrue(platform_type in events)
        self.assertEquals(InvalidResponse.PLATFORM_TYPE, events[platform_type])

    def _get_registered_event_listeners(self):
        listeners = self.oms.get_registered_event_listeners()
        log.info("get_registered_event_listeners returned %s" % str(listeners))
        self.assertIsInstance(listeners, dict)
        return listeners

    def _register_event_listener(self, url, event_types):
        result = self.oms.register_event_listener(url, event_types)
        log.info("register_event_listener returned %s" % str(result))
        self.assertIsInstance(result, dict)
        self.assertEquals(len(result), 1)
        self.assertTrue(url in result)
        return result[url]

    def _register_one_event_listener(self):
        all_events = self._get_all_event_types()
        if len(all_events) == 0:
            log.info("WARNING: No event types reported so not registering any listener")
            return None

        # arbitrarily pick first event type
        event_type_id = all_events.keys()[0]

        url = self.__get_url()
        res = self._register_event_listener(url, [event_type_id])[0]

        # check that it's registered
        listeners = self._get_registered_event_listeners()
        self.assertTrue(url in listeners)

        log.info("_register_one_event_listener: res=%s" % str(res))
        return url, res

    def test_be_register_event_listener(self):
        self._register_one_event_listener()

        if self._http_server:
            # wait for a bit to see if we get some notifications
            log.info("waiting for possible event notifications...")
            time.sleep(6)

    def test_bf_register_event_listener_invalid_event_type(self):
        url = self.__get_url()
        event_type_id = BOGUS_EVENT_TYPE
        res = self._register_event_listener(url, [event_type_id])
        self.assertEquals(len(res), 1)
        self.assertEquals((event_type_id, InvalidResponse.EVENT_TYPE), tuple(res[0]))

        # check that it's registered
        listeners = self._get_registered_event_listeners()
        self.assertTrue(url in listeners)

    def test_bg_get_registered_event_listeners(self):
        self._get_registered_event_listeners()

    def _unregister_event_listener(self, url, event_types):
        result = self.oms.unregister_event_listener(url, event_types)
        log.info("unregister_event_listener returned %s" % str(result))
        self.assertIsInstance(result, dict)
        self.assertEquals(len(result), 1)
        self.assertTrue(url in result)
        return result[url]

    def test_bh_unregister_event_listener(self):
        result = self._get_registered_event_listeners()
        if len(result) == 0:
            log.info("WARNING: No event listeners to unregister")
            return

        url = result.keys()[0]
        event_pairs = result[url]
        self.assertTrue(len(event_pairs) > 0)
        event_type_id, time = event_pairs[0]
        self._unregister_event_listener(url, [event_type_id])

        # check that it's unregistered
        listeners = self._get_registered_event_listeners()
        self.assertTrue(url not in listeners)

    def test_bi_unregister_event_listener_not_registered_url(self):
        url = "http://_never_registered_url"
        event_type_id = "dummy_event_type"
        res = self._unregister_event_listener(url, [event_type_id])
        self.assertEquals(InvalidResponse.EVENT_LISTENER_URL, res)

    def test_bj_unregister_event_listener_invalid_event_type(self):
        reg_res = None
        result = self._get_registered_event_listeners()
        if len(result) == 0:
            # register one
            url, reg_res = self._register_one_event_listener()
        else:
            url = result.keys()[0]

        # here we have a valid url; now use a bogus event type:
        log.info("unregistering %s" % BOGUS_EVENT_TYPE)
        res = self._unregister_event_listener(url, [BOGUS_EVENT_TYPE])
        self.assertEquals((BOGUS_EVENT_TYPE, InvalidResponse.EVENT_TYPE), tuple(res[0]))

        if reg_res:
            # unregister the one created above
            event_type_id, reg_time = reg_res
            log.info("unregistering %s" % event_type_id)
            self._unregister_event_listener(url, [event_type_id])

        self._get_registered_event_listeners()

    def test_get_checksum(self):
        platform_id = self.PLATFORM_ID
        retval = self.oms.get_checksum(platform_id)
        log.info("get_checksum = %s" % retval)
