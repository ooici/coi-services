#!/usr/bin/env python

"""
@package ion.agents.platform.oms.test.oms_test_mixin
@file    ion/agents/platform/oms/test/oms_test_mixin.py
@author  Carlos Rueda
@brief   A mixin to facilitate test cases for OMS objects following the
         OMS-CI interface.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.oms.simulator.logger import Logger
log = Logger.get_logger()

from ion.agents.platform.oms.oms_client import InvalidResponse

import time
from gevent.pywsgi import WSGIServer
import yaml


# Some IDs used in the tests (which must be defined in network.yml
# if testing against the simulator)
PLATFORM_ID = 'Node1A'
SUBPLATFORM_IDS = ['MJ01A', 'Node1B']
ATTR_NAMES = ['Node1A_attr_1', 'Node1A_attr_2']
PORT_ID = 'Node1A_port_1'

# some bogus IDs
BOGUS_PLATFORM_ID = 'bogus_plat_id'
BOGUS_ATTR_NAMES = ['bogus_attr1', 'bogus_attr2']
BOGUS_PORT_ID = 'bogus_port_id'
BOGUS_ALARM_TYPE = "bogus_alarm_type"


class OmsTestMixin(object):
    """
    A mixin to facilitate test cases for OMS objects following the OMS-CI interface.
    """

    def _verify_valid_platform_id(self, platform_id, dic):
        """
        verifies the platform_id is the only entry in the dict with a
        valid value. Returns dic[platform_id].
        """
        self.assertTrue(platform_id in dic)
        self.assertEquals(1, len(dic))
        val = dic[platform_id]
        self.assertNotEquals(InvalidResponse.PLATFORM_ID, val)
        return val

    def _verify_invalid_platform_id(self, platform_id, dic):
        """
        verifies the platform_id is the only entry in the dict with a
        value equal to InvalidResponse.PLATFORM_ID.
        """
        self.assertTrue(platform_id in dic)
        self.assertEquals(1, len(dic))
        val = dic[platform_id]
        self.assertEquals(InvalidResponse.PLATFORM_ID, val)

    def _verify_valid_attribute_id(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        valid value. Returns dic[attr_id].
        """
        self.assertTrue(attr_id in dic)
        val = dic[attr_id]
        self.assertIsInstance(val, (tuple, list))
        self.assertNotEquals(InvalidResponse.ATTRIBUTE_NAME_VALUE, val)
        return val

    def _verify_invalid_attribute_id(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        value equal to InvalidResponse.ATTRIBUTE_NAME_VALUE.
        """
        self.assertTrue(attr_id in dic)
        val = dic[attr_id]
        self.assertIsInstance(val, (tuple, list))
        self.assertEquals(InvalidResponse.ATTRIBUTE_NAME_VALUE, tuple(val))

    def _verify_valid_port_id(self, port_id, dic):
        """
        verifies the port_id is an entry in the dict with a
        valid value. Returns dic[port_id].
        """
        self.assertTrue(port_id in dic)
        val = dic[port_id]
        self.assertNotEquals(InvalidResponse.PORT_ID, val)
        return val

    def _verify_invalid_port_id(self, port_id, dic):
        """
        verifies the port_id is an entry in the dict with a
        value equal to InvalidResponse.PORT_ID.
        """
        self.assertTrue(port_id in dic)
        val = dic[port_id]
        self.assertEquals(InvalidResponse.PORT_ID, val)

    def test_aa_ping(self):
        response = self.oms.hello.ping()
        self.assertEquals(response, "pong")

    def test_ab_getPlatformMap(self):
        platform_map = self.oms.config.getPlatformMap()
        self.assertIsInstance(platform_map, list)
        for pair in platform_map:
            self.assertIsInstance(pair, (tuple, list))

    def test_ab_getRootPlatformID(self):
        platform_id = self.oms.config.getRootPlatformID()
        self.assertEquals("ShoreStation", platform_id)

    def test_ab_getSubplatformIDs(self):
        platform_id = PLATFORM_ID
        retval = self.oms.config.getSubplatformIDs(platform_id)
        print("getSubplatformIDs(%r) = %s" % (platform_id,  retval))
        subplatform_ids = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(subplatform_ids, list)
        self.assertTrue(x in subplatform_ids for x in SUBPLATFORM_IDS)

    def test_ac_getPlatformTypes(self):
        retval = self.oms.config.getPlatformTypes()
        log.info("getPlatformTypes = %s" % retval)
        self.assertIsInstance(retval, dict)
        for k, v in retval.iteritems():
            self.assertIsInstance(k, str)
            self.assertIsInstance(v, str)

    def test_ad_getPlatformMetadata(self):
        platform_id = PLATFORM_ID
        retval = self.oms.config.getPlatformMetadata(platform_id)
        log.info("getPlatformMetadata(%r) = %s" % (platform_id,  retval))
        md = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(md, dict)
        self.assertTrue('platform_types' in md)

    def test_ad_getPlatformMetadata_invalid(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.config.getPlatformMetadata(platform_id)
        log.info("getPlatformMetadata(%r) = %s" % (platform_id,retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_af_getPlatformAttributes(self):
        platform_id = PLATFORM_ID
        retval = self.oms.config.getPlatformAttributes(platform_id)
        log.info("getPlatformAttributes(%r) = %s" % (platform_id, retval))
        infos = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(infos, dict)

    def test_ag_getPlatformAttributes_invalid(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.config.getPlatformAttributes(platform_id)
        log.info("getPlatformAttributes(%r) = %s" % (platform_id, retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_ah_getPlatformAttributeValues(self):
        platform_id = PLATFORM_ID
        attrNames = ATTR_NAMES
        from_time = time.time()
        retval = self.oms.getPlatformAttributeValues(platform_id, attrNames, from_time)
        log.info("getPlatformAttributeValues = %s" % retval)
        vals = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(vals, dict)
        for attrName in attrNames:
            self._verify_valid_attribute_id(attrName, vals)

    def test_ah_getPlatformAttributeValues_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        attrNames = ATTR_NAMES
        from_time = time.time()
        retval = self.oms.getPlatformAttributeValues(platform_id, attrNames, from_time)
        log.info("getPlatformAttributeValues = %s" % retval)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_ah_getPlatformAttributeValues_invalid_attributes(self):
        platform_id = PLATFORM_ID
        attrNames = BOGUS_ATTR_NAMES
        from_time = time.time()
        retval = self.oms.getPlatformAttributeValues(platform_id, attrNames, from_time)
        log.info("getPlatformAttributeValues = %s" % retval)
        vals = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(vals, dict)
        for attrName in attrNames:
            self._verify_invalid_attribute_id(attrName, vals)

    def _getPlatformPorts(self, platform_id):
        retval = self.oms.getPlatformPorts(platform_id)
        log.info("getPlatformPorts(%r) = %s" % (platform_id, retval))
        ports = self._verify_valid_platform_id(platform_id, retval)
        return ports

    def test_ak_getPlatformPorts(self):
        platform_id = PLATFORM_ID
        ports = self._getPlatformPorts(platform_id)
        for port_id, info in ports.iteritems():
            self.assertIsInstance(info, dict)
            self.assertTrue('attrs' in info)
            self.assertTrue('comms' in info)
            self.assertTrue('ip' in info['comms'])

    def test_ak_getPlatformPorts_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.getPlatformPorts(platform_id)
        log.info("getPlatformPorts = %s" % retval)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_am_setUpPort(self):
        platform_id = PLATFORM_ID
        port_id = PORT_ID
        # TODO proper attributes and values
        valid_attributes = {'maxCurrentDraw': 1, 'initCurrent': 2,
                      'dataThroughput': 3, 'instrumentType': 'FOO'}
        invalid_attributes = {'invalid' : 'dummy'}
        all_attributes = valid_attributes.copy()
        all_attributes.update(invalid_attributes)
        retval = self.oms.setUpPort(platform_id, port_id, all_attributes)
        log.info("setUpPort = %s" % retval)
        ports = self._verify_valid_platform_id(platform_id, retval)
        port_val = self._verify_valid_port_id(port_id, ports)
        self.assertIsInstance(port_val, dict)
        for attr_name in valid_attributes:
            self.assertTrue(attr_name in port_val)
        for attr_name in invalid_attributes:
            self.assertFalse(attr_name in port_val)

    def test_am_setUpPort_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        port_id = PORT_ID
        attributes = {}
        retval = self.oms.setUpPort(platform_id, port_id, attributes)
        log.info("setUpPort = %s" % retval)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_am_setUpPort_invalid_port_id(self):
        platform_id = PLATFORM_ID
        port_id = BOGUS_PORT_ID
        attributes = {}
        retval = self.oms.setUpPort(platform_id, port_id, attributes)
        log.info("setUpPort = %s" % retval)
        ports = self._verify_valid_platform_id(platform_id, retval)
        self._verify_invalid_port_id(port_id, ports)

    def test_an_turnOnPort(self):
        platform_id = PLATFORM_ID
        ports = self._getPlatformPorts(platform_id)
        for port_id in ports.iterkeys():
            retval = self.oms.turnOnPort(platform_id, port_id)
            log.info("turnOnPort(%s,%s) = %s" % (platform_id, port_id, retval))
            portRes = self._verify_valid_platform_id(platform_id, retval)
            res = self._verify_valid_port_id(port_id, portRes)
            self.assertIsInstance(res, bool)
            self.assertTrue(res)

    def test_an_turnOnPort_invalid_platform_id(self):
        # use valid for getPlatformPorts
        platform_id = PLATFORM_ID
        ports = self._getPlatformPorts(platform_id)

        # use invalid for turnOnPort
        requested_platform_id = BOGUS_PLATFORM_ID
        for port_id in ports.iterkeys():
            retval = self.oms.turnOnPort(requested_platform_id, port_id)
            log.info("turnOnPort(%s,%s) = %s" % (requested_platform_id, port_id, retval))
            self._verify_invalid_platform_id(requested_platform_id, retval)

    def test_ao_turnOffPort(self):
        platform_id = PLATFORM_ID
        ports = self._getPlatformPorts(platform_id)
        for port_id in ports.iterkeys():
            retval = self.oms.turnOffPort(platform_id, port_id)
            log.info("turnOffPort(%s,%s) = %s" % (platform_id, port_id, retval))
            portRes = self._verify_valid_platform_id(platform_id, retval)
            res = self._verify_valid_port_id(port_id, portRes)
            self.assertIsInstance(res, bool)
            self.assertFalse(res)

    def test_ao_turnOffPort_invalid_platform_id(self):
        # use valid for getPlatformPorts
        platform_id = PLATFORM_ID
        ports = self._getPlatformPorts(platform_id)

        # use invalid for turnOffPort
        requested_platform_id = BOGUS_PLATFORM_ID
        for port_id in ports.iterkeys():
            retval = self.oms.turnOffPort(requested_platform_id, port_id)
            log.info("turnOffPort(%s,%s) = %s" % (requested_platform_id, port_id, retval))
            self._verify_invalid_platform_id(requested_platform_id, retval)

    ###################################################################
    # ALARMS
    ###################################################################

    # { (url, alarm_type): [alarm_instance, ...], ...}
    _notifications = {}
    _http_server = None

    @classmethod
    def start_http_server(cls, host='localhost', port=0):
        """
        A subclass call this to start a server that handles the notification
        of alarms keeping record of them in the member _notifications, which
        can be consulted directly and it is also returned by stop_http_server.
        """
        def application(environ, start_response):
            input = environ['wsgi.input']
            body = "\n".join(input.readlines())
            alarm_instance = yaml.load(body)
            log.info('notification received alarm_instance=%s' % str(alarm_instance))
            if not 'url' in alarm_instance:
                log.warn("expecting 'url' entry in notification call")
                return
            if not 'ref_id' in alarm_instance:
                log.warn("expecting 'ref_id' entry in notification call")
                return

            url = alarm_instance['url']
            alarm_type = alarm_instance['ref_id']

            if (url, alarm_type) in cls._notifications:
                cls._notifications[(url, alarm_type)].append(alarm_instance)
            else:
                cls._notifications[(url, alarm_type)] = [alarm_instance]

            status = '200 OK'
            headers = [('Content-Type', 'text/plain')]
            start_response(status, headers)
            return alarm_type

        cls._notifications = {}
        cls._http_server = WSGIServer((host, port), application)
        log.info("HTTP SERVER: starting http server for receiving alarm notifications...")
        cls._http_server.start()
        address = cls._http_server.address
        log.info("HTTP SERVER: ... http server started: address: host=%r port=%r" % address)

    @classmethod
    def _get_url(cls):
        """
        Not to be called by subclasses. Rather is used here to compose the
        URL for various tests. This URL is based on the HTTP server launched
        via start_http_server (if called), or just an ad hoc url.
        """
        if cls._http_server:
            # use actual URL corresponding to the launched server
            url = "http://%s:%s" % cls._http_server.address
        else:
            # use ad hoc server.
            url = "http://localhost:9753"
        return url

    @classmethod
    def stop_http_server(cls):
        if cls._http_server:
            address = cls._http_server.address
            log.info("HTTP SERVER: stopping http server: address: host=%r port=%r" % address)
            cls._http_server.stop()
            cls._http_server = None

        return cls._notifications

    def _get_all_alarm_types(self):
        all_alarms = self.oms.describeAlarmTypes([])
        self.assertIsInstance(all_alarms, dict)
        log.info('all_alarms = %s' % all_alarms)
        return all_alarms

    def test_ba_describeAlarmTypes(self):
        all_alarms = self._get_all_alarm_types()
        if len(all_alarms) == 0:
            return

        # get a specific alarm
        alarm_type_id = all_alarms.keys()[0]
        alarms = self.oms.describeAlarmTypes([alarm_type_id])
        self.assertIsInstance(alarms, dict)
        self.assertEquals(len(alarms), 1)
        self.assertTrue(alarm_type_id in alarms)

    def test_bb_describeAlarmTypes_invalid_alarm_type(self):
        alarm_type_id = BOGUS_ALARM_TYPE
        alarms = self.oms.describeAlarmTypes([alarm_type_id])
        self.assertIsInstance(alarms, dict)
        self.assertEquals(len(alarms), 1)
        self.assertTrue(alarm_type_id in alarms)
        self.assertEquals(InvalidResponse.ALARM_TYPE, alarms[alarm_type_id])

    def test_bc_getAlarmsByPlatformType(self):
        # get all alarm types
        all_alarms = self.oms.getAlarmsByPlatformType([])
        self.assertIsInstance(all_alarms, dict)

        log.info('all_alarms = %s' % all_alarms)
        if len(all_alarms) == 0:
            return

        # arbitrarily get first platform type again
        platform_type = all_alarms.keys()[0]
        alarms = self.oms.getAlarmsByPlatformType([platform_type])
        self.assertIsInstance(alarms, dict)
        self.assertEquals(len(alarms), 1)
        self.assertTrue(platform_type in alarms)
        self.assertEquals(all_alarms[platform_type], alarms[platform_type])

    def test_bd_getAlarmsByPlatformType_invalid_platform_type(self):
        platform_type = "bogus_platform_type"
        alarms = self.oms.getAlarmsByPlatformType([platform_type])
        self.assertIsInstance(alarms, dict)
        self.assertEquals(len(alarms), 1)
        self.assertTrue(platform_type in alarms)
        self.assertEquals(InvalidResponse.PLATFORM_TYPE, alarms[platform_type])

    def _get_registered_alarm_listeners(self):
        listeners = self.oms.getRegisteredAlarmListeners()
        log.info("getRegisteredAlarmListeners returned %s" % str(listeners))
        self.assertIsInstance(listeners, dict)
        return listeners

    def _register_alarm_listener(self, url, alarm_types):
        result = self.oms.registerAlarmListener(url, alarm_types)
        log.info("registerAlarmListener returned %s" % str(result))
        self.assertIsInstance(result, dict)
        self.assertEquals(len(result), 1)
        self.assertTrue(url in result)
        return result[url]

    def _register_one_alarm_listener(self):
        all_alarms = self._get_all_alarm_types()
        if len(all_alarms) == 0:
            log.info("WARNING: No alarm types reported so not registering any listener")
            return None

        # arbitrarily pick first alarm type
        alarm_type_id = all_alarms.keys()[0]

        url = self._get_url()
        res = self._register_alarm_listener(url, [alarm_type_id])[0]

        # check that it's registered
        listeners = self._get_registered_alarm_listeners()
        self.assertTrue(url in listeners)

        log.info("_register_one_alarm_listener: res=%s" % str(res))
        return url, res

    def test_be_registerAlarmListener(self):
        self._register_one_alarm_listener()

        if self._http_server:
            # wait for a bit to see if we get some notifications
            log.info("waiting for possible alarm notifications...")
            time.sleep(6)

    def test_bf_registerAlarmListener_invalid_alarm_type(self):
        url = self._get_url()
        alarm_type_id = BOGUS_ALARM_TYPE
        res = self._register_alarm_listener(url, [alarm_type_id])
        self.assertEquals(len(res), 1)
        self.assertEquals((alarm_type_id, InvalidResponse.ALARM_TYPE), tuple(res[0]))

        # check that it's registered
        listeners = self._get_registered_alarm_listeners()
        self.assertTrue(url in listeners)

    def test_bg_getRegisteredAlarmListeners(self):
        self._get_registered_alarm_listeners()

    def _unregister_alarm_listener(self, url, alarm_types):
        result = self.oms.unregisterAlarmListener(url, alarm_types)
        log.info("unregisterAlarmListener returned %s" % str(result))
        self.assertIsInstance(result, dict)
        self.assertEquals(len(result), 1)
        self.assertTrue(url in result)
        return result[url]

    def test_bh_unregisterAlarmListener(self):
        result = self._get_registered_alarm_listeners()
        if len(result) == 0:
            log.info("WARNING: No alarm listeners to unregister")
            return

        url = result.keys()[0]
        alarm_pairs = result[url]
        self.assertTrue(len(alarm_pairs) > 0)
        alarm_type_id, time = alarm_pairs[0]
        self._unregister_alarm_listener(url, [alarm_type_id])

        # check that it's unregistered
        listeners = self._get_registered_alarm_listeners()
        self.assertTrue(url not in listeners)

    def test_bi_unregisterAlarmListener_not_registered_url(self):
        url = "http://_never_registered_url"
        alarm_type_id = "dummy_alarm_type"
        res = self._unregister_alarm_listener(url, [alarm_type_id])
        self.assertEquals(InvalidResponse.ALARM_LISTENER_URL, res)

    def test_bj_unregisterAlarmListener_invalid_alarm_type(self):
        reg_res = None
        result = self._get_registered_alarm_listeners()
        if len(result) == 0:
            # register one
            url, reg_res = self._register_one_alarm_listener()
        else:
            url = result.keys()[0]

        # here we have a valid url; now use a bogus alarm type:
        log.info("unregistering %s" % BOGUS_ALARM_TYPE)
        res = self._unregister_alarm_listener(url, [BOGUS_ALARM_TYPE])
        self.assertEquals((BOGUS_ALARM_TYPE, InvalidResponse.ALARM_TYPE), tuple(res[0]))

        if reg_res:
            # unregister the one created above
            alarm_type_id, reg_time = reg_res
            log.info("unregistering %s" % alarm_type_id)
            self._unregister_alarm_listener(url, [alarm_type_id])

        self._get_registered_alarm_listeners()