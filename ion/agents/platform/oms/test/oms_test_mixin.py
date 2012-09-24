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


from ion.agents.platform.oms.oms_client import InvalidResponse

import time

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
        self.assertEquals(InvalidResponse.ATTRIBUTE_NAME_VALUE, val)

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
        print("getPlatformTypes = %s" % retval)
        self.assertIsInstance(retval, dict)
        for k, v in retval.iteritems():
            self.assertIsInstance(k, str)
            self.assertIsInstance(v, str)

    def test_ad_getPlatformMetadata(self):
        platform_id = PLATFORM_ID
        retval = self.oms.config.getPlatformMetadata(platform_id)
        print("getPlatformMetadata(%r) = %s" % (platform_id,  retval))
        md = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(md, dict)
        self.assertTrue('platform_types' in md)

    def test_ad_getPlatformMetadata_invalid(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.config.getPlatformMetadata(platform_id)
        print("getPlatformMetadata(%r) = %s" % (platform_id,retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_af_getPlatformAttributes(self):
        platform_id = PLATFORM_ID
        retval = self.oms.config.getPlatformAttributes(platform_id)
        print("getPlatformAttributes(%r) = %s" % (platform_id, retval))
        infos = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(infos, dict)

    def test_ag_getPlatformAttributes_invalid(self):
        platform_id = BOGUS_PLATFORM_ID
        retval = self.oms.config.getPlatformAttributes(platform_id)
        print("getPlatformAttributes(%r) = %s" % (platform_id, retval))
        self._verify_invalid_platform_id(platform_id, retval)

    def test_ah_getPlatformAttributeValues(self):
        platform_id = PLATFORM_ID
        attrNames = ATTR_NAMES
        from_time = time.time()
        retval = self.oms.getPlatformAttributeValues(platform_id, attrNames, from_time)
        print("getPlatformAttributeValues = %s" % retval)
        vals = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(vals, dict)
        for attrName in attrNames:
            self._verify_valid_attribute_id(attrName, vals)

    def test_ah_getPlatformAttributeValues_invalid_platform_id(self):
        platform_id = BOGUS_PLATFORM_ID
        attrNames = ATTR_NAMES
        from_time = time.time()
        retval = self.oms.getPlatformAttributeValues(platform_id, attrNames, from_time)
        print("getPlatformAttributeValues = %s" % retval)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_ah_getPlatformAttributeValues_invalid_attributes(self):
        platform_id = PLATFORM_ID
        attrNames = BOGUS_ATTR_NAMES
        from_time = time.time()
        retval = self.oms.getPlatformAttributeValues(platform_id, attrNames, from_time)
        print("getPlatformAttributeValues = %s" % retval)
        vals = self._verify_valid_platform_id(platform_id, retval)
        self.assertIsInstance(vals, dict)
        for attrName in attrNames:
            self._verify_invalid_attribute_id(attrName, vals)

    def _getPlatformPorts(self, platform_id):
        retval = self.oms.getPlatformPorts(platform_id)
        print("getPlatformPorts(%r) = %s" % (platform_id, retval))
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
        print("getPlatformPorts = %s" % retval)
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
        print("setUpPort = %s" % retval)
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
        print("setUpPort = %s" % retval)
        self._verify_invalid_platform_id(platform_id, retval)

    def test_am_setUpPort_invalid_port_id(self):
        platform_id = PLATFORM_ID
        port_id = BOGUS_PORT_ID
        attributes = {}
        retval = self.oms.setUpPort(platform_id, port_id, attributes)
        print("setUpPort = %s" % retval)
        ports = self._verify_valid_platform_id(platform_id, retval)
        self._verify_invalid_port_id(port_id, ports)

    def test_an_turnOnPort(self):
        platform_id = PLATFORM_ID
        ports = self._getPlatformPorts(platform_id)
        for port_id in ports.iterkeys():
            retval = self.oms.turnOnPort(platform_id, port_id)
            print("turnOnPort(%s,%s) = %s" % (platform_id, port_id, retval))
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
            print("turnOnPort(%s,%s) = %s" % (requested_platform_id, port_id, retval))
            self._verify_invalid_platform_id(requested_platform_id, retval)

    def test_ao_turnOffPort(self):
        platform_id = PLATFORM_ID
        ports = self._getPlatformPorts(platform_id)
        for port_id in ports.iterkeys():
            retval = self.oms.turnOffPort(platform_id, port_id)
            print("turnOffPort(%s,%s) = %s" % (platform_id, port_id, retval))
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
            print("turnOffPort(%s,%s) = %s" % (requested_platform_id, port_id, retval))
            self._verify_invalid_platform_id(requested_platform_id, retval)
