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


class OmsTestMixin(object):
    """
    A mixin to facilitate test cases for OMS objects following the OMS-CI interface.
    """

    def test_aa_ping(self):
        response = self.oms.hello.ping()
        self.assertEquals(response, "pong")

    def test_ab_getPlatformMap(self):
        platform_map = self.oms.config.getPlatformMap()
        self.assertIsInstance(platform_map, list)
        for pair in platform_map:
            self.assertIsInstance(pair, (tuple, list))

    def test_ac_getPlatformTypes(self):
        retval = self.oms.config.getPlatformTypes()
        print("getPlatformTypes = %s" % retval)
        self.assertIsInstance(retval, dict)
        for k, v in retval.iteritems():
            self.assertIsInstance(k, str)
            self.assertIsInstance(v, str)

    def test_ad_getPlatformMetadata(self):
        platform_id = 'platA'
        retval = self.oms.config.getPlatformMetadata(platform_id)
        print("getPlatformMetadata(%r) = %s" % (platform_id,  retval))
        self.assertIsInstance(retval, dict)
        self.assertTrue(platform_id in retval)
        self.assertEquals(1, len(retval))
        md = retval[platform_id]
        self.assertIsInstance(md, dict)
        self.assertTrue('platform_types' in md)

    def test_ad_getPlatformMetadata_invalid(self):
        platform_id = 'bogus_plat_id'
        retval = self.oms.config.getPlatformMetadata(platform_id)
        print("getPlatformMetadata(%r) = %s" % (platform_id,retval))
        self.assertIsInstance(retval, dict)
        self.assertTrue(platform_id in retval)
        self.assertEquals(1, len(retval))
        self.assertEquals(InvalidResponse.PLATFORM_ID, retval[platform_id])

    def test_af_getPlatformAttributes(self):
        platform_id = 'platA'
        retval = self.oms.config.getPlatformAttributes(platform_id)
        print("getPlatformAttributes(%r) = %s" % (platform_id, retval))
        self.assertIsInstance(retval, dict)
        self.assertTrue(platform_id in retval)
        self.assertEquals(1, len(retval))
        infos = retval[platform_id]
        self.assertIsInstance(infos, dict)

    def test_ag_getPlatformAttributes_invalid(self):
        platform_id = 'bogus_plat_id'
        retval = self.oms.config.getPlatformAttributes(platform_id)
        print("getPlatformAttributes(%r) = %s" % (platform_id, retval))
        self.assertIsInstance(retval, dict)
        self.assertTrue(platform_id in retval)
        self.assertEquals(1, len(retval))
        self.assertEquals(InvalidResponse.PLATFORM_ID, retval[platform_id])

    def test_ah_getPlatformAttributeValues(self):
        platAttrMap = {'platA': ['bazA', 'fooA']}
        from_time = time.time()
        retval = self.oms.getPlatformAttributeValues(platAttrMap, from_time)
        print("getPlatformAttributeValues = %s" % retval)
        self.assertIsInstance(retval, dict)
        self.assertTrue('platA' in retval)

    def test_ak_getPlatformPorts(self):
        retval = self.oms.getPlatformPorts('platA')
        print("getPlatformPorts = %s" % retval)

        self.assertIsInstance(retval, list)

    def test_al_getPortInfo(self):
        port_ids = self.oms.getPlatformPorts('platA')
        for port_id in port_ids:
            port_info = self.oms.getPortInfo('platA', port_id)
            print("getPortInfo(%s,%s) = %s" % ('platA', port_id, port_info))
            self.assertIsInstance(port_info, dict)
            self.assertTrue('ip' in port_info)

    def test_am_setUpPort(self):
        # TODO proper attributes and values
        valid_attributes = {'maxCurrentDraw': 1, 'initCurrent': 2,
                      'dataThroughput': 3, 'instrumentType': 'FOO'}
        invalid_attributes = {'invalid' : 'dummy'}
        all_attributes = valid_attributes.copy()
        all_attributes.update(invalid_attributes)
        retval = self.oms.setUpPort('platA', 'portA_1', all_attributes)
        print("setUpPort = %s" % retval)
        self.assertIsInstance(retval, dict)
        for attr_name in valid_attributes:
            self.assertTrue(attr_name in retval)
        for attr_name in invalid_attributes:
            self.assertFalse(attr_name in retval)

    def test_an_turnOnPort(self):
        port_ids = self.oms.getPlatformPorts('platA')
        for port_id in port_ids:
            retval = self.oms.turnOnPort('platA', port_id)
            print("turnOnPort(%s,%s) = %s" % ('platA', port_id, retval))
            self.assertTrue(retval)

    def test_ao_turnOffPort(self):
        port_ids = self.oms.getPlatformPorts('platA')
        for port_id in port_ids:
            retval = self.oms.turnOffPort('platA', port_id)
            print("turnOffPort(%s,%s) = %s" % ('platA', port_id, retval))
            self.assertFalse(retval)
