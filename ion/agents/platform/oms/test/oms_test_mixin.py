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


import time


class OmsTestMixin(object):
    """
    A mixin to facilitate test cases for OMS objects following the OMS-CI interface.
    """

    def test_aa_ping(self):
        response = self.oms.hello.ping()
        self.assertEquals(response, "pong")

    def test_ac_getPlatformMap(self):
        platform_map = self.oms.config.getPlatformMap()
        self.assertIsInstance(platform_map, list)
        for pair in platform_map:
            self.assertIsInstance(pair, (tuple, list))

    def test_ad_getPlatformAttributeNames(self):
        attrNames = self.oms.getPlatformAttributeNames('platA')
        print("getPlatformAttributeNames = %s" % attrNames)
        self.assertIsInstance(attrNames, list)

    def test_ae_getPlatformAttributeValues(self):
        platAttrMap = {'platA': ['bazA', 'fooA']}
        from_time = time.time()
        retval = self.oms.getPlatformAttributeValues(platAttrMap, from_time)
        print("getPlatformAttributeValues = %s" % retval)
        self.assertIsInstance(retval, dict)
        self.assertTrue('platA' in retval)

    def test_af_getPlatformAttributeInfos(self):
        platAttrMap = {'platA': ['bazA', 'fooA']}
        retval = self.oms.getPlatformAttributeInfo(platAttrMap)
        print("getPlatformAttributeInfo = %s" % retval)

        self.assertIsInstance(retval, dict)
        self.assertTrue('platA' in retval)

        platA = retval['platA']
        self.assertIsInstance(platA, dict)
        self.assertTrue('bazA' in platA)

        bazA = platA['bazA']
        self.assertIsInstance(bazA, dict)
        self.assertTrue('monitorCycleSeconds' in bazA)

    def test_ag_getPlatformPorts(self):
        retval = self.oms.getPlatformPorts('platA')
        print("getPlatformPorts = %s" % retval)

        self.assertIsInstance(retval, list)

    def test_ah_getPortInfo(self):
        port_ids = self.oms.getPlatformPorts('platA')
        for port_id in port_ids:
            port_info = self.oms.getPortInfo('platA', port_id)
            print("getPortInfo(%s,%s) = %s" % ('platA', port_id, port_info))
            self.assertIsInstance(port_info, dict)
            self.assertTrue('ip' in port_info)

    def test_ai_setUpPort(self):
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

    def test_aj_turnOnPort(self):
        port_ids = self.oms.getPlatformPorts('platA')
        for port_id in port_ids:
            retval = self.oms.turnOnPort('platA', port_id)
            print("turnOnPort(%s,%s) = %s" % ('platA', port_id, retval))
            self.assertTrue(retval)

    def test_ak_turnOffPort(self):
        port_ids = self.oms.getPlatformPorts('platA')
        for port_id in port_ids:
            retval = self.oms.turnOffPort('platA', port_id)
            print("turnOffPort(%s,%s) = %s" % ('platA', port_id, retval))
            self.assertFalse(retval)
