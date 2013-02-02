#!/usr/bin/env python

"""
@package ion.agents.platform.test.helper
@file    ion/agents/platform/test/helper.py
@author  Carlos Rueda
@brief   Definitions and functionality to facilitate common validations in tests.
         The expected structures and error responses are currently based on the
         CI-OMS interface. This scheme may of course need to be adjusted according
         to needed refactorings when CG or other platform networks are incorporated.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.oms.simulator.logger import Logger
log = Logger.get_logger()

from ion.agents.platform.oms.oms_client import InvalidResponse


class HelperTestMixin:
    """
    A mixin to facilitate common validations in tests.
    """

    @classmethod
    def setUpClass(cls):
        """
        Sets some various IDs from network.yml, which is used by the OMS
        simulator, and ad hoc values for testing.
        """
        cls.PLATFORM_ID = 'Node1A'
        cls.SUBPLATFORM_IDS = ['MJ01A', 'Node1B']
        cls.ATTR_NAMES = ['input_voltage', 'Node1A_attr_2']
        cls.WRITABLE_ATTR_NAMES = ['Node1A_attr_2']
        cls.VALID_ATTR_VALUE = "7"  # within the range
        cls.INVALID_ATTR_VALUE = "9876"  # out of range

        cls.PORT_ID = 'Node1A_port_1'
        cls.PORT_ATTR_NAME = 'maxCurrentDraw'
        cls.VALID_PORT_ATTR_VALUE = 12345

        import os
        if os.getenv('SMALL_PLATFORM_NETWORK') is not None:
            # This env variable helps use a smaller network locally.
            print("SMALL_PLATFORM_NETWORK")
            cls.PLATFORM_ID = 'Node1D'
            cls.SUBPLATFORM_IDS = ['MJ01C']
            cls.ATTR_NAMES = ['input_voltage', 'Input Bus Current']
            cls.WRITABLE_ATTR_NAMES = ['Input Bus Current']

            cls.PORT_ID = 'Node1D_port_1'

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
        self.assertTrue(attr_id in dic, "%s in %s" %(attr_id, dic))
        val = dic[attr_id]
        self.assertIsInstance(val, (tuple, list))
        self.assertNotEquals(InvalidResponse.ATTRIBUTE_NAME, val)
        return val

    def _verify_invalid_attribute_id(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        value equal to InvalidResponse.ATTRIBUTE_NAME
        """
        self.assertTrue(attr_id in dic)
        val = dic[attr_id]
        self.assertEquals(InvalidResponse.ATTRIBUTE_NAME, val,
                          "attr_id=%r, val=%r" % (attr_id, val))

    def _verify_attribute_value_out_of_range(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        value equal to InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE
        """
        self.assertTrue(attr_id in dic)
        val = dic[attr_id]
        self.assertEquals(InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE, val,
                          "attr_id=%r, val=%r" % (attr_id, val))

    def _verify_not_writable_attribute_id(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        value equal to InvalidResponse.ATTRIBUTE_NOT_WRITABLE.
        """
        self.assertTrue(attr_id in dic)
        val = dic[attr_id]
        self.assertEquals(InvalidResponse.ATTRIBUTE_NOT_WRITABLE, val,
                          "attr_id=%r, val=%r" % (attr_id, val))

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
