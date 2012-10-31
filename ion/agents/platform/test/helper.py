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

#######################################################################
# Various IDs from network.yml, which is used by the OMS simulator, and
# ad hoc values for testing
PLATFORM_ID = 'Node1A'
SUBPLATFORM_IDS = ['MJ01A', 'Node1B']
ATTR_NAMES = ['Node1A_attr_1', 'Node1A_attr_2']
WRITABLE_ATTR_NAMES = ['Node1A_attr_2']
VALID_ATTR_VALUE = "7"  # within the range
INVALID_ATTR_VALUE = "9876"  # out of range

PORT_ID = 'Node1A_port_1'
PORT_ATTR_NAME = 'maxCurrentDraw'
VALID_PORT_ATTR_VALUE = 12345

#######################################################################


class HelperTestMixin:
    """
    A mixin to facilitate common validations in tests.
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
        self.assertTrue(attr_id in dic, "%s in %s" %(attr_id, dic))
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

    def _verify_not_writable_attribute_id(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        value equal to InvalidResponse.ATTRIBUTE_NOT_WRITABLE.
        """
        self.assertTrue(attr_id in dic)
        val = dic[attr_id]
        self.assertEquals(InvalidResponse.ATTRIBUTE_NOT_WRITABLE, val)

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
