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


from ion.agents.platform.rsn.simulator.logger import Logger
log = Logger.get_logger()

from ion.agents.platform.responses import NormalResponse, InvalidResponse


class HelperTestMixin:
    """
    A mixin to facilitate common validations in tests.
    """

    @classmethod
    def setUpClass(cls):
        """
        Sets some various IDs from network.yml, which is used by our RSN OMS
        simulator, and other ad hoc values for testing.

        By default, the following values are relative to the platform with id
        'Node1D' taken as the base platform for the topology under under testing.
        The PLAT_NETWORK environment variable can be used to change this root:

            PLAT_NETWORK=single
                Use network with a single platform (no children)
                corresponding to id 'LJ01D'.
        """

        ######################################################################
        # NOTE: the following values relative to 'Node1A' (which roots a
        # significantly bigger hierarchy) were previously used as the default.
        # It is now enabled at the moment (and probably it won't eventually)
        # pending alignment with the new platform configuration structure.
        #
        # cls.PLATFORM_ID          = 'Node1A'
        # cls.SUBPLATFORM_IDS      = ['MJ01A', 'Node1B']
        # cls.ATTR_NAMES           = ['input_voltage', 'Node1A_attr_2']
        # cls.WRITABLE_ATTR_NAMES  = ['Node1A_attr_2']
        # cls.VALID_ATTR_VALUE     = "7"       # within the range
        # cls.INVALID_ATTR_VALUE   = "9876"  # out of range
        #
        # cls.PORT_ID              = 'Node1A_port_1'
        # cls.INSTRUMENT_ID        = 'Node1A_port_1_instrument_1'
        # cls.INSTRUMENT_ATTR_NAME = 'maxCurrentDraw'
        # cls.VALID_INSTRUMENT_ATTR_VALUE = 12345
        ######################################################################

        cls.PLATFORM_ID           = 'Node1D'
        cls.SUBPLATFORM_IDS       = ['MJ01C']
        cls.ATTR_NAMES            = ['input_voltage', 'input_bus_current']
        cls.WRITABLE_ATTR_NAMES   = ['input_bus_current']
        cls.VALID_ATTR_VALUE      = "7"     # within the range
        cls.INVALID_ATTR_VALUE    = "9876"  # out of range

        cls.PORT_ID               = 'Node1D_port_1'
        cls.INSTRUMENT_ID         = 'Node1D_port_1_instrument_1'
        cls.INSTRUMENT_ATTR_NAME  = 'maxCurrentDraw'
        cls.VALID_INSTRUMENT_ATTR_VALUE = 12345

        cls.INSTRUMENT_ATTRIBUTES_AND_VALUES = {
            'maxCurrentDraw' : 12345,
            'initCurrent'    : 23456,
            'dataThroughput' : 34567,
            'instrumentType' : "FOO_INSTRUMENT_TYPE"
        }

        import os
        plat_network_size = os.getenv('PLAT_NETWORK', None)
        if "single" == plat_network_size:
            #
            # network with just a single platform (no children).
            #
            cls.PLATFORM_ID = 'LJ01D'
            print("PLAT_NETWORK=single -> using base platform: %r" % cls.PLATFORM_ID)
            cls.SUBPLATFORM_IDS = []
            cls.ATTR_NAMES = ['input_voltage', 'input_bus_current']
            cls.WRITABLE_ATTR_NAMES = ['input_bus_current']

            cls.PORT_ID = 'LJ01D_port_1'
            cls.INSTRUMENT_ID = 'LJ01D_port_1_instrument_1'
        else:
            print("PLAT_NETWORK undefined -> using base platform: %r" % cls.PLATFORM_ID)

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

    def _verify_valid_instrument_id(self, instrument_id, dic):
        """
        verifies the instrument_id is an entry in the dict with a valid value,
        either a dict or InvalidResponse.INSTRUMENT_ALREADY_CONNECTED.
        Returns dic[instrument_id].
        """
        self.assertTrue(instrument_id in dic)
        val = dic[instrument_id]
        self.assertTrue(
            isinstance(val, dict) or
            val == InvalidResponse.INSTRUMENT_ALREADY_CONNECTED,
            "%r: val should be a dict but is: %s" % (
                instrument_id, str(val)))
        return val

    def _verify_invalid_instrument_id(self, instrument_id, dic):
        """
        verifies the instrument_id is an entry in the dict with a
        value that is not a dict.
        """
        self.assertTrue(instrument_id in dic)
        val = dic[instrument_id]
        self.assertFalse(isinstance(val, dict))

    def _verify_instrument_disconnected(self, instrument_id, result):
        """
        verifies the result is equal to NormalResponse.INSTRUMENT_DISCONNECTED.
        """
        expected = NormalResponse.INSTRUMENT_DISCONNECTED
        self.assertEquals(expected, result, "instrument_id=%r: expecting %r but "
                    "got result=%r" % (instrument_id, expected, result))
