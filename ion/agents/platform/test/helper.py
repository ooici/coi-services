#!/usr/bin/env python

"""
@package ion.agents.platform.test.helper
@file    ion/agents/platform/test/helper.py
@author  Carlos Rueda
@brief   Definitions and functionality to facilitate common validations in tests.
         It also provides some supporting methods related with the simulator.

         The expected structures and error responses are currently based on the
         CI-OMS interface. This scheme may need to be adjusted when CG or other
         platform networks are incorporated.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.rsn.simulator.logger import Logger
log = Logger.get_logger()

from ion.agents.platform.responses import NormalResponse, InvalidResponse
from ion.agents.platform.rsn.simulator.oms_simulator import CIOMSSimulator
from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory

import os


class HelperTestMixin:
    """
    A mixin to facilitate tests:
      - launch/disable/enable simulator
      - set up definitions against either simulator or actual RSN OMS endpoint.
      - common validations
    """

    @classmethod
    def using_actual_rsn_oms_endpoint(cls):
        """
        Determines whether we are testing against the actual RSN OMS endpoint.
        This is based on looking up the "USING_ACTUAL_RSN_OMS_ENDPOINT"
        environment variable, which normally will only be defined as
        convenient while doing local tests. See OOIION-1352.
        """
        return "yes" == os.getenv('USING_ACTUAL_RSN_OMS_ENDPOINT')

    @classmethod
    def setUpClass(cls):
        if cls.using_actual_rsn_oms_endpoint():
            print("HelperTestMixin: setUpClassBasedOnRealEndpoint")
            cls._setUpClassBasedOnRealEndpoint()

        else:
            print("HelperTestMixin: _setUpClassBasedOnSimulator")
            cls._setUpClassBasedOnSimulator()

        # _oms_uri: see _dispatch_simulator and related methods:
        cls._oms_uri = None
        cls._inactivity_period = None

    @classmethod
    def _setUpClassBasedOnRealEndpoint(cls):
        """
        Various IDs from the actual endpoint (by inspecting output from
        the oms_simple.py program).
        """
        cls.PLATFORM_ID           = 'LPJBox_CI_Ben_Hall'
        cls.SUBPLATFORM_IDS       = []
        cls.ATTR_NAMES            = ['Ambient Temperature|758',
                                     'Output Voltage|765',
                                     'Relative Humidity|759',
                                     'Unit Temperature|766']

        cls.WRITABLE_ATTR_NAMES   = ['']
        cls.VALID_ATTR_VALUE      = "7"     # within the range
        cls.INVALID_ATTR_VALUE    = "9876"  # out of range

        cls.PORT_ID               = '0'
        cls.INSTRUMENT_ID         = 'PENDING_instrument_id'
        cls.INSTRUMENT_ATTR_NAME  = 'maxCurrentDraw'
        cls.VALID_INSTRUMENT_ATTR_VALUE = 12345

        cls.INSTRUMENT_ATTRIBUTES_AND_VALUES = {
            'maxCurrentDraw' : 12345,
            'initCurrent'    : 23456,
            'dataThroughput' : 34567,
            'instrumentType' : "FOO_INSTRUMENT_TYPE"
        }

    @classmethod
    def _setUpClassBasedOnSimulator(cls):
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

        cls.PLATFORM_ID           = 'Node1D'
        cls.SUBPLATFORM_IDS       = ['MJ01C']
        cls.ATTR_NAMES            = ['input_voltage|0', 'input_bus_current|0']
        cls.WRITABLE_ATTR_NAMES   = ['input_bus_current|0']
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

        plat_network_size = os.getenv('PLAT_NETWORK', None)
        if "single" == plat_network_size:
            #
            # network with just a single platform (no children).
            #
            cls.PLATFORM_ID = 'LJ01D'
            print("PLAT_NETWORK=single -> using base platform: %r" % cls.PLATFORM_ID)
            cls.SUBPLATFORM_IDS = []
            cls.ATTR_NAMES = ['input_voltage|0', 'input_bus_current|0']
            cls.WRITABLE_ATTR_NAMES = ['input_bus_current|0']

            cls.PORT_ID = '1'
            cls.INSTRUMENT_ID = 'LJ01D_port_1_instrument_1'
        else:
            print("PLAT_NETWORK undefined -> using base platform: %r" % cls.PLATFORM_ID)

    def _verify_valid_platform_id(self, platform_id, dic):
        """
        verifies the platform_id is the only entry in the dict with a
        valid value. Returns dic[platform_id].
        """
        self.assertIsInstance(dic, dict)
        self.assertIn(platform_id, dic)
        self.assertEquals(1, len(dic))
        val = dic[platform_id]
        self.assertNotEquals(InvalidResponse.PLATFORM_ID, val)
        return val

    def _verify_invalid_platform_id(self, platform_id, dic):
        """
        verifies the platform_id is the only entry in the dict with a
        value equal to InvalidResponse.PLATFORM_ID.
        """
        self.assertIsInstance(dic, dict)
        self.assertIn(platform_id, dic)
        self.assertEquals(1, len(dic))
        val = dic[platform_id]
        self.assertEquals(InvalidResponse.PLATFORM_ID, val)

    def _verify_valid_attribute_id(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        valid value. Returns dic[attr_id].
        """
        self.assertIn(attr_id, dic, "%s in %s" %(attr_id, dic))
        val = dic[attr_id]
        self.assertIsInstance(val, (tuple, list))
        self.assertNotEquals(InvalidResponse.ATTRIBUTE_ID, val)
        return val

    def _verify_invalid_attribute_id(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        value equal to InvalidResponse.ATTRIBUTE_ID
        """
        self.assertIn(attr_id, dic)
        val = dic[attr_id]
        self.assertEquals(InvalidResponse.ATTRIBUTE_ID, val,
                          "attr_id=%r, val=%r but expected=%r" % (
                          attr_id, val, InvalidResponse.ATTRIBUTE_ID))

    def _verify_attribute_value_out_of_range(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        value equal to InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE
        """
        self.assertIn(attr_id, dic)
        val = dic[attr_id]
        self.assertEquals(InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE, val,
                          "attr_id=%r, val=%r but expected=%r" % (
                          attr_id, val, InvalidResponse.ATTRIBUTE_VALUE_OUT_OF_RANGE))

    def _verify_not_writable_attribute_id(self, attr_id, dic):
        """
        verifies the attr_id is an entry in the dict with a
        value equal to InvalidResponse.ATTRIBUTE_NOT_WRITABLE.
        """
        self.assertIn(attr_id, dic)
        val = dic[attr_id]
        self.assertEquals(InvalidResponse.ATTRIBUTE_NOT_WRITABLE, val,
                          "Expecting val=%s for attr_id=%r. Got val=%r" % (
                              InvalidResponse.ATTRIBUTE_NOT_WRITABLE,
                              attr_id,
                              val))

    def _verify_valid_port_id(self, port_id, dic):
        """
        verifies the port_id is an entry in the dict with a
        valid value. Returns dic[port_id].
        """
        self.assertIn(port_id, dic)
        val = dic[port_id]
        self.assertNotEquals(InvalidResponse.PORT_ID, val)
        return val

    def _verify_invalid_port_id(self, port_id, dic):
        """
        verifies the port_id is an entry in the dict with a
        value equal to InvalidResponse.PORT_ID.
        """
        self.assertIn(port_id, dic)
        val = dic[port_id]
        self.assertEquals(InvalidResponse.PORT_ID, val)

    def _verify_valid_instrument_id(self, instrument_id, dic):
        """
        verifies the instrument_id is an entry in the dict with a valid value,
        either a dict or InvalidResponse.INSTRUMENT_ALREADY_CONNECTED.
        Returns dic[instrument_id].
        """
        self.assertIn(instrument_id, dic)
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
        self.assertIn(instrument_id, dic)
        val = dic[instrument_id]
        self.assertEquals(val, InvalidResponse.MISSING_INSTRUMENT_ATTRIBUTE)

    def _verify_instrument_disconnected(self, instrument_id, result):
        """
        verifies the result is equal to NormalResponse.INSTRUMENT_DISCONNECTED.
        """
        expected = NormalResponse.INSTRUMENT_DISCONNECTED
        self.assertEquals(expected, result, "instrument_id=%r: expecting %r but "
                    "got result=%r" % (instrument_id, expected, result))

    def _dispatch_simulator(self, oms_uri, inactivity_period=180):
        """
        Method to be called in setUp. If oms_uri == "launchsimulator", it
        launches the simulator and returns the updated URI (which can be the
        alias "localsimulator" or some concrete URI) to be used to update the
        driver configuration. Otherwise it returns the given argument.

        In case of launched simulator:
        - addCleanup is called here to stop the launched simulator at end of test.
        - _simulator_disable can be called by a test to disable the simulator
          at any time (for example, for testing connection lost).
        - _simulator_enable can be called to explicitly re-enable it.

        @param oms_uri            The initial URI
        @param inactivity_period  If launched, the simulator process will
                                  exit after this period of inactivity (180)

        @return The updated URI in case the oms_uri is "launchsimulator"
                or the given oms_uri.
        """
        self._oms_uri = oms_uri
        self._inactivity_period = inactivity_period
        return self._launch_simulator()

    def _launch_simulator(self):
        if self._oms_uri == "launchsimulator":
            self.addCleanup(CIOMSClientFactory.stop_launched_simulator)
            log.debug("launch_simulator inactivity: %s", self._inactivity_period)
            return CIOMSClientFactory.launch_simulator(self._inactivity_period)
        else:
            return self._oms_uri

    def _simulator_disable(self):
        """
        Disables the simulator to cause the effect of having lost the connection.
        """
        if self._oms_uri == "launchsimulator":
            rsn_oms = CIOMSClientFactory.get_rsn_oms_for_launched_simulator()
            self.assertIsNotNone(rsn_oms, "the simulator must have been "
                                          "launched and be running")
            rsn_oms.x_disable()

        elif self._oms_uri == "embsimulator":
            CIOMSSimulator.x_disable()

        else:
            self.fail("_simulator_disable does not work for: %s" % self._oms_uri)

    def _simulator_enable(self):
        """
        Reenables the simulator.
        """
        if self._oms_uri == "launchsimulator":
            rsn_oms = CIOMSClientFactory.get_rsn_oms_for_launched_simulator()
            self.assertIsNotNone(rsn_oms, "the simulator must have been "
                                          "launched and be running")
            rsn_oms.x_enable()

        elif self._oms_uri == "embsimulator":
            CIOMSSimulator.x_enable()

        else:
            self.fail("_simulator_enable does not work for: %s" % self._oms_uri)
