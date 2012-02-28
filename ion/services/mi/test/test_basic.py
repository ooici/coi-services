#!/usr/bin/env python

"""
@package ion.services.mi.test.test_basic
@file ion/services/mi/test/test_basic.py
@author Carlos Rueda
@brief Some unit tests for R2 instrument driver base classes.
This file defines subclasses of core classes mainly to suply required
definitions and then tests functionality in the base classes.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

import logging
import unittest
from nose.plugins.attrib import attr
from mock import Mock
from ion.services.mi.common import BaseEnum
from ion.services.mi.common import InstErrorCode
from ion.services.mi.common import DriverAnnouncement
from ion.services.mi.exceptions import RequiredParameterException
from ion.services.mi.instrument_protocol import InstrumentProtocol
from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverChannel

import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')


class Command(BaseEnum):
    pass


class Channel(BaseEnum):
    CHAN1 = "CHAN1"
    CHAN2 = "CHAN2"

    ALL = DriverChannel.ALL
    INSTRUMENT = DriverChannel.INSTRUMENT


class Error(BaseEnum):
    pass


class Status(BaseEnum):
    pass


class MetadataParameter(BaseEnum):
    pass


class Parameter(BaseEnum):
    PARAM1 = 'PARAM1'
    PARAM2 = 'PARAM2'
    PARAM3 = 'PARAM3'


class MyProtocol(InstrumentProtocol):
    """
    A MyProtocol instance will be created for each driver channel.
    """

    # a base for values that can be easily associated to each protocol
    # (facilitates inspection)
    next_base_value = 0

    def __init__(self, channel, params, evt_callback=None):
        """
        @param channel identifies the particular protocol instance.
        @param params the particular parameters for this channel.
        """
        InstrumentProtocol.__init__(self, evt_callback)
        self._channel = channel

        # initialize values for the params:
        MyProtocol.next_base_value += 1000
        next_value = MyProtocol.next_base_value
        self._values = {}
        for param in params:
            next_value += 1
            self._values[param] = next_value

    def initialize(self, *args, **kwargs):
        self._state = DriverState.UNCONFIGURED
        return InstErrorCode.OK

    def configure(self, config, *args, **kwargs):
        self.config = config
        self._state = DriverState.DISCONNECTED
        return InstErrorCode.OK

    def connect(self, *args, **kwargs):
        self._state = DriverState.AUTOSAMPLE
        return InstErrorCode.OK

    def disconnect(self, *args, **kwargs):
        self._state = DriverState.DISCONNECTED
        return InstErrorCode.OK

    def attach(self, *args, **kwargs):
        return InstErrorCode.OK

    def detach(self, *args, **kwargs):
        return InstErrorCode.OK

    def get(self, params, *args, **kwargs):
        mi_logger.debug("MyProtocol(%s).get: params=%s" % (self._channel,
                                                           str(params)))
        assert isinstance(params, (list, tuple))
        result = {}
        for param in params:
            if param in self._values:
                value = self._values[param]
            else:
                value = InstErrorCode.INVALID_PARAMETER
            result[param] = value

        return result

    def set(self, params, *args, **kwargs):
        mi_logger.debug("MyProtocol(%s).set: params=%s" % (self._channel,
                                                           str(params)))

        assert isinstance(params, dict)

        updated_params = 0
        result = {}
        for (param, value) in params.items():
            if param in self._values:
                if isinstance(value, int):
                    self._values[param] = value
                    result[param] = InstErrorCode.OK
                    updated_params += 1
                else:
                    result[param] = InstErrorCode.INVALID_PARAM_VALUE
            else:
                result[param] = InstErrorCode.INVALID_PARAMETER

        self.announce_to_driver(DriverAnnouncement.CONFIG_CHANGE,
                                msg="%s parameter(s) successfully set." %
                                    updated_params)

        return result


class MyDriver(InstrumentDriver):

    def __init__(self, evt_callback=None):
        InstrumentDriver.__init__(self, evt_callback)

        self.instrument_commands = Command
        self.instrument_parameters = Parameter
        self.instrument_channels = Channel
        self.instrument_errors = Error

        for channel in self.instrument_channels.list():
            #
            # TODO associate some specific params per channel. Note that
            # there is no framework mechanism to specify this. For the
            # moment, just associate *all* parameters to each channel:
            #
            params_per_channel = self.instrument_parameters.list()
            protocol = MyProtocol(channel, params_per_channel,
                                  self.protocol_callback)
            self.chan_map[channel] = protocol


class Some(object):
    VALID_CHANNELS = [
            Channel.CHAN1,
            Channel.CHAN2,
            Channel.INSTRUMENT]

    INVALID_CHANNELS = [
            "invalid_chan1",
            "invalid_chan2"]

    VALID_PARAMS = [
            (Channel.CHAN1, Parameter.PARAM1),
            (Channel.CHAN1, Parameter.PARAM1),  # duplicate of previous one
            (Channel.CHAN2, Parameter.PARAM1),
            (Channel.CHAN2, Parameter.PARAM2),
            (Channel.CHAN2, Parameter.PARAM3)]

    INVALID_PARAMS = [
            ("invalid_chan", Parameter.PARAM1),
            (Channel.CHAN1, "invalid_param")]


def _print_dict(title, d):
    mi_logger.debug("%s:" % title)
    for item in d.items():
        mi_logger.debug("\t%s" % str(item))


@attr('UNIT', group='mi')
class DriverTest(unittest.TestCase):

    def setUp(self):
        self.callback = Mock()
        MyProtocol.next_base_value = 0
        self.driver = MyDriver(self.callback)

    def test_initialize(self):
        """Driver initialization tests"""
        channels = Some.VALID_CHANNELS + Some.INVALID_CHANNELS

        mi_logger.debug("\n initialize: %s" % str(channels))

        result = self.driver.initialize(channels)

        _print_dict("\n initialize result", result)

        for c in channels:
            self.assertTrue(c in result)

        for c in Some.INVALID_CHANNELS:
            self.assertEqual(result[c], InstErrorCode.INVALID_CHANNEL)

        for c in Some.VALID_CHANNELS:
            self.assertEqual(result[c], InstErrorCode.OK)

    def test_configure(self):
        """Driver configuration tests"""
        channels = Some.VALID_CHANNELS + Some.INVALID_CHANNELS

        mi_logger.debug("\n configure: %s" % str(channels))

        configs = {}
        for c in channels:
            configs[c] = {'method': 'ethernet',
                          'device_addr': '1.1.1.1',
                          'device_port': 1,
                          'server_addr': '2.2.2.2',
                          'server_port': 2}

        result = self.driver.configure(configs)

        _print_dict("\n configure result", result)

        for c in channels:
            self.assertTrue(c in result)

        for c in Some.INVALID_CHANNELS:
            self.assertEqual(result[c], InstErrorCode.INVALID_CHANNEL)

        for c in Some.VALID_CHANNELS:
            self.assertEqual(result[c], InstErrorCode.OK)

    def test_connect(self):
        """Driver connection tests"""
        channels = Some.VALID_CHANNELS + Some.INVALID_CHANNELS

        mi_logger.debug("\n connect: %s" % str(channels))

        result = self.driver.connect(channels)

        _print_dict("\n connect result", result)

        for c in channels:
            self.assertTrue(c in result)

        for c in Some.INVALID_CHANNELS:
            self.assertEqual(result[c], InstErrorCode.INVALID_CHANNEL)

        for c in Some.VALID_CHANNELS:
            self.assertEqual(result[c], InstErrorCode.OK)

    def test_get_params(self):
        """Driver get params tests"""
        params = Some.VALID_PARAMS + Some.INVALID_PARAMS

        mi_logger.debug("\nGET: %s" % str(params))

        get_result = self.driver.get(params)

        _print_dict("\nGET get_result", get_result)

        self.assertEqual(get_result[("invalid_chan", Parameter.PARAM1)],
                         InstErrorCode.INVALID_CHANNEL)

        self.assertEqual(get_result[(Channel.CHAN1, "invalid_param")],
                         InstErrorCode.INVALID_PARAMETER)

        for cp in Some.VALID_PARAMS:
            self.assertTrue(cp in get_result)

    def test_get_params_channel_all(self):
        """Driver get all params tests"""
        params = [(Channel.ALL, Parameter.PARAM1),
                  (Channel.ALL, Parameter.PARAM2)]

        mi_logger.debug("\nGET: %s" % str(params))

        get_result = self.driver.get(params)

        _print_dict("\nGET get_result", get_result)

        for c in Channel.list():
            if c != Channel.ALL:
                self.assertTrue((c, Parameter.PARAM1) in get_result)
                self.assertTrue((c, Parameter.PARAM2) in get_result)

    def _prepare_set_params(self, params):
        """Gets a dict for the set operation"""
        value = 99000
        set_params = {}
        for cp in params:
            set_params[cp] = value
            value += 1
        _print_dict("\nset_params", set_params)
        return set_params

    def test_set_params(self):
        """Driver set params tests"""
        params = Some.VALID_PARAMS + Some.INVALID_PARAMS

        set_params = self._prepare_set_params(params)

        set_result = self.driver.set(set_params)

        _print_dict("\nSET set_result", set_result)

        # now, get the values for the valid parameters and check
        get_result = self.driver.get(Some.VALID_PARAMS)

        _print_dict("\nGET get_result", get_result)

        # verify the new values are the ones we wanted
        for cp in Some.VALID_PARAMS:
            self.assertEqual(set_params[cp], get_result[cp])

    def test_set_duplicate_param(self):
        """Driver set duplicate params tests"""
        #
        # Note that via the ALL specifier, along with a specific channel,
        # one could indicate a duplicate parameter for the same channel.
        #
        params = [(Channel.ALL, Parameter.PARAM1),
                  (Channel.CHAN1, Parameter.PARAM1)]

        set_params = self._prepare_set_params(params)

        set_result = self.driver.set(set_params)

        _print_dict("\nSET set_result", set_result)

        self.assertEqual(set_result[(Channel.CHAN1, Parameter.PARAM1)],
                         InstErrorCode.DUPLICATE_PARAMETER)

    def test_check_channel(self):
        """Test the routines to check the channel arguments"""
        self.assertRaises(RequiredParameterException,
                          self.driver._check_channel_args, DriverChannel.ALL)
        self.assertRaises(RequiredParameterException,
                          self.driver._check_channel_args, [])
        self.assertRaises(RequiredParameterException,
                          self.driver._check_channel_args, None)

        (bad, good) = self.driver._check_channel_args(
                [DriverChannel.INSTRUMENT])
        self.assertEquals(bad, {})
        self.assertEquals(good, [DriverChannel.INSTRUMENT])

        (bad, good) = self.driver._check_channel_args(["BAD_CHANNEL"])
        self.assertEquals(bad, {"BAD_CHANNEL": InstErrorCode.INVALID_CHANNEL})
        self.assertEquals(good, [])

        (bad, good) = self.driver._check_channel_args([Channel.CHAN1])
        self.assertEquals(bad, {})
        self.assertEquals(good, [Channel.CHAN1])

        (bad, good) = self.driver._check_channel_args([Channel.CHAN1,
                                                       Channel.CHAN1])
        self.assertEquals(bad, {})
        self.assertEquals(good, [Channel.CHAN1])

        # @todo Need a better test...something with more channels
        (bad, good) = self.driver._check_channel_args([Channel.CHAN1,
                                                       Channel.ALL])
        self.assertEquals(bad, {})
        self.assertEquals(good, [Channel.CHAN1, Channel.CHAN2])

        (bad, good) = self.driver._check_channel_args([Channel.CHAN1,
                                                       Channel.INSTRUMENT])
        self.assertEquals(bad, {})
        self.assertEquals(good.count(Channel.CHAN1), 1)
        self.assertEquals(good.count(Channel.INSTRUMENT), 1)
        self.assertEquals(len(good), 2)

        (bad, good) = self.driver._check_channel_args([Channel.CHAN1,
                                                       "BAD_CHANNEL"])
        self.assertEquals(bad, {"BAD_CHANNEL": InstErrorCode.INVALID_CHANNEL})
        self.assertEquals(good, [Channel.CHAN1])
