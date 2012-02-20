#!/usr/bin/env python

"""
@package ion.services.mi.test.test_basic
@file ion/services/mi/test/test_basic.py
@author Carlos Rueda
@brief Some unit tests for R2 instrument driver base classes.
This file defines subclasses of core classes mainly to suply required
definitions and the tests functionality in the base classes.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.services.mi.common import BaseEnum
from ion.services.mi.common import InstErrorCode
from ion.services.mi.instrument_protocol import InstrumentProtocol
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverChannel

from nose.plugins.attrib import attr
import unittest


class Command(BaseEnum):
    pass


class Channel(BaseEnum):
    CHAN1 = "channel_1"
    CHAN2 = "channel_2"

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
    def __init__(self, channel, evt_callback=None):
        """
        @param channel identifies the particular protocol instance.
        """
        InstrumentProtocol.__init__(self, evt_callback)
        self._channel = channel

    def get(self, params, *args, **kwargs):
        print "MyProtocol(%s).get: params=%s" % (self._channel, str(params))
        result = {}
        for param in params:
            value = "%s_value_in_%s" % (str(param), self._channel)
            result[param] = value
        return result


class MyDriver(InstrumentDriver):

    def __init__(self, evt_callback=None):
        InstrumentDriver.__init__(self, evt_callback)

        self.instrument_commands = Command
        self.instrument_parameters = Parameter
        self.instrument_channels = Channel
        self.instrument_errors = Error

        for channel in self.instrument_channels.list():
            protocol = MyProtocol(channel, self.protocol_callback)
            self.chan_map[channel] = protocol


class Some(object):
    VALID_PARAMS = [
            (Channel.CHAN1, Parameter.PARAM1),
            (Channel.CHAN1, Parameter.PARAM1),  # duplicate of previous one
            (Channel.CHAN2, Parameter.PARAM1),
            (Channel.CHAN2, Parameter.PARAM2),
            (Channel.CHAN2, Parameter.PARAM3)]


@attr('UNIT', group='mi')
class DriverTest(unittest.TestCase):

    def test_params(self):
        driver = MyDriver()

        params = Some.VALID_PARAMS
        # plus some invalid channels:
        params += [("invalid_chan", Parameter.PARAM1)]
        # plus some invalid parameters:
        params += [(Channel.CHAN1, "invalid_param")]

        result = driver.get(params)

        print "\nresult = "
        for item in result.items():
            print "\t%s" % str(item)

        self.assertEqual(result[("invalid_chan", Parameter.PARAM1)],
                         InstErrorCode.INVALID_CHANNEL)

        self.assertEqual(result[(Channel.CHAN1, "invalid_param")],
                         InstErrorCode.INVALID_PARAMETER)

        for param in Some.VALID_PARAMS:
            self.assertIsNotNone(result[param])
