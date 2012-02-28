#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.protocol0 import BarsInstrumentProtocol
from ion.services.mi.drivers.uw_bars.common import BarsParameter

from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode

import time
import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')

from ion.services.mi.drivers.uw_bars.test import BarsTestCase
from nose.plugins.attrib import attr
import unittest
import os


# explicit run_it because of threading + gevent-monkey-patching issues
@unittest.skipIf(os.getenv('run_it') is None, 'define run_it to run this.')
@attr('UNIT', group='mi')
class ProtocolTest(BarsTestCase):

    def _connect(self):

        def evt_callback(event):
            log.debug("CALLBACK: %s" % str(event))
        self.protoc = BarsInstrumentProtocol(evt_callback)
        protoc = self.protoc

        self.assertEqual(DriverState.UNCONFIGURED, protoc.get_current_state())

        # initialize
        result = protoc.initialize()
        self.assertEqual(DriverState.UNCONFIGURED, protoc.get_current_state())
        log.debug("protoc state = %s" % str(protoc.get_current_state()))

        # configure
        config = self.config
        result = protoc.configure(config)
        self.assertEqual(DriverState.DISCONNECTED, protoc.get_current_state())
        log.debug("protoc state = %s" % str(protoc.get_current_state()))

        # connect
        result = protoc.connect()
        log.debug("connect result = %s" % str(result))
        self._assert_auto_sample()

        log.debug("sleeping for a bit to see data streaming")
        time.sleep(4)

    def _disconnect(self):
        log.debug("disconnecting")
        protoc = self.protoc
        result = protoc.disconnect()
        self.assertEqual(DriverState.DISCONNECTED, protoc.get_current_state())
        log.debug("protoc state = %s" % str(protoc.get_current_state()))

    def test_connect_disconnect(self):
        self._connect()
        self._disconnect()

    def _get(self, params):
        protoc = self.protoc

        result = protoc.get(params)
        log.debug("get result = %s" % str(result))
        assert isinstance(result, dict)

        self._assert_auto_sample()

        return result

    def test_get(self):
        self._connect()

        p = BarsParameter.TIME_BETWEEN_BURSTS
        params = [p]

        result = self._get(params)
        seconds = result.get(p)
        assert isinstance(seconds, int)

        time.sleep(1)

        self._disconnect()

    def test_get_set(self):
        self._connect()

        p = BarsParameter.TIME_BETWEEN_BURSTS
        params = [p]

        result = self._get(params)
        seconds = result.get(p)
        assert isinstance(seconds, int)

        protoc = self.protoc

        new_seconds = seconds + 5
        if new_seconds > 30 or new_seconds < 15:
            new_seconds = 15

        # get a parameter
        result = protoc.set({p: new_seconds})
        log.debug("set result = %s" % str(result))
        code = result.get(p)
        self.assertTrue(InstErrorCode.is_ok(code))

        self._assert_auto_sample()

        result = self._get(params)
        seconds = result.get(p)

        self.assertEqual(new_seconds, seconds)
        time.sleep(1)

        self._disconnect()

    def _assert_auto_sample(self):
        """asserts AUTOSAMPLE state"""
        curr_state = self.protoc.get_current_state()
        self.assertEqual(DriverState.AUTOSAMPLE, curr_state)
        log.debug("protoc state = %s" % str(curr_state))


from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.drivers.uw_bars.common import BarsCommand
from ion.services.mi.drivers.uw_bars.common import BarsParameter
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsError

from mock import Mock


class MyDriver(InstrumentDriver):

    def __init__(self, evt_callback=None):
        InstrumentDriver.__init__(self, evt_callback)

        self.instrument_commands = BarsCommand
        self.instrument_parameters = BarsParameter
        self.instrument_channels = BarsChannel
        self.instrument_errors = BarsError

        self.protoc = BarsInstrumentProtocol(self.protocol_callback)
        self.chan_map[BarsChannel.INSTRUMENT] = self.protoc


def _print_dict(title, d):
    log.debug("%s:" % title)
    for item in d.items():
        log.debug("\t%s" % str(item))


# explicit run_it because of threading + gevent-monkey-patching issues
@unittest.skipIf(os.getenv('run_it') is None, 'define run_it to run this.')
@attr('UNIT', group='mi')
class DriverTest(BarsTestCase):

    def setUp(self):
        super(DriverTest, self).setUp()
        self.callback = Mock()
        self.driver = MyDriver(self.callback)
        log.debug("configuring")
        self.driver.configure({BarsChannel.INSTRUMENT: self.config})
        log.debug("connecting")
        self.driver.connect([BarsChannel.INSTRUMENT])

    def tearDown(self):
        log.debug("disconnecting")
        self.driver.disconnect([BarsChannel.INSTRUMENT])
        super(DriverTest, self).tearDown()

    def test_get_params(self):
        params = [(BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)]

        log.debug("\nGET: %s" % str(params))

        get_result = self.driver.get(params)

        _print_dict("\nGET get_result", get_result)

        for cp in params:
            self.assertTrue(cp in get_result)
