#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_bars.driver0
@file ion/services/mi/drivers/uw_bars/driver0.py
@author Carlos Rueda
@brief UW TRHPH BARS driver implementation based on bars_client
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.bars_client import BarsClient
import ion.services.mi.drivers.uw_bars.bars as bars

from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.exceptions import InstrumentProtocolException

from ion.services.mi.common import InstErrorCode
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter

import re

#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


@unittest.skip('Need to align.')
class BarsInstrumentDriver(InstrumentDriver):
    """
    The InstrumentDriver class for the TRHPH BARS sensor.
    """

    # TODO actual handling of the "channel" concept in the design.

    # TODO NOTE: Assumes all interaction is for the INSTRUMENT special channel

    def __init__(self, evt_callback=None):
        InstrumentDriver.__init__(self, evt_callback)

        self.bars_client = None
        self.config = None

        self._state = DriverState.UNCONFIGURED

    def get_current_state(self):
        return self._state

    def _assert_state(self, obj):
        """
        Asserts that the current state is the same as the one given (if not
        a list) or is one of the elements of the given list.
        """
        cs = self.get_current_state()
        if isinstance(obj, list):
            if cs in obj:
                return
            else:
                raise AssertionError("current state=%s, expected one of %s" %
                                 (cs, str(obj)))
        state = obj
        if cs != state:
            raise AssertionError("current state=%s, expected=%s" % (cs, state))

    def initialize(self, channels=None, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        self._assert_state([DriverState.UNCONFIGURED,
                            DriverState.DISCONNECTED])

        channels = channels or [BarsChannel.INSTRUMENT]

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        result = None

        self._state = DriverState.UNCONFIGURED

        return result

    def configure(self, configs, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("configs=%s args=%s kwargs=%s" %
                      (str(configs), str(args), str(kwargs)))

        self._assert_state(DriverState.UNCONFIGURED)

        assert isinstance(configs, dict)
        assert len(configs) == 1
        assert BarsChannel.INSTRUMENT in configs

        self.config = configs.get(BarsChannel.INSTRUMENT, None)

        result = None

        self._state = DriverState.DISCONNECTED

        return result

    def connect(self, channels=None, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        self._assert_state(DriverState.DISCONNECTED)

        channels = channels or [BarsChannel.INSTRUMENT]

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        result = None

        self._setup_bars_client(self.config)

        if self.bars_client.is_collecting_data():
            self._state = DriverState.AUTOSAMPLE
        else:
            #
            # TODO proper handling
            raise Exception("Not handled yet. BARS not collecting data")

        return result

    def _setup_bars_client(self, config):
        config = self.config
        host = config['device_addr']
        port = config['device_port']
        outfile = file('driver0.txt', 'w')
        self.bars_client = BarsClient(host, port, outfile)
        self.bars_client.connect()

    def disconnect(self, channels=None, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        channels = channels or [BarsChannel.INSTRUMENT]

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        result = None

        self.bars_client.end()
        self.bars_client = None

        self._state = DriverState.DISCONNECTED

        return result

    def detach(self, channels, *args, **kwargs):
        """
        """
        pass

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, *args, **kwargs):

        # TODO it only handles [(BarsChannel.INSTRUMENT,
        # BarsParameter.TIME_BETWEEN_BURSTS)]

        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        self._assert_state(DriverState.AUTOSAMPLE)

        assert isinstance(params, list)
        cp = (BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)
        assert params == [cp]

        log.debug("breaking data streaming to enter main menu")
        self.bars_client.enter_main_menu()

        log.debug("select 2 to get system parameter menu")
        self.bars_client.send('2')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))
        string = bars.get_cycle_time(buffer)
        log.debug("VALUE='%s'" % string)
        seconds = bars.get_cycle_time_seconds(string)
        if seconds is None:
            raise InstrumentProtocolException(
                    msg="Unexpected: string could not be matched: %s" % string)

        log.debug("send 3 to return to main menu")
        self.bars_client.send('3')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))

        log.debug("resume data streaming")
        self.bars_client.send('1')

        result = {cp: seconds}
        return result

    def set(self, params, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        self._assert_state(DriverState.AUTOSAMPLE)

        assert isinstance(params, dict)
        assert len(params) == 1
        cp = (BarsChannel.INSTRUMENT, BarsParameter.TIME_BETWEEN_BURSTS)
        assert cp in params
        seconds = params.get(cp)
        assert isinstance(seconds, int)
        assert seconds >= 15

        log.debug("break data streaming to enter main menu")
        self.bars_client.enter_main_menu()

        log.debug("select 2 to get system parameter menu")
        self.bars_client.send('2')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))
        string = bars.get_cycle_time(buffer)
        log.debug("VALUE='%s'" % string)

        log.debug("send 1 to change cycle time")
        self.bars_client.send('1')
        self.bars_client.expect_generic_prompt()

        if seconds <= 59:
            log.debug("send 0 to change cycle time in seconds")
            self.bars_client.send('0')
            self.bars_client.expect_generic_prompt()
            log.debug("send seconds=%d" % seconds)
            self.bars_client.send(str(seconds))
        else:
            log.debug("send 1 to change cycle time in minutes")
            self.bars_client.send('1')
            self.bars_client.expect_generic_prompt()
            minutes = seconds / 60
            log.debug("send minutes=%d" % minutes)
            self.bars_client.send(str(minutes))

        self.bars_client.expect_generic_prompt()

        log.debug("send 3 to return to main menu")
        self.bars_client.send('3')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))

        log.debug("resume data streaming")
        self.bars_client.send('1')

        result = {cp: InstErrorCode.OK}
        return result

    def execute_direct(self, channels, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        pass

    def get_channels(self):
        """
        """
        return BarsChannel.list()

    ########################################################################
    # TBD.
    ########################################################################

    def get_status(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """

        # TODO for the moment just returning the current driver state
        return self.get_current_state()

    def get_capabilities(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass
