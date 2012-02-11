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

#from ion.services.mi.common import InstErrorCode
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter


#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


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

    def initialize(self, channels=[BarsChannel.INSTRUMENT], timeout=10):
        """
        Return a device channel to an unconnected, unconfigured state.
        @param channels List of channel names to initialize.
        @param timeout Number of seconds before this operation times out
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("initialize: channels=%s timeout=%s" %
                      (channels, timeout))

        self._assert_state([DriverState.UNCONFIGURED,
                            DriverState.DISCONNECTED])

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        result = None

        self._state = DriverState.UNCONFIGURED

        return result

    def configure(self, configs, timeout=10):
        """
        Configure the driver for communications with an instrument channel.
        @param config A dict containing channel name keys, with
        dict values containing the comms configuration for the named channel.
        @param timeout Number of seconds before this operation times out
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("configure: configs=%s timeout=%s" % (configs, timeout))

        self._assert_state(DriverState.UNCONFIGURED)

        assert isinstance(configs, dict)
        assert len(configs) == 1
        assert BarsChannel.INSTRUMENT in configs

        self.config = configs.get(BarsChannel.INSTRUMENT, None)

        result = None

        self._state = DriverState.DISCONNECTED

        return result

    def connect(self, channels=[BarsChannel.INSTRUMENT], timeout=10):
        """
        Establish communications with a device channel.
        @param channels List of channel names to connect.
        @param timeout Number of seconds before this operation times out
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("connect: channels=%s timeout=%s" % (channels, timeout))
            log.debug("connect: config=%s" % self.config)

        self._assert_state(DriverState.DISCONNECTED)

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

    def disconnect(self, channels=[BarsChannel.INSTRUMENT], timeout=10):
        """
        Disconnect communications with a device channel.
        @param channels List of channel names to disconnect.
        @param timeout Number of seconds before this operation times out

        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("disconnect: channels=%s timeout=%s" %
                      (channels, timeout))

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        result = None

        self.bars_client.end()
        self.bars_client = None

        self._state = DriverState.DISCONNECTED

        return result

    def detach(self, channels=[BarsChannel.INSTRUMENT], timeout=10):
        """
        Disconnect communications with a device channel.
        @param channels List of channel names to disconnect.
        @param timeout Number of seconds before this operation times out
        """
        pass

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params=[(BarsChannel.INSTRUMENT,
                           BarsParameter.TIME_BETWEEN_BURSTS)],
            timeout=10):

        if log.isEnabledFor(logging.DEBUG):
            log.debug("get: params=%s timeout=%s" % (params, timeout))

        self._assert_state(DriverState.AUTOSAMPLE)

        log.debug("get: break data streaming to enter main menu")
        self.bars_client.enter_main_menu()

        log.debug("get: select 2 to get system parameter menu")
        self.bars_client.send_option('2')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("get: BUFFER='%s'" % repr(buffer))
        value = bars.get_cycle_time(buffer)
        log.debug("get: VALUE='%s'" % value)

        log.debug("get: send 3 to return to main menu")
        self.bars_client.send_option('3')
        self.bars_client.expect_generic_prompt()

        buffer = self.bars_client.get_last_buffer()
        log.debug("get: BUFFER='%s'" % repr(buffer))

        log.debug("get: resume data streaming")
        self.bars_client.send_option('1')

        result = value
        return result

    def set(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("set: params=%s timeout=%s" % (params, timeout))

        pass

    def execute(self, channels, command, timeout=10):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("execute: channels=%s timeout=%s" % (channels, timeout))

        pass

    def execute_direct(self, channels, bytes):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("execute_direct: channels=%s bytes=%s" %
                      (channels, repr(bytes)))

        pass

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
