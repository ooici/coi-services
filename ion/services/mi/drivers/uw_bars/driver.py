#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_bars.driver UW TRHPH BARS driver module
@file ion/services/mi/drivers/uw_bars/driver.py
@author Carlos Rueda
@brief UW TRHPH BARS driver
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_bars.protocol import BarsInstrumentProtocol

from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverState

from ion.services.mi.common import InstErrorCode
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.protocol import BarsProtocolState


#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


class BarsInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the TRHPH BARS sensor.

    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+SA+SV+Instrument+Driver+Interface
    """

    # TODO actual handling of the "channel" concept in the design.

    # TODO harmonize with base class.

    # TODO NOTE: Assumes all interaction is for the INSTRUMENT special channel

    def __init__(self):
        InstrumentDriver.__init__(self)

        self.connection = None
        self.protocol = None
        self.config = None

        self._state = DriverState.UNCONFIGURED

    def get_current_state(self):
        return self._state

    def initialize(self, channels=[BarsChannel.INSTRUMENT], timeout=10):
        """
        Return a device channel to an unconnected, unconfigured state.
        @param channels List of channel names to initialize.
        @param timeout Number of seconds before this operation times out
        """

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        success = InstErrorCode.OK
        result = None

        self._state = DriverState.UNCONFIGURED

        return (success, result)

    def configure(self, configs, timeout=10):
        """
        Configure the driver for communications with an instrument channel.
        @param config A dict containing channel name keys, with
        dict values containing the comms configuration for the named channel.
        @param timeout Number of seconds before this operation times out
        """

        assert isinstance(configs, dict)
        assert len(configs) == 1
        assert BarsChannel.INSTRUMENT in configs

        self.config = configs.get(BarsChannel.INSTRUMENT, None)

        success = InstErrorCode.OK
        result = None

        self._state = DriverState.DISCONNECTED

        return (success, result)

    def connect(self, channels=[BarsChannel.INSTRUMENT], timeout=10):
        """
        Establish communications with a device channel.
        @param channels List of channel names to connect.
        @param timeout Number of seconds before this operation times out
        """

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        success = InstErrorCode.OK
        result = None

        self._setup_protocol(self.config)

        prot_state = self.protocol.get_current_state()
        if prot_state == BarsProtocolState.COLLECTING_DATA:
            self._state = DriverState.AUTOSAMPLE
        else:
            #
            # TODO proper handling
            raise Exception("Not handled yet. Expecting protocol to be in %s" %
                            BarsProtocolState.COLLECTING_DATA)

        return (success, result)

    def _setup_protocol(self, config):
        self.protocol = BarsInstrumentProtocol()
        self.protocol.configure(self.config)
        self.protocol.connect()

    def disconnect(self, channels=[BarsChannel.INSTRUMENT], timeout=10):
        """
        Disconnect communications with a device channel.
        @param channels List of channel names to disconnect.
        @param timeout Number of seconds before this operation times out

        """

        assert len(channels) == 1
        assert channels[0] == BarsChannel.INSTRUMENT

        success = InstErrorCode.OK
        result = None

        self.protocol.disconnect()
        self.protocol = None

        self._state = DriverState.DISCONNECTED

        return (success, result)

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

    def get(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass

    def set(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass

    def execute(self, channels, command, timeout=10):
        """
        """
        pass

    def execute_direct(self, channels, bytes):
        """
        """
        pass

    ########################################################################
    # TBD.
    ########################################################################

    def get_status(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass

    def get_capabilities(self, params, timeout=10):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass
