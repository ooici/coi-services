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


#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


#
# TODO this is not functional yet
#

class BarsInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the TRHPH BARS sensor.

    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+SA+SV+Instrument+Driver+Interface
    """

    # TODO actual handling of the "channel" concept in the design.

    # TODO harmonize with base class.

    # TODO NOTE: Assumes all interaction is for the ALL special channels spec


    def __init__(self):
        InstrumentDriver.__init__(self)

        self.connection = None
        self.protocol = None
        self.config = None

        self._state = DriverState.UNCONFIGURED

    def get_state(self):
        return self._state

    def initialize(self, channels=[BarsChannel.ALL], timeout=10):
        """
        Return a device channel to an unconnected, unconfigured state.
        @param channels List of channel names to initialize.
        @param timeout Number of seconds before this operation times out
        """

        assert len(channels) == 1
        assert channels[0] == BarsChannel.ALL

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
        assert configs.has_key(BarsChannel.ALL)

        self.config = configs.get(BarsChannel.ALL, None)

        success = InstErrorCode.OK
        result = None

        self._state = DriverState.DISCONNECTED;

        return (success, result)


    def connect(self, channels=[BarsChannel.ALL], timeout=10):
        """
        Establish communications with a device channel.
        @param channels List of channel names to connect.
        @param timeout Number of seconds before this operation times out
        """

        assert len(channels) == 1
        assert channels[0] == BarsChannel.ALL

        success = InstErrorCode.OK
        result = None

        self._configure_protocol(self.config)

        self._state = DriverState.CONNECTING

        return (success, result)

    def _configure_protocol(self, config):
        self.protocol = BarsInstrumentProtocol()
        self.protocol.configure(self.config)


    def disconnect(self, channels=[BarsChannel.ALL], timeout=10):
        """
        Disconnect communications with a device channel.
        @param channels List of channel names to disconnect.
        @param timeout Number of seconds before this operation times out

        """
        pass

    def detach(self, channels=[BarsChannel.ALL], timeout=10):
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

    ################################
    # Announcement callback from protocol
    ################################
    def protocol_callback(self, event):
        """The callback method that the protocol calls when there is some sort
        of event worth notifying the driver about.

        @param event The event object from the event service
        @todo Make event a real event object of some sort instead of the hack
        tuple of (DriverAnnouncement enum, any error code, message)
        """


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
