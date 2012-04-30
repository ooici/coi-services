#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.driver
@file ion/services/mi/drivers/uw_trhph/driver.py
@author Carlos Rueda
@brief UW TRHPH driver implementation based on trhph_client
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.drivers.uw_trhph.protocol import TrhphInstrumentProtocol

from ion.services.mi.instrument_driver import InstrumentDriver
#from ion.services.mi.instrument_driver import DriverChannel
from ion.services.mi.drivers.uw_trhph.common import TrhphParameter
from ion.services.mi.drivers.uw_trhph.common import TrhphCommand
from ion.services.mi.drivers.uw_trhph.common import TrhphChannel
from ion.services.mi.drivers.uw_trhph.common import TrhphError

from ion.services.mi.mi_logger import mi_logger
log = mi_logger


class TrhphInstrumentDriver(InstrumentDriver):
    """
    The InstrumentDriver class for the TRHPH sensor.
    """

    # TODO NOTE: Assumes all interaction is for the INSTRUMENT special channel

    def __init__(self, evt_callback=None):
        log.debug("Creating TrhphInstrumentDriver")
        InstrumentDriver.__init__(self, evt_callback)

        self.instrument_commands = TrhphCommand
        self.instrument_parameters = TrhphParameter
        self.instrument_channels = TrhphChannel
        self.instrument_errors = TrhphError

        # a single protocol (associated with TrhphChannel.INSTRUMENT)

        self.protoc = TrhphInstrumentProtocol(self.protocol_callback)
        self.chan_map[TrhphChannel.INSTRUMENT] = self.protoc

    def execute_get_metadata(self, channels=None, *args, **kwargs):
        """
        Returns metadata.

        TODO NOTE: metadata aspect not yet specified in the framework. This
        method is just a quick implementation to exercise the functionality
        provided by the underlying TrhphClient.

        @param channels List of channel names to get metadata from, by default
                        [TrhphChannel.INSTRUMENT]
        @retval Dict of channels and metadata outputs
        """

        # NOTE code written in a way that eventually can be moved to parent
        # class with no or minimal changes.

        channels = channels or [TrhphChannel.INSTRUMENT]

        mi_logger.debug("Issuing base execute_get_metadata...")
        (result, valid_channels) = self._check_channel_args(channels)

        for channel in valid_channels:
            proto = self.chan_map[channel]
            result[channel] = proto.execute_get_metadata(*args,
                                                         **kwargs)

        return result

    def execute_diagnostics(self, channels=None, num_scans=5, *args, **kwargs):
        """
        Executes the diagnostics operation.

        @param channels List of channel names to execute the operation to,
                by default [TrhphChannel.INSTRUMENT].
        @param num_scans the number of scans for the operation, 5 by default.
        @param timeout Timeout for the wait, self._timeout by default.
        @retval a list of rows, one with values per scan
        @throws TimeoutException
        """

        channels = channels or [TrhphChannel.INSTRUMENT]

        mi_logger.debug("Issuing base execute_diagnostics...")
        (result, valid_channels) = self._check_channel_args(channels)

        for channel in valid_channels:
            proto = self.chan_map[channel]
            result[channel] = proto.execute_diagnostics(num_scans, *args,
                                                        **kwargs)

        return result

    def execute_get_power_statuses(self, channels=None, *args, **kwargs):
        """
        Gets the power statuses.

        @param channels List of channel names to execute the operation to,
                by default [TrhphChannel.INSTRUMENT].
        @param timeout Timeout for the wait, self._timeout by default.
        @retval a dict of power statuses per channel
        @throws TimeoutException
        """

        channels = channels or [TrhphChannel.INSTRUMENT]

        mi_logger.debug("Issuing base execute_diagnostics...")
        (result, valid_channels) = self._check_channel_args(channels)

        for channel in valid_channels:
            proto = self.chan_map[channel]
            result[channel] = proto.execute_get_power_statuses(*args,
                                                               **kwargs)

        return result
