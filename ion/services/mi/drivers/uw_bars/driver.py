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
from ion.services.mi.instrument_driver import ConnectionState

from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsCommand
from ion.services.mi.drivers.uw_bars.common import BarsParameter
from ion.services.mi.drivers.uw_bars.common import BarsError
from ion.services.mi.drivers.uw_bars.protocol import BarsProtocolState


from ion.services.mi.mi_logger import mi_logger
log = mi_logger


class BarsInstrumentDriver(InstrumentDriver):
    """
    The InstrumentDriver class for the TRHPH BARS sensor.
    """

    # TODO NOTE: Assumes all interaction is for the INSTRUMENT special channel

    def __init__(self, evt_callback=None):
        log.debug("Creating BarsInstrumentDriver")
        InstrumentDriver.__init__(self, evt_callback)

#        self.instrument_connection = SerialInstrumentConnection()
        self.instrument_commands = BarsCommand
        self.instrument_parameters = BarsParameter
        self.instrument_channels = BarsChannel
        self.instrument_errors = BarsError
#        self.instrument_states = State
#        self.instrument_active_states = [State.COMMAND_MODE,
#                                         State.AUTOSAMPLE_MODE,
#                                         State.POLL_MODE]
        protocol = BarsInstrumentProtocol(self.protocol_callback)
        self.protocol = protocol
        self.chan_map = {BarsChannel.INSTRUMENT: protocol}

        self.state_map = {
            BarsProtocolState.PRE_INIT: DriverState.UNCONFIGURED,
            BarsProtocolState.COLLECTING_DATA: DriverState.AUTOSAMPLE,

            # TODO: review this (driver state vs. protocol state)
            ConnectionState.DISCONNECTED: DriverState.DISCONNECTED
        }
