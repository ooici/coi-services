#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uwash_bars U. Washington TRHPH BARS driver
module
@file ion/services/mi/drivers/uwash_bars.py
@author Carlos Rueda
@brief Instrument driver classes to support interaction with the U. Washington
 TRHPH BARS sensor .

@NOTE preliminary skeleton, largely based on Steve's satlantic_par.py
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol import ScriptInstrumentProtocol
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_connection import SerialInstrumentConnection
from ion.services.mi.comms_method import AMQPCommsMethod

from pyon.util.fsm import FSM


####################################################################
# Static enumerations for this class
####################################################################
class BarsChannel(BaseEnum):
    RESISTIVITY_5 = "Resistivity/5"
    RESISTIVITY_X1 = "Resistivity X1"
    RESISTIVITY_X5 = "Resistivity X5"
    HYDROGEN_5 = "Hydrogen/5"
    HYDROGEN_X1 = "Hydrogen X1"
    HYDROGEN_X5 = "Hydrogen X5"
    EH_SENSOR = "Eh Sensor"
    REFERENCE_TEMP_VOLTS = "Reference Temp Volts"
    REFERENCE_TEMP_DEG_C = "Reference Temp Deg C"
    RESISTIVITY_TEMP_VOLTS = "Resistivity Temp Volts"
    RESISTIVITY_TEMP_DEG_C = "Resistivity Temp Deg C"
    BATTERY_VOLTAGE = "Battery Voltage"


class BarsCommand(BaseEnum):
    pass


class BarsState(BaseEnum):
    AUTOSAMPLE_MODE = 'AUTOSAMPLE_MODE'
    COMMAND_MODE = 'COMMAND_MODE'


class BarsEvent(BaseEnum):
    GO_TO_MENU = 0x23
    BREAK = 0x03
    STOP = 0x13
    AUTOSAMPLE = 0x01
    SAMPLE = 0x0D
    EXIT = 'EXIT'
    EXIT_AND_RESET = 'EXIT_AND_RESET'


class BarsStatus(BaseEnum):
    pass


class BarsMetadataParameter(BaseEnum):
    pass


class BarsParameter(BaseEnum):
    pass


class BarsError(BaseEnum):
    pass


class BarsCapability(BaseEnum):
    pass


####################################################################
# Protocol
####################################################################
class BarsInstrumentProtocol(ScriptInstrumentProtocol):
    """The instrument protocol classes to deal with a TRHPH BARS sensor.

    """

    def __init__(self, connection):
        ScriptInstrumentProtocol.__init__(self, connection)

        self.protocol_fsm = FSM(BarsState.AUTOSAMPLE_MODE)

        self.protocol_fsm.add_transition_catch(BarsEvent.RESET,
                                         action=self._handle_reset,
                                         next_state=BarsState.AUTOSAMPLE_MODE)
        self.protocol_fsm.add_transition(BarsEvent.BREAK,
                                         BarsState.AUTOSAMPLE_MODE,
                                         action=self._handle_break,
                                         next_state=BarsState.COMMAND_MODE)
        self.protocol_fsm.add_transition(BarsEvent.STOP,
                                         BarsState.AUTOSAMPLE_MODE,
                                         action=self._handle_stop,
                                         next_state=BarsState.POLL_MODE)
        self.protocol_fsm.add_transition(BarsEvent.AUTOSAMPLE,
                                         BarsState.POLL_MODE,
                                         action=self._handle_autosample,
                                         next_state=BarsState.AUTOSAMPLE_MODE)
        self.protocol_fsm.add_transition(BarsEvent.SAMPLE,
                                         BarsState.POLL_MODE,
                                         action=self._handle_sample,
                                         next_state=BarsState.POLL_MODE)
        self.protocol_fsm.add_transition_list([BarsEvent.EXIT,
                                               BarsEvent.EXIT_AND_RESET],
                                         StaBarsStatete.COMMAND_MODE,
                                         action=self._handle_exit,
                                         next_state=BarsState.POLL_MODE)
        # Handle commands as part of the input stream
        self.protocol_fsm.add_transition_list(BarsCommand.list(),
                                              BarsState.COMMAND_MODE,
                                              action=self._handle_commands,
                                              next_state=BarsState. \
                                              COMMAND_MODE)


class BarsInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the TRHPH BARS sensor"""

    def __init__(self):
        self.instrument_connection = SerialInstrumentConnection()
        self.protocol = BarsInstrumentProtocol(self.instrument_connection)

        self.comms_method = AMQPCommsMethod()
        self.instrument_commands = BarsCommand()
        self.instrument_metadata_parameters = BarsMetadataParameter()
        self.instrument_parameters = BarsParameter()
        self.instrument_channels = BarsChannel()
        self.instrument_errors = BarsError()
        self.instrument_capabilities = BarsCapability()
        self.instrument_status = BarsStatus()

# Special data decorators?
