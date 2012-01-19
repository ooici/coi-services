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

# imports go here
from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol import ScriptInstrumentProtocol
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_connection import SerialInstrumentConnection


####################################################################
# Static enumerations for this class
####################################################################
class Channel(BaseEnum):
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


class Command(BaseEnum):
    pass


class State(BaseEnum):
    AUTOSAMPLE_MODE = 'AUTOSAMPLE_MODE'
    COMMAND_MODE = 'COMMAND_MODE'


class Event(BaseEnum):
    pass


class Status(BaseEnum):
    pass


class MetadataParameter(BaseEnum):
    pass


class Parameter(BaseEnum):
    pass


class Error(BaseEnum):
    pass


class Capability(BaseEnum):
    pass


####################################################################
# Protocol
####################################################################
class UWashBarsInstrumentProtocol(ScriptInstrumentProtocol):
    """The instrument protocol classes to deal with a TRHPH BARS sensor.

    """

    def __init__(self, connection):
        ScriptInstrumentProtocol.__init__(self, connection)

#        self.protocol_fsm = FSM(State.AUTOSAMPLE_MODE)

        pass




class UWashBarsInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the TRHPH BARS sensor"""

    def __init__(self):
        self.instrument_connection = SerialInstrumentConnection()
        self.protocol = UWashBarsInstrumentProtocol(self.instrument_connection)

#        self.comms_method = AMQPCommsMethod()
        self.instrument_commands = Command()
        self.instrument_metadata_parameters = MetadataParameter()
        self.instrument_parameters = Parameter()
        self.instrument_channels = Channel()
        self.instrument_errors = Error()
        self.instrument_capabilities = Capability()
        self.instrument_status = Status()

# Special data decorators?
