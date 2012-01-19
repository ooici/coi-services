#!/usr/bin/env python

"""
@package ion.services.mi.drivers.satlantic_par Satlantic PAR driver module
@file ion/services/mi/drivers/satlantic_par.py
@author Steve Foley
@brief Instrument driver classes that provide structure towards interaction
with the Satlantic PAR sensor (PARAD in RSN nomenclature).
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

# imports go here

####################################################################
# Static enumerations for this class
####################################################################
class Channel(BaseEnum):
    pass

class Command(BaseEnum):
    SAVE = 'save'
    EXIT = 'exit'
    EXIT_AND_RESET = 'exit!'

class State(BaseEnum):
    COMMAND_MODE = 'COMMAND_MODE'
    POLL_MODE = 'POLL_MODE'
    AUTOSAMPLE_MODE = 'AUTOSAMPLE_MODE'

class Event(BaseEnum):
    RESET = 0x12
    BREAK = 0x03
    STOP = 0x13
    AUTOSAMPLE = 0x01
    SAMPLE = 0x0D
    EXIT = 'EXIT'
    EXIT_AND_RESET = 'EXIT_AND_RESET'

class Status(BaseEnum):
    pass

class MetadataParameter(BaseEnum):
    pass

class Parameter(BaseEnum):
    TELBAUD = 'telbaud'
    MAXRATE = 'maxrate'
    
class Error(BaseEnum):
    pass

class Capability(BaseEnum):
    pass

####################################################################
# Protocol
####################################################################
class SatlanticPARInstrumentProtocol(CommandResponseInstrumentProtocol):
    """The instrument protocol classes to deal with a Satlantic PAR sensor.
    
    The protocol is a very simple command/response protocol with a few show
    commands and a few set commands.
    """
    
    
    def __init__(self, connection):
        CommandResponseInstrumentProtocol.__init__(self, connection,
              command_list=Command,
              get_prefix="show ",
              set_prefix="set ",
              set_delimiter=" ",
              execute_prefix="",
              eoln="\n")
        
        self.protocol_fsm = FSM(State.AUTOSAMPLE_MODE)
        self.protocol_fsm.add_transition_catch(Event.RESET,
                                         action=self._handle_reset,
                                         next_state=State.AUTOSAMPLE_MODE)
        self.protocol_fsm.add_transition(Event.BREAK,
                                         State.AUTOSAMPLE_MODE,
                                         action=self._handle_break,
                                         next_state=State.COMMAND_MODE)
        self.protocol_fsm.add_transition(Event.STOP,
                                         State.AUTOSAMPLE_MODE,
                                         action=self._handle_stop,
                                         next_state=State.POLL_MODE)
        self.protocol_fsm.add_transition(Event.AUTOSAMPLE,
                                         State.POLL_MODE,
                                         action=self._handle_autosample,
                                         next_state=State.AUTOSAMPLE_MODE)
        self.protocol_fsm.add_transition(Event.SAMPLE,
                                         State.POLL_MODE,
                                         action=self._handle_sample,
                                         next_state=State.POLL_MODE)
        self.protocol_fsm.add_transition_list([Event.EXIT,
                                               Event.EXIT_AND_RESET],
                                         State.COMMAND_MODE,
                                         action=self._handle_exit,
                                         next_state=State.POLL_MODE)
        # Handle commands as part of the input stream
        self.protocol_fsm.add_transition_list(Command.list(),
                                              State.COMMAND_MODE,
                                              action=self._handle_commands,
                                              next_state=State.COMMAND_MODE)

    # The normal interface for a protocol. These should drive the FSM
    # transitions as they get things done.
    def get(self, params=[]):
        pass
    
    def set(self, params={}):
        pass
    
    def execute(self, command=[]):
        pass
    
    def get_config(self):
        pass
    
    def restore_config(self, config={}):
        pass
    
    def get_status(self):
        pass
    
    def _break_to_command_mode(self):
        # Ctrl-C does it for this instrument
        pass
    
    def _handle_exit(self):
        """Handle exit or exit_and_reset transition"""

    def _handle_sample(self):
        """Handle sample transition"""

    def _handle_autosample(self):
        """Handle autosample transition"""
    
    def _handle_stop(self):
        """Handle stop transition"""
    
    def _handle_break(self):
        """Handle break transition"""
    
    def _handle_reset(self):
        """Handle reset transition"""
        
    def _handle_commands(self):
        """Handle command input while in command mode"""
        # Driven by the get/set/execute calls above
        # switch on current symbol, do something if it needs to be done
        # on state transition...or not.


class SatlanticPARInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the Satlantic PAR sensor PARAD"""

    def __init__(self):
        self.protocol = SatlanticPARInstrumentProtocol()
        self.comms_method = AMQPCommsMethod()
        self.instrument_connection = SerialInstrumentConnection()
        self.instrument_commands = Command()
        self.instrument_metadata_parameters = MetadataParameter()
        self.instrument_parameters = Parameter()
        self.instrument_channels = Channel()
        self.instrument_errors = Error()
        self.instrument_capabilities = Capability()
        self.instrument_status = Status()

# Special data decorators?
