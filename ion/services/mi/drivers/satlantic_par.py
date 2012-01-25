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

from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol import CommandResponseInstrumentProtocol
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_connection import SerialInstrumentConnection
from ion.services.mi.common import InstErrorCode
from ion.services.mi.common import DriverAnnouncement
from ion.services.mi.comms_method import AMQPCommsMethod
from ion.services.mi.instrument_fsm import InstrumentFSM

####################################################################
# Static enumerations for this class
####################################################################

class Channel(BaseEnum):
    """Just default instrument driver channels, add no more"""
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
    COMMAND = 'COMMAND'
    EXIT_STATE = 'EXIT'
    ENTER_STATE = 'ENTER'
    

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
    
    
    def __init__(self, connection, callback=None):
        CommandResponseInstrumentProtocol.__init__(self, connection, callback,
              command_list=Command,
              get_prefix="show ",
              set_prefix="set ",
              set_delimiter=" ",
              execute_prefix="",
              eoln="\n")
        
        self._state_handlers = {
            State.AUTOSAMPLE_MODE : self._state_handler_autosample,
            State.COMMAND_MODE : self._state_handler_command,
            State.POLL_MODE : self._state_handler_poll
        }
        
        self.protocol_fsm = InstrumentFSM(State, Event, self._state_handlers,
                                          Event.ENTER_STATE, Event.EXIT_STATE)

    # The normal interface for a protocol. These should drive the FSM
    # transitions as they get things done.
    def get(self, params=[]):
        # check param
        # build query
        # get result from instrument
        # build return
        pass
    
    def set(self, params={}):
        # check param
        # build set
        # set and get result from instrument
        # build return
        pass
    
    def execute(self, command=[]):
        # check command and arguments for validity
        # build command string
        # execute and get result from instrument
        # build return
        pass
    
    def get_config(self):
        # build query
        # get result from instrument
        # build return
        pass
    
    def restore_config(self, config={}):
        # check param list
        # build set string
        # set and get result from instrument
        # build return
        pass
    
    def get_status(self):
        # build status query
        # get result from instrument
        # build return
        pass
    
    def _break_from_autosample(self, break_char):
        """Break out of autosample mode.
        
        Issue the proper sequence of stuff to get the device out of autosample
        mode. The character used will result in a different end state. Ctrl-S
        goes to poll mode, Ctrl-C goes to command mode. Ctrl-R resets. 
        @param break_char The character to send to get out of autosample.
        Should be Event.STOP, Event.BREAK, or Event.RESET.
        @retval return True for success, Error for failure
        @todo Write this
        """
        assert break_char == (Event.STOP or Event.BREAK or Event.RESET), "Bad argument to break_from_autosample()"
        
        # do the magic sequence of sending lots of characters really fast
        
        
    ################
    # State handlers
    ################
    def _state_handler_autosample(self, event, params):
        """Handle State.AUTOSAMPLE_MODE state
        
        @param event The event being handed into this state
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        """
        success = InstErrorCode.OK
        next_state = None
        result = None
        
        if event == Event.ENTER_STATE:
            pass
                
        elif event == Event.RESET:
            if (self._break_from_autosample(Event.RESET)):
                self.publish_to_driver((DriverAnnouncement.STATE_CHANGE, None,
                                        "Reset while autosampling!"))
        
        elif event == Event.BREAK:
            if (self._break_from_autosample(Event.BREAK)):
                self.publish_to_driver((DriverAnnouncement.STATE_CHANGE, None,
                                        "Leaving auto sample!"))
                next_state = State.COMMAND_MODE
            else:
                self.publish_to_driver((DriverAnnouncement.ERROR,
                                        InstErrorCode.HARDWARE_ERROR,
                                        "Could not break from autosample!"))
                success = InstErrorCode.HARDWARE_ERROR
            
        elif event == Event.STOP:
            if (self._break_from_autosample(Event.STOP)):
                self.publish_to_driver((DriverAnnouncement.STATE_CHANGE, None,
                                        "Leaving auto sample!"))
                next_state = State.POLL_MODE
            else:
                self.publish_to_driver((DriverAnnouncement.ERROR,
                                        InstErrorCode.HARDWARE_ERROR,
                                        "Could not stop autosample!"))
                success = InstErrorCode.HARDWARE_ERROR
                
        elif event == Event.EXIT_STATE:
            pass
            
        return (success, next_state, result)
        

    def _state_handler_command(self, event, params):
        """Handle State.COMMAND_MODE state
        
        @param event The event being handed into this state
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        """
        success = InstErrorCode.OK
        next_state = None
        result = None
        
        if event == Event.ENTER_STATE:
            pass
                
        elif event == Event.RESET:
            # @todo do reset things, still in cmd mode
            self.publish_to_driver((DriverAnnouncement.STATE_CHANGE, None,
                                    "Reset while autosampling!"))
        
        elif event == Event.COMMAND:
            """ @todo Add command logic handling here """
            if params == Command.EXIT:
                pass
            
            if params == Command.EXIT_AND_RESET:
                pass
            
            if params == Command.SAVE:
                pass
            
        elif event == Event.EXIT_STATE:
            pass
            
        return (success, next_state, result)

    def _state_handler_poll(self, event, params):
        """Handle State.POLL_MODE state
        
        @param event The event being handed into this state
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        """
        success = InstErrorCode.OK
        next_state = None
        result = None
        
        if event == Event.ENTER_STATE:
            pass
                
        elif event == Event.RESET:
            # do reset things, still in poll state
            self.publish_to_driver((DriverAnnouncement.STATE_CHANGE, None,
                                    "Reset while autosampling!"))
        
        elif event == Event.COMMAND:
            """ @todo Add command logic handling here for CR and space """
            if param == (Event.SAMPLE):
                # get the sample
                pass
            else:
                self.publish_to_driver((DriverAnnouncement.ERROR,
                                        InstErrorCode.INVALID_COMMAND,
                                        "Could not get sample"))
                success = InstErrCode.INVALID_COMMAND
        
        elif event == Event.AUTOSAMPLE:
            """ @todo issue Ctrl-A"""
            next_state = State.AUTOSAMPLE_MODE
            
        elif event == Event.EXIT_STATE:
            #send even to driver
            pass
            
        return (success, next_state, result)


class SatlanticPARInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the Satlantic PAR sensor PARAD"""

    def __init__(self):
        """Instrument-specific enums"""
        self.comms_method = AMQPCommsMethod()
        self.instrument_connection = SerialInstrumentConnection()
        self.instrument_commands = Command()
        self.instrument_metadata_parameters = MetadataParameter()
        self.instrument_parameters = Parameter()
        self.instrument_channels = Channel()
        self.instrument_errors = Error()
        self.instrument_capabilities = Capability()
        self.instrument_status = Status()
        self.protocol = SatlanticPARInstrumentProtocol(self.instrument_connection,
                                                       self.announce_to_driver)

# Special data decorators?
