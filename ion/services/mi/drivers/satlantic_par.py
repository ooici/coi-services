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

import logging

from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol import CommandResponseInstrumentProtocol
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_connection import SerialInstrumentConnection
from ion.services.mi.common import InstErrorCode
from ion.services.mi.common import DriverAnnouncement
from ion.services.mi.instrument_fsm import InstrumentFSM

mi_logger = logging.getLogger('mi_logger')

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
    GET = 'get'
    SET = 'set'

class State(BaseEnum):
    COMMAND_MODE = 'COMMAND_MODE'
    POLL_MODE = 'POLL_MODE'
    AUTOSAMPLE_MODE = 'AUTOSAMPLE_MODE'
    UNKNOWN = 'UNKNOWN'

class Event(BaseEnum):
    RESET = 0x12
    BREAK = 0x03
    STOP = 0x13
    AUTOSAMPLE = 0x01
    SAMPLE = 0x0D
    COMMAND = 'COMMAND'
    EXIT_STATE = 'EXIT'
    ENTER_STATE = 'ENTER'
    INITIALIZE = 'INITIALIZE'
    

class Status(BaseEnum):
    pass

class MetadataParameter(BaseEnum):
    pass

class Parameter(BaseEnum):
    TELBAUD = 'telbaud'
    MAXRATE = 'maxrate'
    
class Prompt(BaseEnum):
    """
    Command Prompt
    """
    COMMAND = '$'
    
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
    
    
    def __init__(self, callback=None):
        CommandResponseInstrumentProtocol.__init__(self, callback, Prompt, "\n")
        
        self._fsm = InstrumentFSM(State, Event, Event.ENTER_STATE,
                                  Event.EXIT_STATE,
                                  InstErrorCode.UNHANDLED_EVENT)
        self._fsm.add_handler(State.COMMAND_MODE, Event.COMMAND,
                              self._handler_command_command)
        self._fsm.add_handler(State.COMMAND_MODE, Event.RESET,
                              self._handler_reset)
        self._fsm.add_handler(State.AUTOSAMPLE_MODE, Event.BREAK,
                              self._handler_autosample_break)
        self._fsm.add_handler(State.AUTOSAMPLE_MODE, Event.STOP,
                              self._handler_autosample_stop)
        self._fsm.add_handler(State.AUTOSAMPLE_MODE, Event.RESET,
                              self._handler_reset)
        self._fsm.add_handler(State.POLL_MODE, Event.COMMAND,
                              self._handler_poll_command)
        self._fsm.add_handler(State.POLL_MODE, Event.AUTOSAMPLE,
                              self._handler_poll_autosample)
        self._fsm.add_handler(State.POLL_MODE, Event.RESET,
                              self._handler_reset)
        self._fsm.add_handler(State.UNKNOWN, Event.INITIALIZE,
                              self._handler_initialize)
        self._fsm.start(State.UNKNOWN)

        self._add_build_handler(Command.SET, self._build_set_command)
        self._add_build_handler(Command.GET, self._build_simple_command)
        
    # The normal interface for a protocol. These should drive the FSM
    # transitions as they get things done.
    def get(self, params=[]):
        (success, result) = self._fsm.on_event(Event.COMMAND,
                                               {'command':Command.GET,
                                                'params':params})

        # check param
        for param in params:
            if not Parameter.has(param):
                raise InstrumentProtocolException
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
    
    def initialize(self, timeout=10):
        CommandResponseInstrumentProtocol.initialize(self, timeout)
        self._fsm.on_event(Event.INITIALIZE)
            
    def _break_from_autosample(self, break_char, timeout=30):
        """Break out of autosample mode.
        
        Issue the proper sequence of stuff to get the device out of autosample
        mode. The character used will result in a different end state. Ctrl-S
        goes to poll mode, Ctrl-C goes to command mode. Ctrl-R resets. 
        @param break_char The character to send to get out of autosample.
        Should be Event.STOP, Event.BREAK, or Event.RESET.
        @retval return True for success, Error for failure
        @throw InstrumentTimeoutException
        @throw InstrumentProtocolException
        """
        
        if not ((break_char == Event.BREAK)
            or (break_char == Event.STOP)
            or (break_char == Event.RESET)):
            return False
        
        # do the magic sequence of sending lots of characters really fast
        self._wakeup()
        starttime = time.time()
        while True:
            self._logger_client.send(Event.BREAK+Event.BREAK)
            (prompt, result) = self._get_response(timeout)
            mi_logger.debug("Got prompt %s when trying to break from autosample",
                            prompt)
            if (prompt):                
                return True
            else:
                if time.time() > starttime + timeout:
                    raise InstrumentProtocolException(InstErrorCode.TIMEOUT)
                    
        # catch all
        return False
    
    ################
    # State handlers
    ################
    def _handler_initialize(self, params):
        """Handle transition from UNKNOWN state to a known one.
        
        This method determines what state the device is in or gets it to a
        known state so that the instrument and protocol are in sync.
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        """
        next_state = None
        result = None
        
        # Break to command mode, then set next state to command mode
        if self._break_from_autosample(Event.BREAK):
            next_state = State.COMMAND_MODE
            
        return (next_state, result)
        
        
    def _handler_reset(self, params):
        """Handle reset condition for all states.
        
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        """
        next_state = None
        result = None
        if (self._break_from_autosample(Event.RESET)):
            self.publish_to_driver((DriverAnnouncement.STATE_CHANGE, None,
                                    "Reset while autosampling!"))

        ''' @todo fill this in '''
        return (next_state, result)
        
    def _handler_autosample_break(self, params):
        """Handle State.AUTOSAMPLE_MODE Event.BREAK
        
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        @throw InstrumentProtocolException For hardware error
        """
        next_state = None
        result = None
        
        if (self._break_from_autosample(Event.BREAK)):
            self.publish_to_driver((DriverAnnouncement.STATE_CHANGE, None,
                                    "Leaving auto sample!"))
            next_state = State.COMMAND_MODE
        else:
            self.publish_to_driver((DriverAnnouncement.ERROR,
                                    InstErrorCode.HARDWARE_ERROR,
                                    "Could not break from autosample!"))
            raise InstrumentProtocolException(InstErrorCode.HARDWARE_ERROR)
            
        return (next_state, result)
        
    def _handler_autosample_stop(self, params):
        """Handle State.AUTOSAMPLE_MODE Event.STOP
        
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        @throw InstrumentProtocolException For hardware error
        """
        next_state = None
        result = None
        
        if (self._break_from_autosample(Event.STOP)):
            self.publish_to_driver((DriverAnnouncement.STATE_CHANGE, None,
                                    "Leaving auto sample!"))
            next_state = State.POLL_MODE
        else:
            self.publish_to_driver((DriverAnnouncement.ERROR,
                                    InstErrorCode.HARDWARE_ERROR,
                                    "Could not stop autosample!"))
            raise InstrumentProtocolException(InstErrorCode.HARDWARE_ERROR)
                
        return (next_state, result)
        

    def _handler_command_command(self,params):
        """Handle State.COMMAND_MODE Event.COMMAND transition
        
        @param params Dict with "command" enum and "params" of the parameters to
        pass to the state
        @retval return (success/fail code, next state, result)
        @throw InstrumentProtocolException For invalid parameter
        """
        next_state = None
        result = None
                
        """ @todo Add command logic handling here """
        if params['command'] == Command.EXIT:
            pass
        
        if params['command'] == Command.EXIT_AND_RESET:
            pass
           
        if params['command'] == Command.SAVE:
            pass

        if params['command'] == Command.SET:
            pass
        
        if params['command'] == Command.GET:
            for param in params['params']:
                if not Parameter.has(param):
                    raise InstrumentProtocolException(InstErrorCode.INVALID_PARAMETER)
                    break
                result_vals[param] = self._do_cmd_resp(Command.GET, param)
            result = result_vals
            
        return (next_state, result)

    def _handler_poll_command(self, params):
        """Handle State.POLL_MODE Event.COMMAND
        
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        @throw InstrumentProtocolException For invalid command
        """
        next_state = None
        result = None
                
        """ @todo Add command logic handling here for CR and space """
        if param == (Event.SAMPLE):
            # get the sample
            pass
        else:
            self.publish_to_driver((DriverAnnouncement.ERROR,
                                    InstErrorCode.INVALID_COMMAND,
                                    "Could not get sample"))
            raise InstrumentProtocolException(InstErrCode.INVALID_COMMAND)
  
        return (next_state, result)

    def _handler_poll_autosample(self, params):
        """Handle State.POLL_MODE Event.AUTOSAMPLE
        
        @param params Parameters to pass to the state
        @retval return (success/fail code, next state, result)
        """
        next_state = None
        result = None
                
        """ @todo issue Ctrl-A"""
        next_state = State.AUTOSAMPLE_MODE
                        
        return (next_state, result)

    def _build_set_command(self, param, value):
        """
        Build a command that is ready to send out to the instrument. Checks for
        valid parameter name, only handles one value at a time.
        
        @param param The name of the parameter to set. From Parameter enum
        @param value The value to set for that parameter
        @retval Returns string ready for sending to instrument
        """
        # Check to make sure all parameters are valid up front
        
        # return the string to send

class SatlanticPARInstrumentDriver(InstrumentDriver):
    """The InstrumentDriver class for the Satlantic PAR sensor PARAD"""

    def __init__(self):
        """Instrument-specific enums"""
        self.instrument_connection = SerialInstrumentConnection()
        self.instrument_commands = Command()
        self.instrument_metadata_parameters = MetadataParameter()
        self.instrument_parameters = Parameter()
        self.instrument_channels = Channel()
        self.instrument_errors = Error()
        self.instrument_capabilities = Capability()
        self.instrument_status = Status()
        self.protocol = SatlanticPARInstrumentProtocol(self.announce_to_driver)

# Special data decorators?
