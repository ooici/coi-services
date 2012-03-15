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
import time
import re

from ion.services.mi.common import BaseEnum
from ion.services.mi.data_decorator import ChecksumDecorator
from ion.services.mi.instrument_protocol import CommandResponseInstrumentProtocol
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverChannel
from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.common import InstErrorCode
from ion.services.mi.common import DriverAnnouncement
from ion.services.mi.instrument_fsm_args import InstrumentFSM
from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentDataException

mi_logger = logging.getLogger('mi_logger')

####################################################################
# Module-wide values
####################################################################

# ex SATPAR0229,10.01,2206748544,234
sample_pattern = r'SATPAR(?P<sernum>\d{4}),(?P<timer>\d{1,7}.\d\d),(?P<counts>\d{10}),(?P<checksum>\d{1,3})'
sample_regex = re.compile(sample_pattern)
        
####################################################################
# Static enumerations for this class
####################################################################

class Channel(BaseEnum):
    # defaults
    INSTRUMENT = DriverChannel.INSTRUMENT
    ALL = DriverChannel.ALL
    # Name the one specific channel we respond as
    PAR = 'CHANNEL_PAR'

class Command(BaseEnum):
    SAVE = 'save'
    EXIT = 'exit'
    EXIT_AND_RESET = 'exit!'
    POLL = 'POLL'
    GET = 'show'
    SET = 'set'
    RESET = 0x12
    BREAK = 0x03
    STOP = 0x13
    AUTOSAMPLE = 0x01
    SAMPLE = 0x0D

class State(BaseEnum):
    COMMAND_MODE = 'COMMAND_MODE'
    POLL_MODE = 'POLL_MODE'
    AUTOSAMPLE_MODE = 'AUTOSAMPLE_MODE'
    UNKNOWN = 'UNKNOWN'

class Event(BaseEnum):
    RESET = 'RESET'
    BREAK = 'BREAK'
    STOP = 'STOP'
    AUTOSAMPLE = 'AUTOSAMPLE'
    SAMPLE = 'SAMPLE'
    COMMAND = 'COMMAND'
    EXIT_STATE = 'EXIT'
    ENTER_STATE = 'ENTER'
    CONFIGURE = 'INITIALIZE'
    GET = 'GET'
    SET = 'SET'

class Parameter(BaseEnum):
    TELBAUD = 'telbaud'
    MAXRATE = 'maxrate'
    
class Prompt(BaseEnum):
    """
    Command Prompt
    """
    COMMAND = '$'
    NULL = ''
    
class Error(BaseEnum):
    INVALID_COMMAND = "Invalid command"
    
class KwargsKey(BaseEnum):
    COMMAND = 'command'

####################################################################
# Protocol
####################################################################
class SatlanticPARInstrumentProtocol(CommandResponseInstrumentProtocol):
    """The instrument protocol classes to deal with a Satlantic PAR sensor.
    
    The protocol is a very simple command/response protocol with a few show
    commands and a few set commands.
    @todo Check for valid state transitions and handle requests appropriately
    possibly using better exceptions from the fsm.on_event() method
    """
    
    
    def __init__(self, callback=None):
        CommandResponseInstrumentProtocol.__init__(self, callback, Prompt, "\r\n")
        
        self._fsm = InstrumentFSM(State, Event, Event.ENTER_STATE,
                                  Event.EXIT_STATE,
                                  InstErrorCode.UNHANDLED_EVENT)
        self._fsm.add_handler(State.COMMAND_MODE, Event.COMMAND,
                              self._handler_command_command)
        self._fsm.add_handler(State.COMMAND_MODE, Event.ENTER_STATE,
                              self._handler_command_enter_state)
        self._fsm.add_handler(State.COMMAND_MODE, Event.GET,
                              self._handler_command_get)    
        self._fsm.add_handler(State.COMMAND_MODE, Event.SET,
                              self._handler_command_set)
        self._fsm.add_handler(State.AUTOSAMPLE_MODE, Event.BREAK,
                              self._handler_autosample_break)
        self._fsm.add_handler(State.AUTOSAMPLE_MODE, Event.STOP,
                              self._handler_autosample_stop)
        self._fsm.add_handler(State.AUTOSAMPLE_MODE, Event.RESET,
                              self._handler_reset)
        self._fsm.add_handler(State.AUTOSAMPLE_MODE, Event.COMMAND,
                              self._handler_autosample_command)
        self._fsm.add_handler(State.POLL_MODE, Event.AUTOSAMPLE,
                              self._handler_poll_autosample)
        self._fsm.add_handler(State.POLL_MODE, Event.RESET,
                              self._handler_reset)
        self._fsm.add_handler(State.POLL_MODE, Event.BREAK,
                              self._handler_poll_break)
        self._fsm.add_handler(State.POLL_MODE, Event.SAMPLE,
                              self._handler_poll_sample)
        self._fsm.add_handler(State.POLL_MODE, Event.COMMAND,
                              self._handler_poll_command)
        self._fsm.add_handler(State.UNKNOWN, Event.CONFIGURE,
                              self._handler_configure)
        self._fsm.start(State.UNKNOWN)

        self._add_build_handler(Command.SET, self._build_set_command)
        self._add_build_handler(Command.GET, self._build_param_fetch_command)
        self._add_build_handler(Command.SAVE, self._build_exec_command)
        self._add_build_handler(Command.EXIT, self._build_exec_command)
        self._add_build_handler(Command.EXIT_AND_RESET, self._build_exec_command)
        self._add_build_handler(Command.AUTOSAMPLE, self._build_control_command)
        self._add_build_handler(Command.RESET, self._build_control_command)
        self._add_build_handler(Command.BREAK, self._build_control_command)
        self._add_build_handler(Command.SAMPLE, self._build_control_command)
        self._add_build_handler(Command.STOP, self._build_control_command)

        self._add_response_handler(Command.GET, self._parse_get_response)
        self._add_response_handler(Command.SET, self._parse_set_response)
        self._add_response_handler(Command.BREAK, self._parse_silent_response)
        self._add_response_handler(Command.RESET, self._parse_silent_response)
        self._add_response_handler(Command.STOP, self._parse_silent_response)
        # self._add_response_handler(Command.GET, self._parse_get_response)

        self._add_param_dict(Parameter.TELBAUD,
                             r'Telemetry Baud Rate:\s+(\d+) bps',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        
        self._add_param_dict(Parameter.MAXRATE,
                             r'Maximum Frame Rate:\s+(\d+) Hz',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
                
    # The normal interface for a protocol. These should drive the FSM
    # transitions as they get things done.
    def get(self, *args, **kwargs):
        """ Get the given parameters from the instrument
        
        @param params The parameter values to get
        @retval Result of FSM event handle, hould be a dict of parameters and values
        @throws InstrumentProtocolException On invalid parameter
        """
        # Parameters checked in Handler
        result = self._fsm.on_event(Event.GET, *args, **kwargs)
        if result == None:
            raise InstrumentProtocolException(InstErrorCode.INCORRECT_STATE)
        assert (isinstance(result, dict))
        return result
   
    def set(self, *args, **kwargs):
        """ Set the given parameters on the instrument
        
        @param params The dict of parameters and values to set
        @retval result of FSM event handle
        @throws InstrumentProtocolException On invalid parameter
        """
        # Parameters checked in handler
        result = self._fsm.on_event(Event.SET, *args, **kwargs)
        if result == None:
            raise InstrumentProtocolException(InstErrorCode.INCORRECT_STATE)
        assert(isinstance(result, dict))
        return result
    
    def execute_save(self, *args, **kwargs):
        """ Execute the save command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        kwargs.update({KwargsKey.COMMAND:Command.SAVE})
        return self._fsm.on_event(Event.COMMAND, *args, **kwargs)
        
    def execute_exit(self, *args, **kwargs):
        """ Execute the exit command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        kwargs.update({KwargsKey.COMMAND:Command.EXIT})
        return self._fsm.on_event(Event.COMMAND, *args, **kwargs)
        
    def execute_exit_and_reset(self, *args, **kwargs):
        """ Execute the exit and reset command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        kwargs.update({KwargsKey.COMMAND:Command.EXIT_AND_RESET})
        return self._fsm.on_event(Event.COMMAND, *args, **kwargs)
    
    def execute_poll(self, *args, **kwargs):
        """ Execute the poll command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        kwargs.update({KwargsKey.COMMAND:Command.POLL})
        return self._fsm.on_event(Event.COMMAND, *args, **kwargs)
    
    def execute_reset(self, *args, **kwargs):
        """ Execute the reset command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        return self._fsm.on_event(Event.RESET, *args, **kwargs)
    
    def execute_break(self, *args, **kwargs):
        """ Execute the break command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        return self._fsm.on_event(Event.BREAK, *args, **kwargs)
    
    def execute_stop(self, *args, **kwargs):
        """ Execute the stop command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        return self._fsm.on_event(Event.STOP, *args, **kwargs)
    
    def execute_start_autosample(self, *args, **kwargs):
        """ Execute the autosample command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        return self.execute_exit(args, kwargs)
        
    def execute_stop_autosample(self, *args, **kwargs):
        """ Execute the autosample command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        return self._fsm.on_event(Event.BREAK, *args, **kwargs) 
    
    def execute_sample(self, *args, **kwargs):
        """ Execute the sample command

        @retval None if nothing was done, otherwise result of FSM event handle
        @throws InstrumentProtocolException On invalid command or missing
        """
        return self._fsm.on_event(Event.SAMPLE, *args, **kwargs)
        
    def get_config(self):
        """ Get the entire configuration for the instrument
        
        @param params The parameters and values to set
        @retval None if nothing was done, otherwise result of FSM event handle
        Should be a dict of parameters and values
        @throws InstrumentProtocolException On invalid parameter
        """
        result = self.get([Parameter.TELBAUD, Parameter.MAXRATE])
        assert (isinstance(result, dict))
        assert (result.has_key(Parameter.TELBAUD))
        assert (result.has_key(Parameter.MAXRATE))
        return result
        
    def restore_config(self, config=None):
        """ Apply a complete configuration.
        
        In this instrument, it is simply a compound set that must contain all
        of the parameters.
        @throws InstrumentProtocolException on missing or bad config
        """
        if (config == None):
            raise InstrumentProtocolException(InstErrorCode.INVALID_PARAMETER)
        
        if ((config.has_key(Parameter.TELBAUD))
            and (config.has_key(Parameter.MAXRATE))):  
            assert (isinstance(config, dict))
            assert (len(config) == 2)
            self.set(config)
        else:
            raise InstrumentProtocolException(InstErrorCode.INVALID_PARAMETER)
    
    def configure(self, config, *args, **kwargs):
        mi_logger.info('Configuring PAR sensor')
        CommandResponseInstrumentProtocol.configure(self, config, *args, **kwargs)
        self._fsm.on_event(Event.CONFIGURE, *args, **kwargs)
        
    ################
    # State handlers
    ################
    def _handler_configure(self, *args, **kwargs):
        """Handle transition from UNKNOWN state to a known one.
        
        This method determines what state the device is in or gets it to a
        known state so that the instrument and protocol are in sync.
        @param params Parameters to pass to the state
        @retval return (next state, result)
        """
        next_state = None
        result = None
        
        # Break to command mode, then set next state to command mode
        # If we are doing this, we must be connected
        if self._send_break(Command.BREAK):
            self.announce_to_driver(DriverAnnouncement.STATE_CHANGE,
                                    msg="Configured fresh, in command mode")            
            next_state = State.COMMAND_MODE
            
        return (next_state, result)
        
        
    def _handler_reset(self, *args, **kwargs):
        """Handle reset condition for all states.
        
        @param params Parameters to pass to the state
        @retval return (next state, result)
        """
        next_state = None
        result = None
        if (self._send_break(Command.RESET)):
            self.announce_to_driver(DriverAnnouncement.STATE_CHANGE,
                                    msg="Reset!")
            next_state = State.AUTOSAMPLE_MODE
            
        return (next_state, result)
        
    def _handler_autosample_break(self, *args, **kwargs):
        """Handle State.AUTOSAMPLE_MODE Event.BREAK
        
        @param params Parameters to pass to the state
        @retval return (next state, result)
        @throw InstrumentProtocolException For hardware error
        """
        next_state = None
        result = None
        
        if (self._send_break(Command.BREAK)):
            self.announce_to_driver(DriverAnnouncement.STATE_CHANGE,
                                    msg="Leaving auto sample!")
            next_state = State.COMMAND_MODE
        else:
            self.announce_to_driver(DriverAnnouncement.ERROR,
                                    error_code=InstErrorCode.HARDWARE_ERROR,
                                    msg="Could not break from autosample!")
            raise InstrumentProtocolException(InstErrorCode.HARDWARE_ERROR)
            
        return (next_state, result)
        
    def _handler_autosample_stop(self, *args, **kwargs):
        """Handle State.AUTOSAMPLE_MODE Event.STOP
        
        @param params Parameters to pass to the state
        @retval return (next state, result)
        @throw InstrumentProtocolException For hardware error
        """
        next_state = None
        result = None
        
        if (self._send_break(Command.STOP)):
            self.announce_to_driver(DriverAnnouncement.STATE_CHANGE,
                                    msg="Leaving auto sample!")
            next_state = State.POLL_MODE
        else:
            self.announce_to_driver(DriverAnnouncement.ERROR,
                                    error_code=InstErrorCode.HARDWARE_ERROR,
                                    msg="Could not stop autosample!")
            raise InstrumentProtocolException(InstErrorCode.HARDWARE_ERROR)
                
        return (next_state, result)

    def _handler_autosample_command(self, *args, **kwargs):
        """Handle State.AUTOSAMPLE_MODE Event.COMMAND transition
        
        @param params Dict with "command" enum and "params" of the parameters to
        pass to the state
        @retval return (next state, result)
        @throw InstrumentProtocolException For invalid parameter
        """
        next_state = None
        result = None
                    
        cmd = kwargs.get(KwargsKey.COMMAND, None)

        if (cmd == Command.BREAK):
            result = self._fsm.on_event(Event.BREAK, *args, **kwargs)
        elif (cmd == Command.STOP):
            result = self._fsm.on_event(Event.STOP, *args, **kwargs)
        elif (cmd == Command.RESET):
            result = self._fsm.on_event(Event.RESET, *args, **kwargs)
        else:
            raise InstrumentProtocolException(InstErrorCode.INVALID_COMMAND)
        
        mi_logger.debug("next: %s, result: %s", next_state, result) 
        return (next_state, result)

    def _handler_command_enter_state(self, *args, **kwargs):
        """Handle State.COMMAND_MODE Event.ENTER_STATE transition
        
        @param params Dict with "command" enum and "params" of the parameters to
        pass to the state
        @retval return (next state, result)
        @throw InstrumentProtocolException For invalid parameter'
        @todo Make this only active when we are connected.
        """
        # Just update parameters, no state change
        
        try:
            pass # dont do anything for now
            # self._update_params()
        except InstrumentTimeoutException:
            #squelch the error if we timeout...best effort update
            pass
        return (None, None)


    def _handler_command_command(self, *args, **kwargs):
        """Handle State.COMMAND_MODE Event.COMMAND transition
        
        @param params Dict with "command" enum and "params" of the parameters to
        pass to the state
        @retval return (next state, result)
        @throw InstrumentProtocolException For invalid parameter'
        @todo Fix this funky on_event logic...should just feed one on_event call
        """
        next_state = None
        result = None
        cmd = kwargs.get(KwargsKey.COMMAND, None)

        mi_logger.info("Handling command event [%s] in command mode...", cmd)
        if cmd == Command.EXIT:
            result = self._do_cmd_no_resp(Command.EXIT, None)
            if result:
                self.announce_to_driver(DriverAnnouncement.STATE_CHANGE,
                                         msg="Starting auto sample")
                next_state = State.AUTOSAMPLE_MODE
            
        elif cmd == Command.EXIT_AND_RESET:
            result = self._do_cmd_no_resp(Command.EXIT_AND_RESET, None)
            if result:
                self.announce_to_driver(DriverAnnouncement.STATE_CHANGE,
                                         msg="Starting auto sample")
                next_state = State.AUTOSAMPLE_MODE
            
        elif cmd == Command.SAVE:
            result = self._do_cmd_no_resp(Command.SAVE, None)
        
        elif cmd == Command.POLL:
            try:
                kwargs.update({KwargsKey.COMMAND:Command.EXIT})
                result = self._fsm.on_event(Event.COMMAND, *args, **kwargs)
                result = self._fsm.on_event(Event.STOP, *args, **kwargs)
                result = self._fsm.on_event(Event.SAMPLE, *args, **kwargs)
                # result should have data, right?
                mi_logger.debug("Polled sample: %s", result)
                result = self._fsm.on_event(Event.AUTOSAMPLE, *args, **kwargs)
                result = self._fsm.on_event(Event.BREAK, *args, **kwargs)   
            except (InstrumentTimeoutException, InstrumentProtocolException) as e:
                if self._fsm.current_state == State.AUTOSAMPLE_MODE:
                    result = self._fsm.on_event(Event.BREAK, *args, **kwargs)
                elif (self._fsm.current_state == State.POLL_MODE):
                    result = self._fsm.on_event(Event.AUTOSAMPLE, *args, **kwargs)
                    result = self._fsm.on_event(Event.BREAK, *args, **kwargs)
        
        else:
            raise InstrumentProtocolException(InstErrorCode.INVALID_COMMAND)

        mi_logger.debug("next: %s, result: %s", next_state, result) 
        return (next_state, result)

    def _handler_command_get(self, params=None, *args, **kwargs):
        """Handle getting data from command mode
         
        @param params List of the parameters to pass to the state
        @retval return (next state, result)
        @throw InstrumentProtocolException For invalid parameter
        """
        next_state = None
        result = None
        result_vals = {}    
        
        if ((params == None) or (not isinstance(params, list))):
                raise InstrumentProtocolException(InstErrorCode.INVALID_PARAMETER)
                
        for param in params:
            if not Parameter.has(param):
                raise InstrumentProtocolException(InstErrorCode.INVALID_PARAMETER)
                break
            result_vals[param] = self._do_cmd_resp(Command.GET, param,
                                                   expected_prompt=Prompt.COMMAND)
        result = result_vals
            
        mi_logger.debug("next: %s, result: %s", next_state, result) 
        return (next_state, result)

    def _handler_command_set(self, params, *args, **kwargs):
        """Handle setting data from command mode
         
        @param params Dict of the parameters and values to pass to the state
        @retval return (next state, result)
        @throw InstrumentProtocolException For invalid parameter
        """
        next_state = None
        result = None
        result_vals = {}    
        
        if ((params == None) or (not isinstance(params, dict))):
            raise InstrumentProtocolException(InstErrorCode.INVALID_PARAMETER)
        name_values = params
        for key in name_values.keys():
            if not Parameter.has(key):
                raise InstrumentProtocolException(InstErrorCode.INVALID_PARAMETER)
                break
            result_vals[key] = self._do_cmd_resp(Command.SET, key, name_values[key],
                                                 expected_prompt=Prompt.COMMAND)
            self._update_params()
        """@todo raise a parameter error if there was a bad value"""
        result = result_vals
            
        mi_logger.debug("next: %s, result: %s", next_state, result) 
        return (next_state, result)

    def _handler_poll_sample(self, *args, **kwargs):
        """Handle State.POLL_MODE Event.SAMPLE
        
        @retval return (next state, result)
        @throw InstrumentProtocolException For invalid command
        """
        next_state = None
        result = None
        
        result = self._do_cmd_resp(Command.SAMPLE, None,
                                   expected_prompt=Prompt.NULL)

        self.announce_to_driver(DriverAnnouncement.DATA_RECEIVED,
                                msg=result)          
        return (next_state, result)
    
    def _handler_poll_break(self, *args, **kwargs):
        """Handle State.POLL_MODE, Event.BREAK
        
        @retval return (next state, result)
        @throw InstrumentProtocolException For invalid command
        """
        next_state = None
        result = None
        
        result = self._send_break(Command.BREAK)
        
        if (result == False):
            raise InstrumentProtocolException(InstErrorCode.HARDWARE_ERROR,
                                              "Could not interrupt hardware!")
        else:
            next_state = State.COMMAND_MODE
            
        return (next_state, result)

    def _handler_poll_autosample(self, *args, **kwargs):
        """Handle State.POLL_MODE Event.AUTOSAMPLE
        
        @retval return (success/fail code, next state, result)
        """
        next_state = None
        result = None
                
        if (self._do_cmd_no_resp(Command.AUTOSAMPLE, None)):
            self.announce_to_driver(DriverAnnouncement.STATE_CHANGE,
                                    msg="Starting auto sample")
            next_state = State.AUTOSAMPLE_MODE
                        
        return (next_state, result)
        
    def _handler_poll_command(self, *args, **kwargs):
        """Handle State.POLL_MODE Event.COMMAND transition
        
        @param params Dict with "command" enum and "params" of the parameters to
        pass to the state
        @retval return (next state, result)
        @throw InstrumentProtocolException For invalid command
        """
        next_state = None
        result = None
        result_vals = {} 
        
        cmd = kwargs.get(KwargsKey.COMMAND, None)
        
        if (cmd == Command.AUTOSAMPLE):
            result = self._fsm.on_event(Event.AUTOSAMPLE, *args, **kwargs)
        elif (cmd == Command.RESET):
            result = self._fsm.on_event(Event.RESET, *args, **kwargs)
        elif (cmd == Command.POLL):
            result = self._fsm.on_event(Event.SAMPLE, *args, **kwargs)
        else:
            raise InstrumentProtocolException(InstErrorCode.INVALID_COMMAND)
        
        mi_logger.debug("next: %s, result: %s", next_state, result) 
        return (next_state, result)

    ###################################################################
    # Builders
    ###################################################################
    def _build_set_command(self, cmd, param, value):
        """
        Build a command that is ready to send out to the instrument. Checks for
        valid parameter name, only handles one value at a time.
        
        @param cmd The command...in this case, Command.SET
        @param param The name of the parameter to set. From Parameter enum
        @param value The value to set for that parameter
        @retval Returns string ready for sending to instrument
        """
        # Check to make sure all parameters are valid up front
        assert Parameter.has(param)
        assert cmd == Command.SET
        return "%s %s %s%s" % (Command.SET, param,
                               self._format_param_dict(param, value),
                               self.eoln)
        
    def _build_param_fetch_command(self, cmd, param):
        """
        Build a command to fetch the desired argument.
        
        @param cmd The command being used (Command.GET in this case)
        @param param The name of the parameter to fetch
        @retval Returns string ready for sending to instrument
        """
        assert Parameter.has(param)
        return "%s %s%s" % (Command.GET, param, self.eoln)
    
    def _build_exec_command(self, cmd, *args):
        """
        Builder for simple commands

        @param cmd The command being used (Command.GET in this case)
        @param args Unused arguments
        @retval Returns string ready for sending to instrument        
        """
        mi_logger.debug("*** building command with args: %s, cmd: %s", args, cmd)
        assert args == (None,)
        return "%s%s" % (cmd, self.eoln)
    
    def _build_control_command(self, cmd, *args):
        """ Send a quick series of control char command
        
        @param cmd The control character to send
        @param args Unused arguments
        @retval The string with the complete command (1 char)
        """
        return "%s%s%s" % (cmd, cmd, cmd)
    
    ##################################################################
    # Response parsers
    ##################################################################
    def _parse_set_response(self, response, prompt):
        """Determine if a set was successful or not
        
        @param response What was sent back from the command that was sent
        @param prompt The prompt that was returned from the device
        """
        mi_logger.debug("Parsing SET response of %s with prompt %s",
                        response, prompt)
        if prompt == Prompt.COMMAND:
            return True
        elif response == Error.INVALID_COMMAND:
            return InstErrorCode.SET_DEVICE_ERR
        else:
            return InstErrorCode.HARDWARE_ERROR
        
    def _parse_get_response(self, response, prompt):
        """ Parse the response from the instrument for a couple of different
        query responses.
        
        @param response The response string from the instrument
        @param prompt The prompt received from the instrument
        @retval return The numerical value of the parameter in the known units
        @todo Fill this in
        """
        # should only have one line with an eoln if this is really a get
        if len(response.split(self.eoln)) != 2:
            return InstErrorCode.HARDWARE_ERROR
        
        name_set = self._update_param_dict(response)
        if (name_set):
            return self._get_param_dict(name_set)
        else:
            return InstErrorCode.HARDWARE_ERROR
        
    def _parse_silent_response(self, response, prompt):
        """Parse a silent response
        
        @param response What was sent back from the command that was sent
        @param prompt The prompt that was returned from the device
        @retval return An InstErrorCode value
        """
        mi_logger.debug("Parsing silent response of [%s] with prompt [%s]",
                        response, prompt)
        if (response == "") and \
           ((prompt == Prompt.NULL) or (prompt == Prompt.COMMAND)):
            return InstErrorCode.OK
        else:
            return InstErrorCode.HARDWARE_ERROR
        
    ###################################################################
    # Helpers
    ###################################################################
    def _wakeup(self, timeout):
        """There is no wakeup sequence for this instrument"""
        pass
    
    def _update_params(self, *args, **kwargs):
        """Fetch the parameters from the device, and update the param dict.
        
        @param args Unused
        @param kwargs Takes timeout value
        @throws InstrumentProtocolException
        @throws InstrumentTimeoutException
        """
        timeout = kwargs.get('timeout', 10)
        old_config = self._get_config_param_dict()
        self.get_config()
        new_config = self._get_config_param_dict()            
        if new_config != old_config:
            self.announce_to_driver(DriverAnnouncement.CONFIG_CHANGE,
                                    msg="Device configuration changed")            
                
    def _send_break(self, break_char, timeout=30):
        """Break out of autosample mode.
        
        Issue the proper sequence of stuff to get the device out of autosample
        mode. The character used will result in a different end state. Ctrl-S
        goes to poll mode, Ctrl-C goes to command mode. Ctrl-R resets. 
        @param break_char The character to send to get out of autosample.
        Should be Event.STOP, Event.BREAK, or Event.RESET.
        @retval return True for success, Error for failure
        @throw InstrumentTimeoutException
        @throw InstrumentProtocolException
        @todo handle errors correctly here, deal with repeats at high sample rate
        """
        if not ((break_char == Command.BREAK)
            or (break_char == Command.STOP)
            or (break_char == Command.RESET)):
            return False

        mi_logger.debug("Sending break char %s", break_char)        
        # do the magic sequence of sending lots of characters really fast
        starttime = time.time()
        while True:
            #self._do_cmd_no_resp(break_char, None)
            #(prompt, result) = self._get_response(timeout)
            result_code = self._do_cmd_resp(break_char)
            mi_logger.debug("Got result code %s when trying to break",
                            result_code)
            if (result_code == InstErrorCode.OK):                
                return True
            elif InstErrorCode.has(result_code):
                return False
            else:
                if time.time() > starttime + timeout:
                    raise InstrumentTimeoutException(InstErrorCode.TIMEOUT)
    
        # Catch all    
        return False
    
    def _got_data(self, data):
        """ The comms object fires this when data is received
        
        @param data The chunk of data that was received
        """
        mi_logger.debug("*** Data received: %s", data)
        CommandResponseInstrumentProtocol._got_data(self, data)
            
        # If we are streaming, process the line buffer for samples.
        if self._fsm.get_current_state() == State.AUTOSAMPLE_MODE:
            if self.eoln in self._linebuf:
                lines = self._linebuf.split(self.eoln)
                self._linebuf = lines[-1]
                for line in lines:
                    self.announce_to_driver(DriverAnnouncement.DATA_RECEIVED,
                                            msg=line)
        else:
            # yank out the command we sent, split at the self.eoln
            self._linebuf = self._linebuf.split(self.eoln, 1)[1]
            if self._linebuf.endswith(Prompt.COMMAND):
                self._linebuf = self._linebuf[:-1]            
        

class SatlanticPARInstrumentDriver(InstrumentDriver):
    """
    The InstrumentDriver class for the Satlantic PAR sensor PARAD.
    @note If using this via Ethernet, must use a SLOW Ethernet connection
    or commands may not make it to the PAR successfully. A delay of 0.1
    appears to be sufficient for 19200 operations, maybe more for 9600
    """

    def __init__(self, evt_callback):
        """Instrument-specific enums
        @param evt_callback The callback function to use for events
        """
        InstrumentDriver.__init__(self, evt_callback)
        self.instrument_commands = Command
        self.instrument_parameters = Parameter
        self.instrument_channels = Channel
        self.instrument_errors = Error
        self.instrument_states = State
        self.instrument_active_states = [State.COMMAND_MODE,
                                         State.AUTOSAMPLE_MODE,
                                         State.POLL_MODE]
        self.protocol = SatlanticPARInstrumentProtocol(self.protocol_callback)
        
        # A few mappings from protocol to driver
        self.chan_map = {DriverChannel.INSTRUMENT:self.protocol,
                         Channel.PAR:self.protocol}
        self.state_map = {State.AUTOSAMPLE_MODE:DriverState.AUTOSAMPLE,
                          State.COMMAND_MODE:DriverState.COMMAND,
                          State.POLL_MODE:DriverState.ACQUIRE_SAMPLE,
                          State.UNKNOWN:DriverState.UNCONFIGURED}
        
    def execute_acquire_sample(self, channels, *args, **kwargs):
        self.protocol.execute_sample(args, kwargs)
        
    def execute_start_autosample(self, channels, *args, **kwargs):
        self.protocol.execute_start_autosample(args, kwargs)
        
    def execute_stop_autosample(self, channels, *args, **kwargs):
        self.protocol.execute_stop_autosample(args, kwargs)
        
class SatlanticChecksumDecorator(ChecksumDecorator):
    """Checks the data checksum for the Satlantic PAR sensor"""
    
    def handle_incoming_data(self, original_data=None, chained_data=None):    
        if (self._checksum_ok(original_data)):          
            if self.next_decorator == None:
                return (original_data, chained_data)
            else:
                self.next_decorator.handle_incoming_data(original_data, chained_data)
        else:
            raise InstrumentDataException(InstErrorCode.HARDWARE_ERROR,
                                          "Checksum failure!")
            
    def _checksum_ok(self, data):
        """Confirm that the checksum is valid for the data line
        
        @param data The entire line of data, including the checksum
        @retval True if the checksum fits, False if the checksum is bad
        """
        assert (data != None)
        assert (data != "")
        match = sample_regex.match(data)
        if not match:
            return False
        try:
            received_checksum = int(match.group('checksum'))
            line_end = match.start('checksum')-1        
        except IndexError:
            # Didnt have a checksum!
            return False
        
        line = data[:line_end]        
        # Calculate checksum on line
        checksum = 0
        for char in line:
            checksum += ord(char)
        checksum = checksum & 0xFF
        
        return (checksum == received_checksum)
        
