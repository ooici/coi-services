#!/usr/bin/env python

"""
@package ion.services.mi.instrument_protocol Base instrument protocol structure
@file ion/services/mi/instrument_protocol.py
@author Steve Foley, 
        Bill Bollenbacher
@brief Instrument protocol classes that provide structure towards the
nitty-gritty interaction with individual instruments in the system.
@todo Figure out what gets thrown on errors
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

import logging
import time
import os
import signal
import re

from mi.core.common import BaseEnum, InstErrorCode
from mi.core.instrument.protocol_param_dict import ProtocolParameterDict
from mi.core.exceptions import InstrumentTimeoutException
from mi.core.exceptions import InstrumentProtocolException
from mi.core.exceptions import InstrumentParameterException
from mi.core.exceptions import NotImplementedException 

from mi.core.log import get_logger ; log = get_logger()

class InterfaceType(BaseEnum):
    """The methods of connecting to a device"""
    ETHERNET = 'ethernet'
    SERIAL = 'serial'

class InstrumentProtocol(object):
    """
        
    Base instrument protocol class.
    """    
    def __init__(self, driver_event):
        """
        Base constructor.
        @param driver_event The callback for asynchronous driver events.
        """
        # Event callback to send asynchronous events to the agent.
        self._driver_event = driver_event

        # The connection used to talk to the device.
        self._connection = None
        
        # The protocol state machine.
        self._protocol_fsm = None
        
        # The parameter dictionary.
        self._param_dict = ProtocolParameterDict()

    ########################################################################
    # Helper methods
    ########################################################################
    def got_data(self, data):
        """
        Called by the instrument connection when data is available.
         Defined in subclasses.
        """
        pass
    
    def get_current_state(self):
        """
        Return current state of the protocol FSM.
        """
        return self._protocol_fsm.get_current_state()

    def get_resource_capabilities(self, current_state=True):
        """
        """
        
        res_cmds = self._protocol_fsm.get_events(current_state)
        res_cmds = self._filter_capabilities(res_cmds)        
        res_params = self._param_dict.get_keys()
        
        return [res_cmds, res_params]

    def _filter_capabilities(self, events):
        """
        """
        return events

    ########################################################################
    # Command build and response parse handlers.
    ########################################################################            
    def _add_response_handler(self, cmd, func, state=None):
        """
        Insert a handler class responsible for handling the response to a
        command sent to the instrument, optionally available only in a
        specific state.
        
        @param cmd The high level key of the command to respond to.
        @param func The function that handles the response
        @param state The state to pair with the command for which the function
        should be used
        """
        if state == None:
            self._response_handlers[cmd] = func
        else:            
            self._response_handlers[(state, cmd)] = func

    def _add_build_handler(self, cmd, func):
        """
        Add a command building function.
        @param cmd The device command to build.
        @param func The function that constructs the command.
        """
        self._build_handlers[cmd] = func
        
    ########################################################################
    # Helpers to build commands.
    ########################################################################
    def _build_simple_command(self, cmd, *args):
        """
        Builder for simple commands

        @param cmd The command to build
        @param args Unused arguments
        @retval Returns string ready for sending to instrument        
        """
        return "%s%s" % (cmd, self.eoln)
    
    def _build_keypress_command(self, cmd, *args):
        """
        Builder for simple, non-EOLN-terminated commands

        @param cmd The command to build
        @param args Unused arguments
        @retval Returns string ready for sending to instrument        
        """
        return "%s" % (cmd)
    
    def _build_multi_keypress_command(self, cmd, *args):
        """
        Builder for simple, non-EOLN-terminated commands

        @param cmd The command to build
        @param args Unused arguments
        @retval Returns string ready for sending to instrument        
        """
        return "%s%s%s%s%s%s" % (cmd, cmd, cmd, cmd, cmd, cmd)

    ########################################################################
    # Static helpers to format set commands.
    ########################################################################

    @staticmethod
    def _true_false_to_string(v):
        """
        Write a boolean value to string formatted for "generic" set operations.
        Subclasses should overload this as needed for instrument-specific
        formatting.
        
        @param v a boolean value.
        @retval A yes/no string formatted as a Python boolean for set operations.
        @throws InstrumentParameterException if value not a bool.
        """
        
        if not isinstance(v,bool):
            raise InstrumentParameterException('Value %s is not a bool.' % str(v))
        return str(v)

    @staticmethod
    def _int_to_string(v):
        """
        Write an int value to string formatted for "generic" set operations.
        Subclasses should overload this as needed for instrument-specific
        formatting.
        
        @param v An int val.
        @retval an int string formatted for generic set operations.
        @throws InstrumentParameterException if value not an int.
        """
        
        if not isinstance(v,int):
            raise InstrumentParameterException('Value %s is not an int.' % str(v))
        else:
            return '%i' % v

    @staticmethod
    def _float_to_string(v):
        """
        Write a float value to string formatted for "generic" set operations.
        Subclasses should overload this as needed for instrument-specific
        formatting.
        
        @param v A float val.
        @retval a float string formatted for "generic" set operations.
        @throws InstrumentParameterException if value is not a float.
        """

        if not isinstance(v,float):
            raise InstrumentParameterException('Value %s is not a float.' % v)
        else:
            return '%e' % v

class CommandResponseInstrumentProtocol(InstrumentProtocol):
    """
    Base class for text-based command-response instruments.
    """
    
    def __init__(self, prompts, newline, driver_event):
        """
        Constructor.
        @param prompts Enum class containing possible device prompts used for
        command response logic.
        @param newline The device newline.
        @driver_event The callback for asynchronous driver events.
        """
        
        # Construct superclass.
        InstrumentProtocol.__init__(self, driver_event)

        # The end of line delimiter.                
        self._newline = newline
    
        # Class of prompts used by device.
        self._prompts = prompts
    
        # Line buffer for input from device.
        self._linebuf = ''
        
        # Short buffer to look for prompts from device in command-response
        # mode.
        self._promptbuf = ''
        
        # Lines of data awaiting further processing.
        self._datalines = []

        # Handlers to build commands.
        self._build_handlers = {}

        # Handlers to parse responses.
        self._response_handlers = {}

        self._last_data_receive_timestamp = None
        
    def _get_response(self, timeout=10, expected_prompt=None):
        """
        Get a response from the instrument
        @todo Consider cases with no prompt
        @param timeout The timeout in seconds
        @param expected_prompt Only consider the specific expected prompt as
        presented by this string
        @throw InstrumentProtocolExecption on timeout
        """
        # Grab time for timeout and wait for prompt.
        starttime = time.time()
                
        if expected_prompt == None:
            prompt_list = self._prompts.list()
        else:
            assert isinstance(expected_prompt, str)
            prompt_list = [expected_prompt]            
        while True:
            for item in prompt_list:
                if self._promptbuf.endswith(item):
                    return (item, self._linebuf)
                else:
                    time.sleep(.1)
            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException()
               
    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        Perform a command-response on the device.
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional wakeup and command timeout.
        @retval resp_result The (possibly parsed) response result.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """
        
        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', 10)
        expected_prompt = kwargs.get('expected_prompt', None)
        write_delay = kwargs.get('write_delay', 0)
        retval = None
        
        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolException('Cannot build command: %s' % cmd)
        
        cmd_line = build_handler(cmd, *args)
        
        # Wakeup the device, pass up exception if timeout
        prompt = self._wakeup(timeout)
                    
        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        log.debug('_do_cmd_resp: %s, timeout=%s, write_delay=%s, expected_prompt=%s,' %
                        (repr(cmd_line), timeout, write_delay, expected_prompt))
        if (write_delay == 0):
            self._connection.send(cmd_line)
        else:
            for char in cmd_line:
                self._connection.send(char)
                time.sleep(write_delay)

        # Wait for the prompt, prepare result and return, timeout exception
        (prompt, result) = self._get_response(timeout,
                                              expected_prompt=expected_prompt)
        resp_handler = self._response_handlers.get((self.get_current_state(), cmd), None) or \
            self._response_handlers.get(cmd, None)
        resp_result = None
        if resp_handler:
            resp_result = resp_handler(result, prompt)
        
        return resp_result
            
    def _do_cmd_no_resp(self, cmd, *args, **kwargs):
        """
        Issue a command to the instrument after a wake up and clearing of
        buffers. No response is handled as a result of the command.
        
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional wakeup timeout.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built.        
        """
        timeout = kwargs.get('timeout', 10)
        write_delay = kwargs.get('write_delay', 0)

        
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolException(error_code=InstErrorCode.BAD_DRIVER_COMMAND)
        cmd_line = build_handler(cmd, *args)
        
        # Wakeup the device, timeout exception as needed
        prompt = self._wakeup(timeout)

        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        log.debug('_do_cmd_no_resp: %s, timeout=%s' % (repr(cmd_line), timeout))
        if (write_delay == 0):
            self._connection.send(cmd_line)
        else:
            for char in cmd_line:
                self._connection.send(char)
                time.sleep(write_delay)
    
    def _do_cmd_direct(self, cmd):
        """
        Issue an untranslated command to the instrument. No response is handled 
        as a result of the command.
        
        @param cmd The high level command to issue
        """

        # Send command.
        log.debug('_do_cmd_direct: <%s>' % cmd)
        self._connection.send(cmd)
 
    ########################################################################
    # Incomming data callback.
    ########################################################################            
    def got_data(self, data):
        """
       Called by the instrument connection when data is available.
       Append line and prompt buffers. Extended by device specific
       subclasses.
        """
        # Update the line and prompt buffers.
        self._linebuf += data        
        self._promptbuf += data
        self._last_data_timestamp = time.time()

    ########################################################################
    # Wakeup helpers.
    ########################################################################            
    
    def _send_wakeup(self):
        """
        Send a wakeup to the device. Overridden by device specific
        subclasses.
        """
        pass
        
    def  _wakeup(self, timeout, delay=1):
        """
        Clear buffers and send a wakeup command to the instrument
        @param timeout The timeout to wake the device.
        @param delay The time to wait between consecutive wakeups.
        @throw InstrumentTimeoutException if the device could not be woken.
        """
        # Clear the prompt buffer.
        self._promptbuf = ''
        
        # Grab time for timeout.
        starttime = time.time()
        
        while True:
            # Send a line return and wait a sec.
            log.debug('Sending wakeup.')
            self._send_wakeup()
            time.sleep(delay)
            
            for item in self._prompts.list():
                if self._promptbuf.endswith(item):
                    log.debug('wakeup got prompt: %s' % repr(item))
                    return item

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException()

    def _wakeup_until(self, timeout, desired_prompt, delay=1, no_tries=5):
        """
        Continue waking device until a specific prompt appears or a number
        of tries has occurred.
        @param timeout The timeout to wake the device.
        @desired_prompt Continue waking until this prompt is seen.
        @delay Time to wake between consecutive wakeups.
        @no_tries Maximum number of wakeup tries to see desired prompt.
        @raises InstrumentTimeoutException if device could not be woken.
        @raises InstrumentProtocolException if the desired prompt is not seen in the
        maximum number of attempts.
        """
        count = 0
        while True:
            prompt = self._wakeup(timeout, delay)
            if prompt == desired_prompt:
                break
            else:
                time.sleep(delay)
                count += 1
                if count >= no_tries:
                    raise InstrumentProtocolException('Incorrect prompt.')
                    
                    
class MenuInstrumentProtocol(CommandResponseInstrumentProtocol):
    """
    Base class for menu-based instrument interfaces that can use a cmd/response approach to
    walking down the menu from its root.
    """
    
    class MenuTree(object):
        # The _node_directions variable is a dictionary of menu sub-menus keyed by the sub-menu's name.
        # Each sub-menu entry contains a list of directions, which are either cmd/response pairs or 
        # sub_menu names. These commands need to be executed in the specified order to get from the root menu
        # to the sub-menu.
        # example:
        #
        # for these enumerations:
        #
        # class SubMenues(BaseEnum):
        #     SUB_MENU1 = 'sub_menu1'
        #     SUB_MENU2 = 'sub_menu2'
        #     SUB_MENU3 = 'sub_menu3'
        #     SUB_MENU4 = 'sub_menu4'
        #
        # class InstrumentPrompts(BaseEnum):
        #     MAIN_MENU = '\a\b ? \a\b'
        #     SUB_MENU1  = '\a\b 1'
        #     SUB_MENU2  = '\a\b 2'
        #     SUB_MENU3  = '\a\b 3'
        #     SUB_MENU4  = '\a\b 4'
        #
        # the instance creation could look like:
        #
        # Directions = MenuInstrumentProtocol.MenuTree.Directions
        #
        # menu = MenuInstrumentProtocol.MenuTree({
        #    SubMenues.SUB_MENU1   : [Directions("1", InstrumentPrompts.SUB_MENU1)],
        #    SubMenues.SUB_MENU2   : [Directions("2", InstrumentPrompts.SUB_MENU2)],
        #    SubMenues.SUB_MENU3   : [Directions(SubMenues.SUB_MENU2),
        #                            Directions("2", InstrumentPrompts.SUB_MENU3, 20)],
        #    SubMenues.SUB_MENU4   : [Directions(SubMenues.SUB_MENU3),
        #                            Directions("d", InstrumentPrompts.SUB_MENU4)]
        #    })
        #
        # After passing the menu into the constructor via:
        # MenuInstrumentProtocol.__init__(self, menu, prompts, newline, driver_event)
        #
        # directions can be retrieved for a sub-menu using:
        #
        # directions_list = self._menu.get_directions(SubMenues.SUB_MENU4)
        #
        # which should return a list of Directions objects which can be used to walk from
        # the root menu to the sub-menu as follows:
        #
        # for directions in directions_list:
        #     command = directions.get_command()
        #     response = directions.get_response()
        #     timeout = directions.get_timeout()
        #     do_cmd_reponse(command, expected_prompt = response, timeout = timeout)
        

        class Directions(object):
            def __init__(self, command = None, response = None, timeout = 10):
                if command == None:
                    raise InstrumentProtocolException('MenuTree.Directions(): command parameter missing')                
                self.command = command
                self.response = response
                self.timeout = timeout
                
            def __str__(self):
                return "command=%s, response=%s, timeout=%s" %(repr(self.command), 
                                                               repr(self.response), 
                                                               repr(self.timeout))
            
            def get_command(self):
                return self.command
            
            def get_response(self):
                return self.response
                
            def get_timeout(self):
                return self.timeout
                
        _node_directions = {}
        
        def __init__(self, node_directions):
            if not isinstance(node_directions, dict):
                raise InstrumentProtocolException('MenuTree.__init__(): node_directions parameter not a dictionary')                
            self._node_directions = node_directions
            
        def get_directions(self, node):
            try:
                directions_list = self._node_directions[node]
            except:
                raise InstrumentProtocolException('MenuTree.get_directions(): node %s not in _node_directions dictionary'
                                                  %str(node))                
            log.debug("MenuTree.get_directions(): _node_directions = %s, node = %s, d_list = %s" 
                      %(str(self._node_directions), str(node), str(directions_list)))
            directions = []
            for item in directions_list:
                if not isinstance(item, self.Directions):
                    raise InstrumentProtocolException('MenuTree.get_directions(): item %s in directions list not a Directions object'
                                                      %str(item))                
                if item.response != None:
                    directions.append(item)
                else:
                    directions += self.get_directions(item.command)
            return directions
        
           
    def __init__(self, menu, prompts, newline, driver_event):
        """
        Constructor.
        @param prompts Enum class containing possible device prompts used for
        menu system.
        @param newline The device newline.
        @driver_event The callback for asynchronous driver events.
        """
        
        # Construct superclass.
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)
        self._menu = menu

        # The end of line delimiter.                
        self._newline = newline
    
        # Class of prompts used by device.
        self._prompts = prompts
    
        # Linebuffer for input from device.
        self._linebuf = ''
        
        # Short buffer to look for prompts from device in command-response
        # mode.
        self._promptbuf = ''
        
        # Lines of data awaiting further processing.
        self._datalines = []

        # Handlers to build commands.
        self._build_handlers = {}

        # Handlers to parse responses.
        self._response_handlers = {}

        self._last_data_receive_timestamp = None
        
    # DHE Added
    def _get_response(self, timeout=10, expected_prompt=None):
        """
        Get a response from the instrument
        @todo Consider cases with no prompt
        @param timeout The timeout in seconds
        @param expected_prompt Only consider the specific expected prompt as
        presented by this string
        @throw InstrumentProtocolExecption on timeout
        """
        log.debug('MenuInstrumentProtocol._get_response: timeout=%s, expected_prompt=%s, expected_prompt(hex)=%s,' 
                  %(timeout, expected_prompt, expected_prompt.encode("hex")))
        # Grab time for timeout and wait for prompt.
        starttime = time.time()
                
        if expected_prompt == None:
            prompt_list = self._prompts.list()
        else:
            assert isinstance(expected_prompt, str)
            prompt_list = [expected_prompt]            
        while True:
            for item in prompt_list:
                # DHE: this doesn't work well; changing for now.
                #if self._promptbuf.endswith(item):
                #print "---> DHE: get_response looking for item: " + str(item) + " in promptbuf: " + str(self._promptbuf)
                if item in self._promptbuf:
                    #print "---> get_response DHE: FOUND IT!"
                    return (item, self._linebuf)
                else:
                    time.sleep(.1)
            if time.time() > starttime + timeout:
                #print "------->> get_response DHE TIMEOUT!!!!"
                raise InstrumentTimeoutException()
               
    # DHE Added
    def _navigate_and_execute(self, cmd, **kwargs):
        """
        Navigate to a sub-menu and execute a command.  
        @param cmd The command to execute.
        @param timeout=timeout optional wakeup and command timeout.
        @retval resp_result The (possibly parsed) response result.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """
        
        resp_result = None

        # Get dest_submenu arg
        dest_submenu = kwargs.get('dest_submenu', None)
        if dest_submenu == None:
            raise InstrumentProtocolException('_navigate_and_execute(): dest_submenu parameter missing')

        # iterate through the directions 
        directions_list = self._menu.get_directions(dest_submenu)
        for directions in directions_list:
            #print "--------> DHE: nav_and_ex: directions: " + str(directions)
            command = directions.get_command()
            response = directions.get_response()
            timeout = directions.get_timeout()
            self._do_cmd_resp(command, expected_prompt = response, timeout = timeout)

        # Get timeout and initialize response.
        expected_timeout = kwargs.get('timeout', 10)
        expected_prompt = kwargs.get('expected_prompt', None)
        write_delay = kwargs.get('write_delay', 0)
        retval = None
        
        value = kwargs.get('value', None)
        #
        # DHE: this is a kludge; need a way to send a parameter as a "command."  We can't expect to look
        # up all possible values in the build_handlers
        #
        if cmd is None:
            cmd_line = self._build_simple_command(value) 
            #print "-----> DHE: sending value: " + cmd_line + " to connection.send()"
            self._connection.send(cmd_line)
        else:
            #print "-----> DHE: sending command: " + str(cmd) + " + value: " + str(value) + " to do_cmd_resp()"
            resp_result = self._do_cmd_resp(cmd, value = value, expected_prompt = expected_prompt, timeout = expected_timeout)
 
        return resp_result

    def _do_cmd_resp(self, cmd, **kwargs):
        """
        Perform a command-response on the device.
        @param cmd The command to execute.
        @param timeout=timeout optional wakeup and command timeout.
        @retval resp_result The (possibly parsed) response result.
        @raises InstrumentTimeoutException if the response did not occur in time.
        @raises InstrumentProtocolException if command could not be built or if response
        was not recognized.
        """

        #print "-----> DHE: do_cmd_resp sending cmd: " + str(cmd[0])
        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', 10)
        expected_prompt = kwargs.get('expected_prompt', None)
        write_delay = kwargs.get('write_delay', 0)
        retval = None

        # Get the value
        value = kwargs.get('value', None)

        # Get the build handler.
        build_handler = self._build_handlers.get(cmd[0], None)
        if not build_handler:
            raise InstrumentProtocolException('Cannot build command: %s' % cmd[0])

        # DHE taking out args; if we need them we'll use kwargs
        #cmd_line = build_handler(cmd, *args)
        #
        # DHE: Should this really be like this: intent.format_input()?  This way the "command"
        # or "intent" object contains the build instructions?  This seems strange to me to 
        # do it this way.
        # (passing the actual command to this instead of the unique name, because the command
        # overlaps sometimes.
        cmd_line = build_handler(cmd[1])

        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        log.debug('_do_cmd_resp: %s, timeout=%s, write_delay=%s, expected_prompt=%s,' %
                        (repr(cmd_line), timeout, write_delay, expected_prompt))
        if (write_delay == 0):
            self._connection.send(cmd_line)
        else:
            #print "---> DHE: do_cmd_resp() sending cmd_line: " + cmd_line
            for char in cmd_line:
                self._connection.send(char)
                time.sleep(write_delay)

        # Wait for the prompt, prepare result and return, timeout exception
        (prompt, result) = self._get_response(timeout,
                                              expected_prompt=expected_prompt)

        # DHE TEMP
        #print "----->>> DHE: looking for response handler for: " + str(cmd[0])
        resp_handler = self._response_handlers.get((self.get_current_state(), cmd[0]), None) or \
            self._response_handlers.get(cmd[0], None)
        resp_result = None
        if resp_handler:
            #print "--->>> DHE:  calling response handler: " + str(resp_handler)
            resp_result = resp_handler(result, prompt)

        return resp_result
    
    def _go_to_root_menu(self):
        """
        This method needs to be implemented for each instrument.  It performs the commands that 
        returns the instrument to its root menu
        """
        raise NotImplementedException('_go_to_root_menu() not implemented.')

    def got_data(self, data):
        """
        Called by the instrument connection when data is available.
        Append line and prompt buffers. Extended by device specific
        subclasses.
        """
        self._linebuf += data        
        self._promptbuf += data
        self._last_data_timestamp = time.time()    

