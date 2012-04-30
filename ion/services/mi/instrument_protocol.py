#!/usr/bin/env python

"""
@package ion.services.mi.instrument_protocol Base instrument protocol structure
@file ion/services/mi/instrument_protocol.py
@author Steve Foley
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

from ion.services.mi.common import BaseEnum
from ion.services.mi.protocol_param_dict import ProtocolParameterDict
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentProtocolException

mi_logger = logging.getLogger('mi_logger')

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
    
class CommandResponseInstrumentProtocol(InstrumentProtocol):
    """
    Base class for text-based command-response instruments.
    """
    
    def __init__(self, prompts, newline, driver_event):
        """
        Constructor.
        @param prompts Enum class containing possbile device prompts used for
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

    ########################################################################
    # Command build and response parse handlers.
    ########################################################################            
    def _add_response_handler(self, cmd, func, state=None):
        """
        Insert a handler class responsible for handling the response to a
        command sent to the instrument, optionally available only in a
        specific state.
        
        @param cmd The high level key of the command to responsd to.
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
            prompt_list = self.prompts.list()
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
        @raises InstrumentTimeoutException if the reponse did not occur in time.
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
        
        # Wakeup the device, pass up exeception if timeout
        prompt = self._wakeup(timeout)
                    
        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        mi_logger.debug('_do_cmd_resp: %s, timeout=%s, write_delay=%s,',
                        repr(cmd_line), timeout, write_delay)
        if (write_delay == 0):
            self._connection.send(cmd_line)
        else:
            for char in cmd_line:
                self._connection.send(char)
                time.sleep(write_delay)

        # Wait for the prompt, prepare result and return, timeout exception
        (prompt, result) = self._get_response(timeout,
                                              expected_prompt=expected_prompt)
        resp_handler = self._response_handlers.get(cmd, None)
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
        @raises InstrumentTimeoutException if the reponse did not occur in time.
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
        mi_logger.debug('_do_cmd_no_resp: %s, timeout=%s', repr(cmd_line), timeout)
        if (write_delay == 0):
            self._connection.send(cmd_line)
        else:
            for char in cmd_line:
                self._connection.send(char)
                time.sleep(write_delay)
    
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
        self._last_data_received_timestamp = time.time()

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
            mi_logger.debug('Sending wakeup.')
            self._send_wakeup()
            time.sleep(delay)
            
            for item in self._prompts.list():
                if self._promptbuf.endswith(item):
                    mi_logger.debug('wakeup got prompt: %s', repr(item))
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
        @raises InstrumentProtocolException if the deisred prompt is not seen in the
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
