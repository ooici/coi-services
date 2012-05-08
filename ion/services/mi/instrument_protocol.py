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

from ion.services.mi.protocol_param_dict import ProtocolParameterDict
from ion.services.mi.exceptions import TimeoutError
from ion.services.mi.exceptions import ProtocolError

mi_logger = logging.getLogger('mi_logger')

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
        
    ########################################################################
    # Command build and response parse handlers.
    ########################################################################            
                   
    def _add_build_handler(self, cmd, func):
        """
        Add a command building function.
        @param cmd The device command to build.
        @param func The function that constructs the command.
        """
        self._build_handlers[cmd] = func
        
    def _add_response_handler(self, cmd, func):
        """
        Add a response parsing function.
        @param The device command to build.
        @func The function that parses the response.
        """
        self._response_handlers[cmd] = func
                   
    def _get_response(self, timeout):
        """
        Wait for and retrieve a command response.
        @param timeout the timeout to wait for a response.
        @retval (prompt, repsponse) tuple.
        @raises TimeoutError if the reponse did not occur in time.
        """
        # Grab time for timeout and wait for prompt.
        starttime = time.time()
        
        while True:
            
            for item in self._prompts.list():
                if self._promptbuf.endswith(item):
                    return (item, self._linebuf)
                else:
                    time.sleep(.1)
            if time.time() > starttime + timeout:
                raise TimeoutError()
               
    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        Perform a command-response on the device.
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional wakeup and command timeout.
        @retval resp_result The (possibly parsed) response result.
        @raises TimeoutError if the reponse did not occur in time.
        @raises ProtocolError if command could not be built or if response
        was not recognized.
        """
        
        # Get timeout and initialize response.
        timeout = kwargs.get('timeout', 10)
        resp_result = None
        
        # Get the build handler.
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise ProtocolError('Cannot build command: %s' % cmd)
        
        cmd_line = build_handler(cmd, *args)
        
        # Wakeup the device, pass up exeception if timeout
        prompt = self._wakeup(timeout)
                    
        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        mi_logger.debug('_do_cmd_resp: %s', repr(cmd_line))
        self._connection.send(cmd_line)

        # Wait for the prompt, prepare result and return, timeout exception
        (prompt, result) = self._get_response(timeout)
                
        resp_handler = self._response_handlers.get(cmd, None)
        if resp_handler:
            resp_result = resp_handler(result, prompt)
        else:
            resp_result = result

        return resp_result
            
    def _do_cmd_no_resp(self, cmd, *args, **kwargs):
        """
        Perform a command without response on the device.
        @param cmd The command to execute.
        @param args positional arguments to pass to the build handler.
        @param timeout=timeout optional wakeup timeout.
        @raises TimeoutError if the reponse did not occur in time.
        @raises ProtocolError if command could not be built.        
        """
        timeout = kwargs.get('timeout', 10)        
        
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise ProtocolError('Cannot build command: %s' % cmd)
        
        cmd_line = build_handler(cmd, *args)
        
        # Wakeup the device, timeout exception as needed
        prompt = self._wakeup(timeout)
       
        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        mi_logger.debug('_do_cmd_no_resp: %s', repr(cmd_line))
        self._connection.send(cmd_line)        
    
    def _do_cmd_direct(self, cmd):
        """
        Issue an untranslated command to the instrument. No response is handled 
        as a result of the command.
        
        @param cmd The high level command to issue
        """

        # Send command.
        mi_logger.debug('_do_cmd_direct: <%s>', cmd)
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
        @throw TimeoutError if the device could not be woken.
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
                raise TimeoutError()

    def _wakeup_until(self, timeout, desired_prompt, delay=1, no_tries=5):
        """
        Continue waking device until a specific prompt appears or a number
        of tries has occurred.
        @param timeout The timeout to wake the device.
        @desired_prompt Continue waking until this prompt is seen.
        @delay Time to wake between consecutive wakeups.
        @no_tries Maximum number of wakeup tries to see desired prompt.
        @raises TimeoutError if device could not be woken.
        @raises ProtocolError if the deisred prompt is not seen in the
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
                    raise ProtocolError('Incorrect prompt.')
