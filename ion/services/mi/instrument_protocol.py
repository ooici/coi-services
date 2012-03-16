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

from ion.services.mi.common import InstErrorCode, EventKey
from ion.services.mi.protocol_param_dict import ProtocolParameterDict
from ion.services.mi.exceptions import InstrumentTimeoutError
from ion.services.mi.exceptions import InstrumentProtocolError

mi_logger = logging.getLogger('mi_logger')

class InstrumentProtocol(object):
    """
    Base instrument protocol class.
    """
    
    def __init__(self, driver_event):
        """
        @param evt_callback A callback to send protocol events to the
        agent.
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
       """
       pass

    def announce_to_driver(self, type, error_code=None, msg=None):
        """
        Packag and send events.
        """
        pass
    
    def get_current_state(self):
        """
        """
        return self._protocol_fsm.get_current_state()
    
class BinaryInstrumentProtocol(InstrumentProtocol):
    """Instrument protocol description for a binary-based instrument
    
    This class wraps standard protocol operations with methods to pack
    commands into the binary structures that they need to be in for the
    instrument to operate on them.
    @todo Consider removing this class if insufficient parameterization of
    message packing is done
    """
    
    def _pack_msg(self, msg=None):
        """Pack the message according to the field before sending it to the
        instrument.
        
        This may involve special packing per parameter, possibly checksumming
        across the whole message, too.
        @param msg The message to pack for the instrument. May need to be
        inspected to determine the proper packing structure
        @retval packed_msg The packed message
        """
        # Default implementation
        return msg.checksum
    
    def _unpack_response(self, type=None, packed_msg=None):
        """Unpack a message from an instrument
        
        When a binary instrument responsed with a packed binary, this routine
        unbundles the response into something usable. Checksums may be added
        @param type The type of message to be unpacked. Will like be a key to
        an unpacking description string.
        @param packed_msg The packed message needing to be unpacked
        @retval msg The unpacked message
        """
        # Default implementation
        return packed_msg
    

class ScriptInstrumentProtocol(InstrumentProtocol):
    """A class to handle a simple scripted interaction with an instrument
    
    For instruments with a predictable interface (such as a menu or well
    known paths through options), this class can be setup to follow a simple
    script of interactions to manipulate the instrument. The script language
    is currently as follows:
    
    * Commands are in name value pairs separated by an =, no whitespace
        around the equals
    * Commands are separated from each other by \n.
    * Control keys are the letter preceeded by a ^ symbol.
    * Use \ to protect a ^ or \n and include it in the string being sent.
    * A final \n is optional.
    * Commands will be executed in order
    * The send command name is "S"
    * The delay command is "D", delay measured in seconds, but is a
        python formatted floating point value
    
    For example, the following script issues a Control-C, "3", "1", "val1\n", then a "0"
    with a 1.5 second delay between to do something like walk through a menu,
    select a parameter, name its value, then return to the previous menu:
    "S=^C\nD=1.5\nS=3\nD=1.5\nS=1\nD=1.5\nS=val1\\nD=1.5\nS=0"

    @todo Add a wait-for command?
    """
    
    def run_script(self, script):
        """Interpret the supplied script and apply it to the instrument
        
        @param script The script to execute in string form
        @throws InstrumentProtocolException Confusion dealing with the
        physical device, possibly due to interrupted communications
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        """
        
    
class CommandResponseInstrumentProtocol(InstrumentProtocol):
    """
    Base class for text-based command-response instruments.
    """
    def __init__(self, prompts, newline, driver_event):
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
        """
        self._build_handlers[cmd] = func
        
    def _add_response_handler(self, cmd, func):
        """
        """
        self._response_handlers[cmd] = func
                   
    def _get_response(self, timeout):
        """
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
                raise InstrumentTimeoutError()
               
    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        """
        timeout = kwargs.get('timeout', 10)
        resp_result = None
        
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolError()
        
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
        mi_logger.info('getting response')
        (prompt, result) = self._get_response(timeout)
        mi_logger.info('got response: %s', repr(result))
                
                
        resp_handler = self._response_handlers.get(cmd, None)
        if resp_handler:
            resp_result = resp_handler(result, prompt)
        else:
            mi_logger.info('No response handler.')

        return resp_result
            
    def _do_cmd_no_resp(self, cmd, *args, **kwargs):
        """
        """
        timeout = kwargs.get('timeout', 10)        
        
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolException(InstErrorCode.BAD_DRIVER_COMMAND)
        
        cmd_line = build_handler(cmd, *args)
        
        # Wakeup the device, timeout exception as needed
        prompt = self._wakeup(timeout)
       
        # Clear line and prompt buffers for result.
        self._linebuf = ''

        # Send command.
        mi_logger.debug('_do_cmd_no_resp: %s', repr(cmd_line))
        self._logger_client.send(cmd_line)        

        return InstErrorCode.OK
    
    ########################################################################
    # Incomming data callback.
    ########################################################################            

                
    def got_data(self, data):
        """
        """
        # Update the line and prompt buffers.
        self._linebuf += data        
        self._promptbuf += data

    ########################################################################
    # Wakeup helpers.
    ########################################################################            
    
    def _send_wakeup(self):
        """
        Use the logger to send what needs to be sent to wake up the device.
        This is intended to be overridden if there is any wake up needed.
        """
        pass
        
    def  _wakeup(self, timeout):
        """
        Clear buffers and send a wakeup command to the instrument
        @todo Consider the case where there is no prompt returned when the
        instrument is awake.
        @param timeout The timeout in seconds
        @throw InstrumentProtocolExecption on timeout
        """
        # Clear the prompt buffer.
        self._promptbuf = ''
        
        # Grab time for timeout.
        starttime = time.time()
        
        while True:
            # Send a line return and wait a sec.
            mi_logger.debug('Sending wakeup.')
            self._send_wakeup()
            time.sleep(1)
            
            for item in self._prompts.list():
                if self._promptbuf.endswith(item):
                    mi_logger.debug('wakeup got prompt: %s', repr(item))
                    return item

            if time.time() > starttime + timeout:
                raise InstrumentTimeoutError()

