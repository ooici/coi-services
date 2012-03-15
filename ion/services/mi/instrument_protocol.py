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
from ion.services.mi.logger_process import EthernetDeviceLogger, LoggerClient
from ion.services.mi.protocol_param_dict import ProtocolParameterDict

mi_logger = logging.getLogger('mi_logger')

class InstrumentProtocol(object):
    """
    Base instrument protocol class.
    """
    
    def __init__(self, driver_event, connection):
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
    def __init__(self, prompts, newline, driver_event, connection):
        InstrumentProtocol.__init__(self, driver_event, connection)

        # The end of line delimiter.                
        self.eoln = newline
    
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
    # Incomming data callback.
    ########################################################################            

    def _add_build_handler(self, cmd, func):
        """
        """
        self._build_handlers[cmd] = func
        
    def _add_response_handler(self, cmd, func):
        """
        """
        self._response_handlers[cmd] = func
                
    def got_data(self, data):
        """
        """
        # Update the line and prompt buffers.
        self._linebuf += data        
        self._promptbuf += data


