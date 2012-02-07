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

from zope.interface import Interface, implements
import logging
import time
import os
import signal


from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentStateException
from ion.services.mi.instrument_connection import IInstrumentConnection
from ion.services.mi.common import InstErrorCode
from ion.services.mi.logger_process import EthernetDeviceLogger, LoggerClient

mi_logger = logging.getLogger('mi_logger')

class InstrumentProtocol(object):
    """The base class for an instrument protocol
    
    The classes derived from this class will carry out the specific
    interactions between a specific device and the instrument driver. At this
    layer of interaction, there are no conflicts or transactions as that is
    handled at the layer above this. Think of this as encapsulating the
    transport layer of the communications.
    """
    
    implements(IInstrumentConnection)
    
    def __init__(self, callback=None):
        """Set instrument connect at creation
        
        @param connection An InstrumetnConnection object
        """
        self._logger = None
        self._logger_client = None
        self._logger_popen = None
        self._fsm = None
        
        self.announce_to_driver = callback
        """The driver callback where we an publish events. Should be a link
        to a function."""
        
    ########################################################################
    # Protocol connection interface.
    ########################################################################

    """
    @todo Move this into the driver state machine?
    """
    
    def initialize(self, timeout=10):
        """
        """
        mi_logger.info('Initializing device comms.')        
        self._logger = None
        self._logger_client = None
    
    def configure(self, config, timeout=10):
        """
        """
        mi_logger.info('Configuring for device comms.')        
        success = InstErrorCode.OK
        
        try:
            method = config['method']
            
            if method == 'ethernet':
                device_addr = config['device_addr']
                device_port = config['device_port']
                server_addr = config['server_addr']
                server_port = config['server_port']
                self._logger = EthernetDeviceLogger(device_addr, device_port,
                                                    server_port)
                self._logger_client = LoggerClient(server_addr, server_port)

            elif method == 'serial':
                pass
            
            else:
                success = InstErrorCode.INVALID_PARAMETER

        except KeyError:
            success = InstErrorCode.INVALID_PARAMETER

        return success

    
    def connect(self, timeout=10):
        """Connect via the instrument connection object
        
        @param args connection arguments
        @throws InstrumentConnectionException
        """
        mi_logger.info('Connecting to device.')
        logger_pid = self._logger.get_pid()
        mi_logger.info('Found logger pid: %s.', str(logger_pid))
        if not logger_pid:
            self._logger_popen = self._logger.launch_process()
            retval = os.wait()
            mi_logger.debug('os.wait returned %s', str(retval))
            mi_logger.debug('popen wait returned %s', str(self._logger_popen.wait()))
        time.sleep(1)         
        self.attach()

        success = InstErrorCode.OK
        return success
    
    def disconnect(self, timeout=10):
        """Disconnect via the instrument connection object
        
        @throws InstrumentConnectionException
        """
        mi_logger.info('Disconnecting from device.')
        self.detach()
        self._logger.stop()
    
    def attach(self, timeout=10):
        """
        """
        mi_logger.info('Attaching to device.')        
        self._logger_client.init_comms(self._got_data)
    
    def detach(self, timeout=10):
        """
        """
        mi_logger.info('Detaching from device.')
        self._logger_client.stop_comms()
        
    def reset(self):
        """Reset via the instrument connection object"""
        # Call logger reset here.
        pass
        
    ########################################################################
    # Protocol command interface.
    ########################################################################
        
    def get(self, params, timeout=10):
        """Get some parameters
        
        @param params A list of parameters to fetch. These must be in the
        fetchable parameter list
        @retval results A dict of the parameters that were queried
        @throws InstrumentProtocolException Confusion dealing with the
        physical device
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        """
        pass
    
    def set(self, params, timeout=10):
        """Set some parameters
        
        @param params A dict with the parameters to fetch. Must be in the
        fetchable list
        @throws InstrumentProtocolException Confusion dealing with the
        physical device
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        """
        pass

    def execute(self, command, timeout=10):
        """Execute a command
        
        @param command A single command as a list with the command ID followed
        by the parameters for that command
        @throws InstrumentProtocolException Confusion dealing with the
        physical device
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        """
        pass
    
    def execute_direct(self, bytes):
        """
        """
        pass
            
    ########################################################################
    # TBD.
    ########################################################################
    
    def get_status(self):
        """Gets the current status of the instrument.
        
        @retval status A dict of the current status of the instrument. Keys are
        listed in the status parameter list.
        """
        pass
    
    def get_capabilities(self):
        """
        """
        pass

    ########################################################################
    # Helper methods
    ########################################################################
    def _got_data(self, data):
       """
       Called by the logger whenever there is data available
       """
       pass

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
    """A base class for text-based command/response instruments
    
    For instruments that have simple command and response interations, this
    class provides some structure for manipulating data to and from the
    instrument.
    """
    def __init__(self, callback, prompts, newline):
        InstrumentProtocol.__init__(self, callback)
                
        self.eoln = newline
        """The end-of-line delimiter to use"""
    
        self.prompts = prompts
    
        self._linebuf = ''
        self._promptbuf = ''
        self._datalines = []

        self._build_handlers = {}
        self._response_handlers = {}

    def _add_build_handler(self, cmd, func):
        """
        Insert a handler class responsible for building a command to send to
        the instrument.
        
        @param cmd The high level key of the command to build for.
        """
        self._build_handlers[cmd] = func
        
    def _add_response_handler(self, cmd, func):
        """
        Insert a handler class responsible for handling the response to a
        command sent to the instrument.
        
        @param cmd The high level key of the command to responsd to.
        """
        self._response_handlers[cmd] = func
                
    def _got_data(self, data):
        """
        """
        # Update the line and prompt buffers.
        self._linebuf += data        
        self._promptbuf += data
        
    
    def _send_wakeup(self):
        """
        Use the logger to send what needs to be sent to wake up the device.
        This is intended to be overridden if there is any wake up needed.
        """
        pass
        
    def  _wakeup(self, timeout=10):
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

            for item in self.prompts.list():
                if self._promptbuf.endswith(item):
                    mi_logger.debug('Got prompt: %s', repr(item))
                    return item
            
            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException(InstErrorCode.TIMEOUT)
        
    def _get_response(self, timeout=10):
        """
        Get a response from the instrument
        @todo Consider cases with no prompt
        @param timeout The timeout in seconds
        @throw InstrumentProtocolExecption on timeout
        """
        # Grab time for timeout and wait for prompt.
        starttime = time.time()
        
        while True:
            
            for item in self.prompts.list():
                if self._promptbuf.endswith(item):
                    mi_logger.debug('Got prompt: %s', repr(item))
                    return (item, self._linebuf)
            
            if time.time() > starttime + timeout:
                raise InstrumentTimeoutException(InstErrorCode.TIMEOUT)

    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        Issue a command to the instrument after a wake up and clearing of
        buffers. Find the response handler, handle the response, and return it.
        
        @param cmd The high level command to issue
        @param args Arguments for the command
        @param kwargs timeout if one exists, defaults to 10
        @retval resp_result The response handler's return value
        @throw InstrumentProtocolException Bad command
        @throw InstrumentTimeoutException Timeout
        """
        timeout = kwargs.get('timeout', 10)
        
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            raise InstrumentProtocolException(InstErrorCode.BAD_DRIVER_COMMAND)
        
        cmd_line = build_handler(cmd, *args)
        
        # Wakeup the device, pass up exeception if timeout
        prompt = self._wakeup(timeout)
            
        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        mi_logger.debug('_do_cmd_resp: %s', repr(cmd_line))
        self._logger_client.send(cmd_line)

        # Wait for the prompt, prepare result and return, timeout exception
        (prompt, result) = self._get_response(timeout)
                
        resp_handler = self._response_handlers.get(cmd, None)
        resp_result = None
        if resp_handler:
            resp_result = resp_handler(result, prompt)

        return resp_result
            
    def _do_cmd_no_resp(self, cmd, *args, **kwargs):
        """
        Issue a command to the instrument after a wake up and clearing of
        buffers. No response is handled as a result of the command.
        
        @param cmd The high level command to issue
        @param args Arguments for the command
        @param kwargs timeout if one exists, defaults to 10
        @throw InstrumentProtocolException Bad command
        @throw InstrumentTimeoutException Timeout
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

    def _build_simple_command(self, command):
        """
        Build a very simple command string consisting of the command and the
        newline associated with this class. This is intended to be extended as
        needed by subclasses.
        
        @param command The command string to send.
        @retval The complete command, ready to send to the device.
        """
        return command+self.eoln

    @staticmethod
    def _int_to_string(v):
        """
        Write an int value to string formatted for sbe37 set operations.
        @param v An int val.
        @retval an int string formatted for sbe37 set operations, or None if
            the input is not a valid int value.
        """
        
        if not isinstance(v,int):
            return None
        else:
            return '%i' % v

    @staticmethod
    def _float_to_string(v):
        """
        Write a float value to string formatted for sbe37 set operations.
        @param v A float val.
        @retval a float string formatted for sbe37 set operations, or None if
            the input is not a valid float value.
        """

        if not isinstance(v,float):
            return None
        else:
            return '%e' % v
