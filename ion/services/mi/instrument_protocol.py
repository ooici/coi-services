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
import re

from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentStateException
from ion.services.mi.instrument_connection import IInstrumentConnection
from ion.services.mi.common import InstErrorCode
from ion.services.mi.logger_process import EthernetDeviceLogger, LoggerClient

mi_logger = logging.getLogger('mi_logger')

class ParameterDictVal(object):
    """
    """
    def __init__(self, name, pattern, f_getval, f_format, value=None):
        """
        """
        self.name = name
        self.pattern = pattern
        self.regex = re.compile(pattern)
        self.f_getval = f_getval
        self.f_format = f_format
        self.value = value

    def update(self, input):
        """
        """
        match = self.regex.match(input)
        if match:
            self.value = self.f_getval(match)
            mi_logger.debug('Updated parameter %s=%s', self.name, str(self.value))
            return True
        else: return False


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
        """Get some parameters
        
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
    """    
    def __init__(self, callback=None,
                 command_list=None,
                 response_regex_list=None,
                 get_prefix="",
                 set_prefix="",
                 set_delimiter="",
                 execute_prefix="",
                 eoln="\n"):
    """
    def __init__(self, callback, prompts, newline):
        InstrumentProtocol.__init__(self, callback)
        
        #self.command_list = command_list
        """The BaseEnum command keys to be used"""
    
        #self.response_regex_list = response_regex_list
        """The response regex dict to be used to map a command's repsonse to
        a specific format
        """
        
        #self.get_prefix = get_prefix
        """The prefix used to start a get
        
        This will be added before the parameter string when querying.
        """
        
        #self.set_prefix = set_prefix
        """The prefix used to start a set
        
        This will be added before the parameter string when setting.
        """
        
        #self.set_delimiter = set_delimiter
        """The delimiter string between a parameter name an new value"""
        
        #self.execute_prefix = execute_prefix
        """The prefix used to start a command
        
        This will be added before the command name string when executing
        a command.
        """
        
        self.eoln = newline
        """The end-of-line delimiter to use"""
    
        self.prompts = prompts
    
        self._linebuf = ''
        self._promptbuf = ''
        self._datalines = []

        self._build_handlers = {}
        self._response_handlers = {}
        self._parameters = {}
    
    def _identify_response(self, response_str=""):
        """Format the response to a command into a usable form
        
        @param response_str The raw response string from the instrument
        @retval response A usably-formatted response structure. A dict?
        """
        assert(isinstance(response_str, str))
        # Apply regexes, separators, delimiters, Eolns, etc.
        
    def _build_command(self, command=None, cmd_args=""):
        """Construct a command string based on the values supplied
        
        @param command A usable struucture (dict?) that needs to be converted
        into a string for the instrument
        @param cmd_args an argument string to append to the command
        @retval command_str The string to send to the instrument
        """
        assert(isinstance(command, dict))
        # Apply regexes, separators, delimiters, Eolns, etc.
                
    ########################################################################
    # Incomming data callback.
    ########################################################################            

    def _got_data(self, data):
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
        """
        pass
        
    def  _wakeup(self, timeout=10):
        """
        """
        """
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
                return InstErrorCode.TIMEOUT
        
    ########################################################################
    # Command-response helpers.
    ########################################################################    

    def _add_build_handler(self, cmd, func):
        """
        """
        self._build_handlers[cmd] = func
        
    def _add_response_handler(self, cmd, func):
        """
        """
        self._response_handlers[cmd] = func

    def _get_response(self, timeout=10):
        """
        """
        # Grab time for timeout and wait for prompt.
        starttime = time.time()
        
        while True:
            
            for item in self.prompts.list():
                if self._promptbuf.endswith(item):
                    mi_logger.debug('Got prompt: %s', repr(item))
                    return (item, self._linebuf)
            
            if time.time() > starttime + timeout:
                return (InstErrorCode.TIMEOUT, None)

    def _do_cmd_resp(self, cmd, *args, **kwargs):
        """
        """
        timeout = kwargs.get('timeout', 10)
        
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            return InstErrorCode.BAD_DRIVER_COMMAND
        
        cmd_line = build_handler(cmd, *args)
        
        # Wakeup the device.
        prompt = self._wakeup(timeout)
        if prompt == InstErrorCode.TIMEOUT:
            return prompt
            
        # Clear line and prompt buffers for result.
        self._linebuf = ''
        self._promptbuf = ''

        # Send command.
        mi_logger.debug('_do_cmd_resp: %s', repr(cmd_line))
        self._logger_client.send(cmd_line)

        # Wait for the prompt, prepare result and return.
        (prompt, result) = self._get_response(timeout)
        if prompt == InstErrorCode.TIMEOUT:
            return prompt
                
        resp_handler = self._response_handlers.get(cmd, None)
        if resp_handler:
            resp_handler(result, prompt)

        return InstErrorCode.OK
            
    def _do_cmd_no_resp(self, cmd, *args, **kwargs):
        """
        """
        timeout = kwargs.get('timeout', 10)        
        
        build_handler = self._build_handlers.get(cmd, None)
        if not build_handler:
            return InstErrorCode.BAD_DRIVER_COMMAND
        
        cmd_line = build_handler(cmd, *args)
        
        # Wakeup the device.
        prompt = self._wakeup(timeout)
        if prompt == InstErrorCode.TIMEOUT:
            return prompt
        
        # Clear line and prompt buffers for result.
        self._linebuf = ''

        # Send command.
        mi_logger.debug('_do_cmd_no_resp: %s', repr(cmd_line))
        self._logger_client.send(cmd_line)

        return InstErrorCode.OK

    ########################################################################
    # Parameter dict helpers.
    ########################################################################    

    def _add_param_dict(self, name, pattern, f_getval, f_format, value=None):
        """
        """
        self._parameters[name] = ParameterDictVal(name, pattern, f_getval,
                            f_format, value)
    
    def _get_param_dict(self, name):
        """
        """
        return self._parameters[name].value
        
    def _set_param_dict(self, name, value):
        """
        """
        self._parameters[name] = value
        
    def _update_param_dict(self, input):
        """
        """
        for (name, val) in self._parameters.iteritems():
            if val.update(input):
                break
            
            