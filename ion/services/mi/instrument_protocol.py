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

from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentStateException

class InstrumentProtocol(object):
    """The base class for an instrument protocol
    
    The classes derived from this class will carry out the specific
    interactions between a specific device and the instrument driver. At this
    layer of interaction, there are no conflicts or transactions as that is
    handled at the layer above this. Think of this as encapsulating the
    transport layer of the communications.
    """
    
    implements(IInstrumentConnection)
    
    def __init__(self, connection):
        """Set instrument connect at creation
        
        @param connection An InstrumetnConnection object
        """
        self.instrument_connection = connection
        self.protocol_fsm = None
        '''This FSM needs to be created by child class'''
        
    def get(self, params=[]):
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

    def set(self, params={}):
        """Get some parameters
        
        @param params A dict with the parameters to fetch. Must be in the
        fetchable list
        @throws InstrumentProtocolException Confusion dealing with the
        physical device
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        """

    def execute(self, command=[]):
        """Execute a command
        
        @param command A single command as a list with the command ID followed
        by the parameters for that command
        @throws InstrumentProtocolException Confusion dealing with the
        physical device
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        """
        
    def get_config(self):
        """Get an entire configuration from a device
        
        @retval config A dict with all of the device's parameters and values at a
        given moment in time.
        @retval results A dict of the entire configuration
        @throws InstrumentProtocolException Confusion dealing with the
        physical device, possibly due to interrupted communications
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        """
    
    def restore_config(self, config={}):
        """Restore the complete supplied config to the device.
        
        This method must take into account any ordering of set requests as
        required to make the entire operation stick. Should have the ability
        to back out changes that failed mid-application, too.
        
        @param config A dict structure of the configuration that should be
        applied to the instrument. May have come directly from a call to
        get_config at some point before.
        @throws InstrumentProtocolException Confusion dealing with the
        physical device, possibly due to interrupted communications
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        """
    
    def get_status(self):
        """Gets the current status of the instrument.
        
        @retval status A dict of the current status of the instrument. Keys are
        listed in the status parameter list.
        """
    
    #######################
    # Instrument Connection interface pass through
    #######################

    def connect(self, *args):
        """Connect via the instrument connection object
        
        @param args connection arguments
        @throws InstrumentConnectionException
        """
        self.instrument_connection.connect(args)

    def disconnect(self):
        """Disconnect via the instrument connection object
        
        @throws InstrumentConnectionException
        """
        self.instrument_connection.disconnect()

    def reset(self):
        """Reset via the instrument connection object"""
        self.instrument_connection.reset()


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
    
    def __init__(self, connection,
                 command_list=None,
                 response_regex_list=None,
                 get_prefix="",
                 set_prefix="",
                 set_delimiter="",
                 execute_prefix="",
                 eoln="\n"):
        InstrumentProtocol.__init__(connection)
        
        self.command_list = command_list
        """The BaseEnum command keys to be used"""
    
        self.response_regex_list = response_regex_list
        """The response regex dict to be used to map a command's repsonse to
        a specific format
        """
        
        self.get_prefix = get_prefix
        """The prefix used to start a get
        
        This will be added before the parameter string when querying.
        """
        
        self.set_prefix = set_prefix
        """The prefix used to start a set
        
        This will be added before the parameter string when setting.
        """
        
        self.set_delimiter = set_delimiter
        """The delimiter string between a parameter name an new value"""
        
        self.execute_prefix = execute_prefix
        """The prefix used to start a command
        
        This will be added before the command name string when executing
        a command.
        """
        
        self.eoln = eoln
        """The end-of-line delimiter to use"""
    
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
        
    def get(self, params=[]):
        
