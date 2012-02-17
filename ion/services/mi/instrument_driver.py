#!/usr/bin/env python

"""
@package ion.services.mi.instrument_driver Instrument driver structures
@file ion/services/mi/instrument_driver.py
@author Steve Foley
@author Edward Hunter
@brief Instrument driver classes that provide structure towards interaction
with individual instruments in the system.
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

from ion.services.mi.common import BaseEnum
from ion.services.mi.exceptions import InstrumentConnectionException 
from ion.services.mi.common import DEFAULT_TIMEOUT

class DriverChannel(BaseEnum):
    """Common channels for all sensors. Driver subclasses contain a subset."""
    INSTRUMENT = 'CHANNEL_INSTRUMENT'
    CTD = 'CHANNEL_CTD'
    ALL = 'CHANNEL_ALL'

class DriverCommand(BaseEnum):
    """Common driver commands
    
    Commands and events should have unique strings that either indicate
    something or can be used in some other rational fashion."""
    
    ACQUIRE_SAMPLE = 'DRIVER_CMD_ACQUIRE_SAMPLE'
    START_AUTO_SAMPLING = 'DRIVER_CMD_START_AUTO_SAMPLING'
    STOP_AUTO_SAMPLING = 'DRIVER_CMD_STOP_AUTO_SAMPLING'
    TEST = 'DRIVER_CMD_TEST'
    CALIBRATE = 'DRIVER_CMD_CALIBRATE'
    RESET = 'DRIVER_CMD_RESET'
    GET = 'DRIVER_CMD_GET'
    SET = 'DRIVER_CMD_SET'
    GET_STATUS = 'DRIVER_CMD_GET_STATUS'
    GET_METADATA = 'DRIVER_CMD_GET_METADATA'
    UPDATE_PARAMS = 'DRIVER_CMD_UPDATE_PARAMS'
    TEST_ERRORS = 'DRIVER_CMD_TEST_ERRORS'    


class DriverState(BaseEnum):
    """Common driver state enum"""
    
    UNCONFIGURED = 'DRIVER_STATE_UNCONFIGURED'
    DISCONNECTED = 'DRIVER_STATE_DISCONNECTED'
    CONNECTING = 'DRIVER_STATE_CONNECTING'
    DISCONNECTING = 'DRIVER_STATE_DISCONNECTING'
    CONNECTED = 'DRIVER_STATE_CONNECTED'
    ACQUIRE_SAMPLE = 'DRIVER_STATE_ACQUIRE_SAMPLE'
    UPDATE_PARAMS = 'DRIVER_STATE_UPDATE_PARAMS'
    SET = 'DRIVER_STATE_SET'
    AUTOSAMPLE = 'DRIVER_STATE_AUTOSAMPLE'
    TEST = 'DRIVER_STATE_TEST'
    CALIBRATE = 'DRIVER_STATE_CALIBRATE'
    DETACHED = 'DRIVER_STATE_DETACHED'
    COMMAND = 'DRIVER_STATE_COMMAND'

class DriverEvent(BaseEnum):
    """Common driver event enum
    
    Commands and events should have unique strings that either indicate
    something or can be used in some other rational fashion."""
    
    CONFIGURE = 'DRIVER_EVENT_CONFIGURE'
    INITIALIZE = 'DRIVER_EVENT_INITIALIZE'
    CONNECT = 'DRIVER_EVENT_CONNECT'
    CONNECTION_COMPLETE = 'DRIVER_EVENT_CONNECTION_COMPLETE'
    CONNECTION_FAILED = 'DRIVER_EVENT_CONNECTION_FAILED'
    CONNECTION_LOST = 'DRIVER_CONNECTION_LOST'
    DISCONNECT = 'DRIVER_EVENT_DISCONNECT'
    DISCONNECT_COMPLETE = 'DRIVER_EVENT_DISCONNECT_COMPLETE'
    DISCONNECT_FAILED = 'DRIVER_EVENT_DISCONNECT_FAILED'
    PROMPTED = 'DRIVER_EVENT_PROMPTED'
    DATA_RECEIVED = 'DRIVER_EVENT_DATA_RECEIVED'
    COMMAND_RECEIVED = 'DRIVER_EVENT_COMMAND_RECEIVED'
    RESPONSE_TIMEOUT = 'DRIVER_EVENT_RESPONSE_TIMEOUT'
    SET = 'DRIVER_EVENT_SET'
    GET = 'DRIVER_EVENT_GET'
    EXECUTE = 'DRIVER_EVENT_EXECUTE'
    ACQUIRE_SAMPLE = 'DRIVER_EVENT_ACQUIRE_SAMPLE'
    START_AUTOSAMPLE = 'DRIVER_EVENT_START_AUTOSAMPLE'
    STOP_AUTOSAMPLE = 'DRIVER_EVENT_STOP_AUTOSAMPLE'
    TEST = 'DRIVER_EVENT_TEST'
    STOP_TEST = 'DRIVER_EVENT_STOP_TEST'
    CALIBRATE = 'DRIVER_EVENT_CALIBRATE'
    RESET = 'DRIVER_EVENT_RESET'
    ENTER = 'DRIVER_EVENT_ENTER'
    EXIT = 'DRIVER_EVENT_EXIT'
    ATTACH = 'DRIVER_EVENT_ATTACH'
    DETACH = 'DRIVER_EVENT_DETACH'
    UPDATE_PARAMS = 'DRIVER_EVENT_UPDATE_PARAMS'
    

class DriverStatus(BaseEnum):
    """Common driver status enum"""
    
    DRIVER_VERSION = 'DRIVER_STATUS_DRIVER_VERSION'
    DRIVER_STATE = 'DRIVER_STATUS_DRIVER_STATE'
    OBSERVATORY_STATE = 'DRIVER_STATUS_OBSERVATORY_STATE'
    DRIVER_ALARMS = 'DRIVER_STATUS_DRIVER_ALARMS'
    ALL = 'DRIVER_STATUS_ALL'


class DriverParameter(BaseEnum):
    """Common driver parameter enum"""
    
    ALL = 'DRIVER_PARAMETER_ALL'


class ObservatoryState(BaseEnum):
    """The status of a device in observatory mode"""
    
    NONE = 'OBSERVATORY_STATUS_NONE'
    STANDBY = 'OBSERVATORY_STATUS_STANDBY'
    STREAMING = 'OBSERVATORY_STATUS_STREAMING'
    TESTING = 'OBSERVATORY_STATUS_TESTING'
    CALIBRATING = 'OBSERVATORY_STATUS_CALIBRATING'
    UPDATING = 'OBSERVATORY_STATUS_UPDATING'
    ACQUIRING = 'OBSERVATORY_STATUS_ACQUIRING'
    UNKNOWN = 'OBSERVATORY_STATUS_UNKNOWN'

"""
TODO:
Do we want timeouts in the driver interface. timeout=DEFAULT_TIMEOUT.
How would we provide such behavior?
"""

class InstrumentDriver(object):
    """The base instrument driver class
    
    This is intended to be extended where necessary to provide a coherent
    driver for interfacing between the instrument and the instrument agent.
    Think of this class as encapsulating the session layer of the instrument
    interaction.
    
    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+SA+SV+Instrument+Driver+Interface
    """

    def __init__(self, evt_callback):

        # Instrument channel dict.
        # A dictionary of channel-name keys and channel protocol object values.
        # We need to change this to protocol or connection name, rather than channel.
        self.channels = {}
        """@todo clean this up as chan_map gets used more"""
        
        self.chan_map = {}
        """The channel to protocol mapping"""
        
        self.send_event = evt_callback
        """The callback method that the protocol uses to alert the driver to
        events"""
        
        self.instrument_commands = None
        """A BaseEnum-derived class of the acceptable commands"""
        
        self.instrument_parameters = None
        """A BaseEnum-derived class of the acceptable parameters"""
    
        self.instrument_channels = None
        """A BaseEnum-derived class of the acceptable channels"""
        
        self.instrument_errors = None
        """A BaseEnum-derived class of the acceptable errors"""
        
        self.instrument_states = None
        """A BaseEnum-derived class of the acceptable channel states"""
        
        self.instrument_active_states = []
        """A list of the active states found in the BaseEnum-derived state
        class"""
        
        # Below are old members with comments from EH.
        #
        # Protocol will create and own the connection it fronts.
        #self.instrument_connection = None
        #"""An object for manipulating connect and disconnect to an instrument"""
        # This is the self.channels member above. Change name. A dict of protocols.
        #self.instrument_protocol = None
        #"""The instrument-specific protocol object"""
        # This is supplied by the driver process that contains and creates the driver.
        #self.instrument_comms_method = None
        #"""The communications method formatting object"""
        # Probably an enum class with execute commands that are possible.
        #self.instrument_commands = None
        #"""The instrument-specific command list"""
        # Ditch this.
        #self.instrument_metadata_parameters = None
        #"""The instrument-specific metadata parameter list"""
        # To be added. A dictionary or class that knows how to match, parse and format all of its params.
        #self.instrument_parameters = None
        #"""The instrument-specific parameter list"""
        # TBD.
        #self.instrument_channels = None
        #"""The instrument-specific channel list"""
        # Why bother, make errors draw from the common list.
        #self.instrument_errors = None
        #"""The instrument-specific error list"""
        # This should return the instrument commands enum values
        #self.instrument_capabilities = None
        #"""The instrument-specific capabilities list"""
        # TBD.
        #self.instrument_status = None
        #"""The instrument-specific status list"""
        
        # Do we need this also? Just use simple harmonizing logic
        # at driver level if there are multiple connections. If one connection,
        # one state machine.
        # Setup the state machine
        """
        self.driver_fsm = FSM(DriverState.UNCONFIGURED)
        self.driver_fsm.add_transition(DriverEvent.CONFIGURE,
                                       DriverState.UNCONFIGURED,
                                       action=self._handle_configure,
                                       next_state=DriverState.DISCONNECTED)
        self.driver_fsm.add_transition(DriverEvent.INITIALIZE,
                                       DriverState.DISCONNECTED,
                                       action=self._handle_initialize,
                                       next_state=DriverState.UNCONFIGURED)        
        self.driver_fsm.add_transition(DriverEvent.DISCONNECT_FAILED,
                                       DriverState.DISCONNECTING,
                                       action=self._handle_disconnect_failure,
                                       next_state=DriverState.CONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.DISCONNECT_COMPLETE,
                                       DriverState.DISCONNECTING,
                                       action=self._handle_disconnect_success,
                                       next_state=DriverState.DISCONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.DISCONNECT,
                                       DriverState.CONNECTED,
                                       action=self._handle_disconnect,
                                       next_state=DriverState.DISCONNECTING) 
        self.driver_fsm.add_transition(DriverEvent.CONNECT,
                                       DriverState.DISCONNECTED,
                                       action=self._handle_connect,
                                       next_state=DriverState.CONNECTING) 
        self.driver_fsm.add_transition(DriverEvent.CONNECTION_COMPLETE,
                                       DriverState.CONNECTING,
                                       action=self._handle_connect_success,
                                       next_state=DriverState.CONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.CONNECTION_FAILED,
                                       DriverState.CONNECTING,
                                       action=self._handle_connect_failed,
                                       next_state=DriverState.DISCONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.START_AUTOSAMPLE,
                                       DriverState.CONNECTED,
                                       action=self._handle_start_autosample,
                                       next_state=DriverState.AUTOSAMPLE) 
        self.driver_fsm.add_transition(DriverEvent.STOP_AUTOSAMPLE,
                                       DriverState.AUTOSAMPLE,
                                       action=self._handle_stop_autosample,
                                       next_state=DriverState.CONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.CONNECTION_LOST,
                                       DriverState.AUTOSAMPLE,
                                       action=self._handle_connection_lost,
                                       next_state=DriverState.DISCONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.CONNECTION_LOST,
                                       DriverState.CONNECTED,
                                       action=self._handle_connection_lost,
                                       next_state=DriverState.DISCONNECTED) 
        
        self.driver_fsm.add_transition(DriverEvent.DATA_RECEIVED,
                                       DriverState.CONNECTED,
                                       action=self._handle_data_received,
                                       next_state=DriverState.CONNECTED)
        self.driver_fsm.add_transition(DriverEvent.DATA_RECEIVED,
                                       DriverState.AUTOSAMPLE,
                                       action=self._handle_data_received,
                                       next_state=DriverState.AUTOSAMPLE)
        self.driver_fsm.add_transition(DriverEvent.GET,
                                       DriverState.CONNECTED,
                                       action=self._handle_get,
                                       next_state=DriverState.CONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.SET,
                                       DriverState.CONNECTED,
                                       action=self._handle_set,
                                       next_state=DriverState.CONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.ACQUIRE_SAMPLE,
                                       DriverState.CONNECTED,
                                       action=self._handle_acquire_sample,
                                       next_state=DriverState.CONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.TEST,
                                       DriverState.CONNECTED,
                                       action=self._handle_test,
                                       next_state=DriverState.CONNECTED) 
        self.driver_fsm.add_transition(DriverEvent.CALIBRATE,
                                       DriverState.CONNECTED,
                                       action=self._handle_calibrate,
                                       next_state=DriverState.CONNECTED) 

        self.driver_fsm.add_transition_catch(DriverEvent.RESET,
                                       action=self._handle_reset,
                                       next_state=DriverState.UNCONFIGURED)    
        """
        
    ########################################################################
    # Channel connection interface.
    ########################################################################
    
    def initialize(self, channels, *args, **kwargs):
        """
        Return a device channel to an unconnected, unconfigured state.
        @param channels List of channel names to initialize.
        @param timeout Number of seconds before this operation times out
        """
        pass

    def configure(self, configs, *args, **kwargs):
        """
        Configure the driver for communications with an instrument channel.
        @param config A dict containing channel name keys, with
        dict values containing the comms configuration for the named channel.
        @param timeout Number of seconds before this operation times out        
        """
        pass        
        
    def connect(self, channels, *args, **kwargs):
        """
        Establish communications with a device channel.
        @param channels List of channel names to connect.
        @param timeout Number of seconds before this operation times out
        """
        pass
    
    def disconnect(self, channels, *args, **kwargs):
        """
        Disconnect communications with a device channel.
        @param channels List of channel names to disconnect.
        @param timeout Number of seconds before this operation times out
        
        """
        pass

    def detach(self, channels, *args, **kwargs):
        """
        Disconnect communications with a device channel.
        @param channels List of channel names to disconnect.
        @param timeout Number of seconds before this operation times out
        """
        pass

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, *args, **kwargs):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass
    
    def set(self, params, *args, **kwargs):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass

    def execute_acquire_sample(self, channels, *args, **kwargs):
        """
        """
        pass

    def execute_start_autosample(self, channels, *args, **kwargs):
        """
        """
        pass

    def execute_stop_autosample(self, channels, *args, **kwargs):
        """
        """
        pass

    def execute_test(self, channels, *args, **kwargs):
        """
        """
        pass
    
    def execute_direct(self, channels, *args, **kwargs):
        """
        """
        pass
        
    ################################
    # Announcement callback from protocol
    ################################
    def protocol_callback(self, event):
        """The callback method that the protocol calls when there is some sort
        of event worth notifying the driver about.
        
        @param event The event object from the event service
        @todo Make event a real event object of some sort instead of the hack
        tuple of (DriverAnnouncement enum, any error code, message)
        """
    
    
    ########################################################################
    # TBD.
    ########################################################################    
        
    def get_resource_commands(self):
        """
        Gets the list of (channel, cmd) pairs gathered from all the
        channels in this driver.
        """
        result = []
        for channel in self.get_channels():
            cmds = self.chan_map[channel].get_resource_commands()
            if cmds:
                result.append([(channel, cmd) for cmd in cmds])
        return result

    def get_resource_params(self):
        """
        Gets the list of (channel, param) pairs gathered from all the
        channels in this driver.
        """
        result = []
        for channel in self.get_channels():
            params = self.chan_map[channel].get_resource_params()
            if params:
                result.append([(channel, param) for param in params])
        return result

    def get_capabilities(self, channels, *args, **kwargs):
        """
        @param timeout Number of seconds before this operation times out
        """
        pass

    def get_channels(self):
        """
        """
        return self.instrument_channels.list()
    
    def get_active_channels(self):
        """Get a list of channels that are in some form of an active state
        
        @retval a list of channels that are in an active state
        """
        result = []
        chan_state_dict = self.get_current_state([DriverChannels.ALL])
        for chan in chan_state_dict.keys:
            if chan_state_dict[chan] in self.instrument_active_states:
                result.append[chan]
            
        return result
    
    def get_current_state(self, channels=None):
        """Get the current state of the instrument
        
        @param channels A list of the channels of interest, values from the
        specific driver's channel enumeration list. Default is DriverChannel.INSTRUMENT
        @retval result A dict of {channel_name:state}
        @throws RequiredParameterException When a parameter is missing
        """
        if (channels == None):
            channels = DriverChannel.INSTRUMENT
            
        (result, valid_channels) = self._check_channel_args(channels)

        for channel in valid_channels:
            result[channel] = self.chan_map[channel].get_current_state()

        return result
    
    ########################################################################
    # Private helpers.
    ######################################################################## 
    def _check_channel_args(self, channels):
        """Checks the channel arguments that are supplied
        
        They should be:
        a. In the self.instrument_channel enum
        b. Have a specific channel or DriverChannel.ALL or DriverChannel.INSTRUMENT
        @param channels The list of channels that are being checked for validity
        @retval A tuple of dicts. The first element is a dict of invalid
        channel IDs mapped to an INVALID_CHANNEL error code. The second
        element is a list of valid channels. Example:
        ({Channel.BAD_NAME:InstErrorCode.INVALID_CHANNEL},[Channel.CHAN1])
        @throws RequiredParameterException If the arguments are missing or invalid
        """
        valid_channels = []
        result = {}
        
        if (channels == None) or (not isinstance(channels, (list, tuple))):
            raise RequiredParameterException()
            
        elif len(channels) == 0:
            raise RequiredParameterException()
            
        else:
            clist = self.instrument_channels.list()
            
            if DriverChannel.ALL in clist:
                clist.remove(DriverChannel.ALL)
            if DriverChannel.INSTRUMENT in clist:
                clist.remove(DriverChannel.INSTRUMENT)
                
            # Expand "ALL" channel keys.
            if DriverChannel.ALL in channels:
                channels = clist
                #channels += clist
                #channels = [c for c in channels if c != DriverChannel.ALL]

            # Make unique
            #channels = list(set(channels))

            # Separate valid and invalid channels.
            valid_channels = [c for c in channels if c in clist]
            invalid_channels = [c for c in channels if c not in clist]
            
            # Build result dict with invalid entries.
            for c in invalid_channels:
                invalid_chan_dict[c] = InstErrorCode.INVALID_CHANNEL
                
        return (invalid_chan_dict, valid_channels)

    ######################
    # State change handlers
    #######################
    #def _handle_configure(self):
    #    """State change handler"""
    #
    #def _handle_initialize(self):
    #    """State change handler"""
    #
    #def _handle_disconnect_failure(self):
    #    """State change handler"""
    #
    #def _handle_disconnect_success(self):
    #    """State change handler"""
    #
    #def _handle_disconnect(self):
    #    """State change handler"""
    #
    #def _handle_connect(self):
    #    """State change handler"""
    #
    #def _handle_connect_success(self):
    #    """State change handler"""
    #
    #def _handle_connect_failed(self):
    #    """State change handler"""
    #
    #def _handle_start_autosample(self):
    #    """State change handler"""
    #
    #def _handle_stop_autosample(self):
    #    """State change handler"""
    #
    #def _handle_connection_lost(self):
    #    """State change handler"""
    #
    #def _handle_reset(self):
    #    """State change handler"""
    #
    #def _handle_handle_data_received(self):
    #    """State change handler"""
    #    
    #def _handle_handle_get(self):
    #    """State change handler"""
    #    
    #def _handle_handle_set(self):
    #    """State change handler"""
    #
    #def _handle_handle_acquire_sample(self):
    #    """State change handler"""
    #    
    #def _handle_handle_test(self):
    #    """State change handler"""
    #
    #def _handle_handle_calibrate(self):
    #    """State change handler"""  
