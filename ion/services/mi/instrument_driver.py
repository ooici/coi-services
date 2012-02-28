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

import logging
from ion.services.mi.common import BaseEnum, InstErrorCode
from ion.services.mi.exceptions import InstrumentConnectionException 
from ion.services.mi.exceptions import RequiredParameterException 
from ion.services.mi.common import DEFAULT_TIMEOUT
from ion.services.mi.instrument_fsm_args import InstrumentFSM


mi_logger = logging.getLogger('mi_logger')

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

class ConnectionState(BaseEnum):
    CONNECTED = 'CONNECTION_STATE_CONNECTED'
    CONNECTIING = 'CONNECTION_STATE_CONNECTING'
    DISCONNECTED = 'CONNECTION_STATE_DISCONNECTED'
    DISCONNECTING = 'CONNECTION_STATE_DISCONNECTING'
    UNKNOWN = 'CONNECTION_STATE_UNKNOWN'
    
class ConnectionEvent(BaseEnum):
    CONNECT = 'CONNECTION_EVENT_CONNECT'
    DISCONNECT = 'CONNECTION_EVENT_DISCONNECT'
    ENTER_STATE = 'CONNECTION_EVENT_ENTER'
    EXIT_STATE = 'CONNECTION_EVENT_EXIT'
    
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
        
        self.state_map = None
        """An optional mapping of specific driver states to DriverState enums"""
        
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
        
        self.connection_fsm = InstrumentFSM(ConnectionState, ConnectionEvent,
                                        ConnectionEvent.ENTER_STATE,
                                        ConnectionEvent.EXIT_STATE,
                                        InstErrorCode.UNHANDLED_EVENT)
        self.connection_fsm.add_handler(ConnectionState.DISCONNECTED,
                                    ConnectionEvent.CONNECT,
                                    self._handler_disconnected_connect)
        self.connection_fsm.add_handler(ConnectionState.CONNECTED,
                                    ConnectionEvent.DISCONNECT,
                                    self._handler_connected_disconnect)
        self.connection_fsm.add_handler(ConnectionState.UNKNOWN,
                                    ConnectionEvent.CONNECT,
                                    self._handler_unknown_connect)
        self.connection_fsm.add_handler(ConnectionState.UNKNOWN,
                                    ConnectionEvent.DISCONNECT,
                                    self._handler_unknown_disconnect)
        self.connection_fsm.start(ConnectionState.UNKNOWN)
                              
        """
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
        mi_logger.debug("Issuing empty initialize...")


    def configure(self, configs, *args, **kwargs):
        """
        Configure the driver for communications with an instrument channel.
        @param config A dict containing channel name keys, with
        dict values containing the comms configuration for the named channel.
        @param timeout Number of seconds before this operation times out
        @retval Dict of channels and configure outputs
        @throws RequiredParameterException On missing parameters in config
        """
        assert configs != None
        
        mi_logger.debug("Issuing base configure...")
        try:
            channels = configs.keys()
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                config = configs[channel]
                result[channel] = self.chan_map[channel].configure(config, *args, **kwargs)
                    
        except (RequiredParameterException, TypeError):
            result = InstErrorCode.REQUIRED_PARAMETER

        return result
        
    def connect(self, channels=None, *args, **kwargs):
        """
        Establish communications with a device channel.
        @param channels List of channel names to connect.
        @param timeout Number of seconds before this operation times out
        @retval Dict of channels and configure outputs
        @throws RequiredParameterException on missing connection parameter
        """
        if channels == None:
            channels = [DriverChannel.INSTRUMENT]
            
        mi_logger.debug("Issuing base connect...")
        try:
            (result, valid_channels) = self._check_channel_args(channels)

            for channel in valid_channels:
                result[channel] = self.chan_map[channel].connect(*args, **kwargs)
                
            self.connection_fsm.on_event(ConnectionEvent.CONNECT)
        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
                
        return result
    
    def disconnect(self, channels=None, *args, **kwargs):
        """
        Disconnect communications with a device channel.
        @param channels List of channel names to disconnect.
        @param timeout Number of seconds before this operation times out
        
        """
        if channels == None:
            channels = [DriverChannel.INSTRUMENT]

        mi_logger.debug("Issuing empty disconnect...")
        self.connection_fsm.on_event(ConnectionEvent.DISCONNECT)

    def detach(self, channels, *args, **kwargs):
        """
        Disconnect communications with a device channel.
        @param channels List of channel names to disconnect.
        @param timeout Number of seconds before this operation times out
        """
        mi_logger.debug("Issuing empty detach...")

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, *args, **kwargs):
        """
        Gets the value of the requested parameters. The general form of
        params should be a list of tuples (c,p) where c and p are channels
        and parameters in the self.instrument_channels and
        self.instrument_parameters lists respectively.
        """
        (result, valid_params) = self._check_get_args(params)

        if mi_logger.isEnabledFor(logging.DEBUG):
            mi_logger.debug("result=%s  valid_params=%s" %
                            (str(result), str(valid_params)))

        for (channel, parameters) in valid_params:
            proto = self.chan_map[channel]
            # ask channel's protocol to get the values for these parameters:
            proto_result = proto.get(parameters, *args, **kwargs)
            for parameter in parameters:
                result[(channel, parameter)] = proto_result[parameter]

        return result

    def set(self, params, *args, **kwargs):
        """
        Sets parameters in this driver.

        @param params A dict of (c,p):v entries where c and p are channels and
        parameters in the self.instrument_channels and
        self.instrument_parameters lists respectively,
        and v is the desired value for such (c,p) pair. For example:
          {
            ("MyChannel.CHAN1", MyParam.PARAM1): 123,
            ("MyChannel.CHAN1", MyParam.PARAM4): 456,
            ("MyChannel.CHAN3", MyParam.PARAM5): 789,
          }
        """
        (result, valid_params) = self._check_set_args(params)

        if mi_logger.isEnabledFor(logging.DEBUG):
            mi_logger.debug("result=%s  valid_params=%s" %
                            (str(result), str(valid_params)))

        for channel in valid_params:
            parameters = valid_params[channel]
            proto = self.chan_map[channel]
            # ask channel's protocol to set the values for these parameters:
            proto_result = proto.set(parameters, *args, **kwargs)
            for parameter in parameters.keys():
                result[(channel, parameter)] = proto_result[parameter]

        return result

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
                result.extend([(channel, cmd) for cmd in cmds])
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
                result.extend([(channel, param) for param in params])
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
        chan_state_dict = self.get_current_state([DriverChannel.ALL])
        for chan in chan_state_dict.keys:
            if chan_state_dict[chan] in self.instrument_active_states:
                result.append[chan]
            
        return result
    
    def get_current_state(self, channels=None):
        """Get the current state of the instrument
        
        @param channels A list of the channels of interest, values from the
        specific driver's channel enumeration list. Default is DriverChannel.INSTRUMENT
        @retval result A dict of {channel_name:state}. If disconnected,
        {DriverChannel.INSTRUMENT:ConnectionState.DISCONNECTED
        @throws RequiredParameterException When a parameter is missing
        """

        if (channels == None):
            channels = [DriverChannel.INSTRUMENT]
        
        (result, valid_channels) = self._check_channel_args(channels)
        if self.connection_fsm.get_current_state() == ConnectionState.DISCONNECTED:
            return {DriverChannel.INSTRUMENT:ConnectionState.DISCONNECTED}

        for channel in valid_channels:
            result[channel] = self.chan_map[channel].get_current_state()

            # lookup the driver state if there is a mapping
            if (self.state_map):
                result[channel] = self.state_map[result[channel]]
                
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
        invalid_chan_dict = {}
        
        if (channels == None) or (not isinstance(channels, (list, tuple))):
            raise RequiredParameterException()
            
        elif len(channels) == 0:
            raise RequiredParameterException()
            
        else:
            clist = self.instrument_channels.list()
            
            if DriverChannel.ALL in clist:
                clist.remove(DriverChannel.ALL)
                
            # Expand "ALL" channel keys.
            if DriverChannel.ALL in channels:
                channels = clist
                channels.remove(DriverChannel.INSTRUMENT)
                #channels += clist
                #channels = [c for c in channels if c != DriverChannel.ALL]

            # Make unique
            channels = list(set(channels))

            # Separate valid and invalid channels.
            for c in channels:
                if c in clist:
                    valid_channels.append(c)
                else:
                    invalid_chan_dict[c] = InstErrorCode.INVALID_CHANNEL
                
        return (invalid_chan_dict, valid_channels)

    def _check_get_args(self, params):
        """
        Checks the params arguments that are supplied.

        @param params A list of tuples (c,p) where c and p are channels and
        parameters in the self.instrument_channels and
        self.instrument_parameters lists respectively.

        @retval A tuple (result, valid_params) where:

        result: an empty dict if the given params arguments is valid;
        otherwise something like:
          { ("bad_channel", "whatever") : InstErrorCode.INVALID_CHANNEL }

        valid_params: a list of tuples (c,pp) where pp is the list of
        parameters associated to channel c in the given params argument,
        for example:
          [ ("MyChannel.CHAN1", [MyParam.PARAM1, MyParam.PARAM4]),
            ("MyChannel.CHAN3", [MyParam.PARAM5]),
          ]
        The list-per-channel allows the caller to pass such list of params in
        one single invocation to the corresponding protocol operation.
        """

        if params == None or not isinstance(params, (list, tuple)):
            raise RequiredParameterException()

        if len(params) == 0:
            raise RequiredParameterException()

        # my list of params excluding ALL
        plist = [p for p in self.instrument_parameters.list() if p !=
                DriverParameter.ALL]

        # my list of channels excluding ALL
        clist = [c for c in self.instrument_channels.list() if c !=
                DriverChannel.ALL]

        # Expand and remove "ALL" channel specifiers.
        params += [(c, parameter) for (channel, parameter) in params
            if channel == DriverChannel.ALL for c in clist]
        params = [(c, p) for (c, p) in params if c != DriverChannel.ALL]

        # Expand and remove "ALL" parameter specifiers.
        params += [(channel, p) for (channel, parameter) in params
            if parameter == DriverParameter.ALL for p in plist]
        params = [(c, p) for (c, p) in params if p != DriverParameter.ALL]

        # Make list unique.
        params = list(set(params))

        # Separate invalid params.
        invalid_params = [(c, p) for (c, p) in params if c in clist and p not in plist]
        invalid_channels = [(c, p) for (c, p) in params if c not in clist]

        # get valid params:
        chan_params_map = {}
        for (c, p) in params:
            if c in clist and p in plist:
                pp = chan_params_map.get(c, [])
                pp.append(p)
                chan_params_map[c] = pp
        valid_params = chan_params_map.items()

        # Build result
        result = {}
        for (c, p) in invalid_params:
            result[(c, p)] = InstErrorCode.INVALID_PARAMETER
        for (c, p) in invalid_channels:
            result[(c, p)] = InstErrorCode.INVALID_CHANNEL

        return (result, valid_params)

    def _check_set_args(self, params):
        """
        Checks the params arguments that are supplied.

        @param params A dict of (c,p):v entries where c and p are channels and
        parameters in the self.instrument_channels and
        self.instrument_parameters lists respectively,
        and v is the desired value for such (c,p) pair. For example:
          {
            ("MyChannel.CHAN1", MyParam.PARAM1): 123,
            ("MyChannel.CHAN1", MyParam.PARAM4): 456,
            ("MyChannel.CHAN3", MyParam.PARAM5): 789,
          }

        @retval A tuple (result, valid_params) where:

        result: an empty dict if the given params arguments is valid;
        otherwise something like:
          { ("bad_channel", "whatever") : InstErrorCode.INVALID_CHANNEL }

        valid_params: a dict of c:pp entries where pp is the dict of p:v
        entries for each parameter p and desired value v associated to channel
        c in the given params argument,
        for example:
          { "MyChannel.CHAN1": {MyParam.PARAM1: 123, MyParam.PARAM4: 456},
            "MyChannel.CHAN3": {MyParam.PARAM5: 789},
          }
        The dict-per-channel allows the caller to pass such dict in
        one single invocation to the corresponding protocol operation.
        """
        #
        # NOTE: validation of values NOT done here; that can be a protocol's
        # responsibility.
        #

        if params == None or not isinstance(params, dict):
            raise RequiredParameterException()

        if len(params) == 0:
            raise RequiredParameterException()

        # my list of params excluding ALL
        plist = [p for p in self.instrument_parameters.list() if p !=
                DriverParameter.ALL]

        # my list of channels excluding ALL
        clist = [c for c in self.instrument_channels.list() if c !=
                DriverChannel.ALL]

        result = {}

        # c_map = { c: [(p1,v1), (p2,v2)...], ... }, will help check for
        # duplicate parameters per channel
        c_map = {}
        for (channel, param) in params.keys():
            value = params[(channel, param)]

            if param == DriverParameter.ALL:
                params_for_channels = plist
            elif not param in plist:
                result[(channel, param)] = InstErrorCode.INVALID_PARAMETER
                continue
            else:
                params_for_channels = [param]

            if channel == DriverChannel.ALL:
                channels = clist
            elif not channel in clist:
                result[(channel, param)] = InstErrorCode.INVALID_CHANNEL
                continue
            else:
                channels = [channel]

            for c in channels:
                # update the list for each channel in this pass:
                pp = c_map.get(c, [])
                for p in params_for_channels:
                    pp.append((p, value))
                c_map[c] = pp

        # finally, while checking for duplicate parameters-per-channel,
        # construct valid_params based on c_map, but this time with a
        # dict for each channel:
        # valid_params = { c: {p1:v1, p2:v2, ...}, ... }
        valid_params = {}
        for c in c_map.keys():
            # check for any duplicate parameters per channel:
            pp = c_map[c]
            ps, vs = zip(*pp)
            if len(ps) > len(set(ps)):
                # pick any of the duplicate parameters to report error:
                dups = list(ps)
                for p in list(set(ps)):
                    dups.remove(p)
                bad_param = dups[0]
                result[(c, bad_param)] = InstErrorCode.DUPLICATE_PARAMETER
            else:
                pv_dict = {}
                for (p, v) in pp:
                    pv_dict[p] = v
                valid_params[c] = pv_dict

        return (result, valid_params)

    #######################
    # State change handlers
    #######################
    def _handler_connected_disconnect(self, *args, **kwards):
        """Handle connected state with disconnect event"""
        return (ConnectionState.DISCONNECTED, None)
        
    def _handler_disconnected_connect(self, *args, **kwards):
        """Handle connected state with disconnect event"""
        return(ConnectionState.CONNECTED, None)
        
    def _handler_unknown_disconnect(self, *args, **kwards):
        """Handle connected state with disconnect event"""
        return (ConnectionState.DISCONNECTED, None)
        
    def _handler_unknown_connect(self, *args, **kwards):
        """Handle connected state with disconnect event"""
        return(ConnectionState.CONNECTED, None)