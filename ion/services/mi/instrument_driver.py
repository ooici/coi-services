#!/usr/bin/env python

"""
@package ion.services.mi.instrument_driver Instrument driver structures
@file ion/services/mi/instrument_driver.py
@author Steve Foley
@brief Instrument driver classes that provide structure towards interaction
with individual instruments in the system.
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

from ion.services.mi.common import BaseEnum
from ion.services.mi.exceptions import InstrumentConnectionException 
from ion.services.mi.common import DEFAULT_TIMEOUT
from pyon.ion.util.fsm import FSM

class DriverChannel(BaseEnum):
    """Common channels for all sensors. Driver subclasses contain a subset."""
    INSTRUMENT = 'CHANNEL_INSTRUMENT'
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

############
# End states
############

class InstrumentDriver(object):
    """The base instrument driver class
    
    This is intended to be extended where necessary to provide a coherent
    driver for interfacing between the instrument and the instrument agent.
    Think of this class as encapsulating the session layer of the instrument
    interaction.
    
    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+SA+SV+Instrument+Driver+Interface
    """

    def __init__(self):
        # Setup instance variables with instrument-specific instances.
        # Some may be fed from the instrument protocol subclass.
        
        self.instrument_connection = None
        """An object for manipulating connect and disconnect to an instrument"""
    
        self.instrument_protocol = None
        """The instrument-specific protocol object"""
    
        self.instrument_comms_method = None
        """The communications method formatting object"""
    
        self.instrument_commands = None
        """The instrument-specific command list"""
    
        self.instrument_metadata_parameters = None
        """The instrument-specific metadata parameter list"""
    
        self.instrument_parameters = None
        """The instrument-specific parameter list"""
    
        self.instrument_channels = None
        """The instrument-specific channel list"""
    
        self.instrument_errors = None
        """The instrument-specific error list"""
    
        self.instrument_capabilities = None
        """The instrument-specific capabilities list"""
    
        self.instrument_status = None
        """The instrument-specific status list"""
    
        # Setup the state machine
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

    def configure(self, params={}, timeout=DEFAULT_TIMEOUT):
        """Configure the driver's parameters
        
        Some parameters are needed soley by the driver to interact with an
        instrument. These parameters can be set here by the instantiating
        class, likely based on some registry entries for the individual
        instrument being interacted with 
        
        @param params A dictionary of the parameters for configuring the driver
        @param timeout Number of seconds before this operation times out
        """
        assert(isinstance(params, dict))
        
    def initialize(self, timeout=DEFAULT_TIMEOUT):
        """
        """
        
    def connect(self, timeout=DEFAULT_TIMEOUT):
        """ Connect to the device
        @param timeout Number of seconds before this operation times out
        @retval result Success/failure result
        @throws InstrumentConnectionException
        @todo determine result if already connected
        """
        # Something like self.InstrumentConnection.connect(), then set state
    
    def disconnect(self, timeout=DEFAULT_TIMEOUT):
        """
        @param timeout Number of seconds before this operation times out
        @retval result Success/failure result
        @throws InstrumentConnectionException
        @todo determine result if already disconnected
        """
        # Something like self.InstrumentConnection.disconnect(), then set state
           
           
    def execute(self, channels, command, timeout):
        """
        """
    
    def execute_direct(self, bytes, timeout):
        """
        """
    
    def get(self, params, timeout):
        """
        """
        
    def set(self, params, timeout):
        """
        """
        
    def get_metadata(self, params, timeout):
        """
        """
        
    def get_status(self, params, timeout):
        """
        """
        
    def get_capabilities(self, params, timeout):
        """
        """
    
    #######################
    # State change handlers
    #######################
    def _handle_configure(self):
        """State change handler"""
    
    def _handle_initialize(self):
        """State change handler"""
    
    def _handle_disconnect_failure(self):
        """State change handler"""

    def _handle_disconnect_success(self):
        """State change handler"""

    def _handle_disconnect(self):
        """State change handler"""

    def _handle_connect(self):
        """State change handler"""

    def _handle_connect_success(self):
        """State change handler"""

    def _handle_connect_failed(self):
        """State change handler"""

    def _handle_start_autosample(self):
        """State change handler"""

    def _handle_stop_autosample(self):
        """State change handler"""
    
    def _handle_connection_lost(self):
        """State change handler"""
    
    def _handle_reset(self):
        """State change handler"""

    def _handle_handle_data_received(self):
        """State change handler"""
        
    def _handle_handle_get(self):
        """State change handler"""
        
    def _handle_handle_set(self):
        """State change handler"""
    
    def _handle_handle_acquire_sample(self):
        """State change handler"""
        
    def _handle_handle_test(self):
        """State change handler"""
    
    def _handle_handle_calibrate(self):
        """State change handler"""
