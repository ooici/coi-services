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

    def __init__(self):

        # Instrument channel dict.
        # A dictionary of channel-name keys and channel protocol object values.
        self.instrument_channels = {}
    
    def configure(self, config):
        """
        Configure the driver for communications with an instrument channel.
        @param config A dict containing channel name keys, with
        dict values containing the comms configuration for the named channel.
        """
        pass
    
    def initialize(self, chan_list):
        """
        Return a device channel to an unconnected, unconfigured state.
        @param chan_list List of channel names to initialize.
        """
        pass
    
    def connect(self, chan_list):
        """
        Establish communications with a device channel.
        @param chan_list List of channel names to connect.
        """
        pass
    
    def disconnect(self, chan_list):
        """
        Disconnect communications with a device channel.
        @param chan_list List of channel names to disconnect.
        """
        pass

    def execute(self, channels, command, timeout):
        """
        """
        pass
    
    def execute_direct(self, bytes, timeout):
        """
        """
        pass
    
    def get(self, params, timeout):
        """
        """
        pass
    
    def set(self, params, timeout):
        """
        """
        pass
    
    def get_status(self, params, timeout):
        """
        """
        pass
    
    def get_capabilities(self, params, timeout):
        """
        """
        pass

