#!/usr/bin/env python

"""
@package ion.services.mi.instrument_driver Instrument driver structures
@file ion/services/mi/instrument_driver.py
@author Edward Hunter
@brief Instrument driver classes that provide structure towards interaction
with individual instruments in the system.
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

from ion.services.mi.common import BaseEnum
from ion.services.mi.exceptions import NotImplementedError 
from ion.services.mi.exceptions import InstrumentException
import time

class DriverProtocolState(BaseEnum):
    """
    Base states for driver protocols. Subclassed for specific driver
    protocols.
    """
    AUTOSAMPLE = 'DRIVER_STATE_AUTOSAMPLE'
    TEST = 'DRIVER_STATE_TEST'
    CALIBRATE = 'DRIVER_STATE_CALIBRATE'
    COMMAND = 'DRIVER_STATE_COMMAND'
    DIRECT_ACCESS = 'DRIVER_STATE_DIRECT_ACCESS'
    UNKNOWN = 'DRIVER_STATE_UNKNOWN'

class DriverConnectionState(BaseEnum):
    """
    Base states for driver connections.
    """
    UNCONFIGURED = 'DRIVER_STATE_UNCONFIGURED'
    DISCONNECTED = 'DRIVER_STATE_DISCONNECTED'
    CONNECTED = 'DRIVER_STATE_CONNECTED'
    
class DriverEvent(BaseEnum):
    """
    Base events for driver state machines. Commands and other events
    are transformed into state machine events for handling.
    """
    CONFIGURE = 'DRIVER_EVENT_CONFIGURE'
    INITIALIZE = 'DRIVER_EVENT_INITIALIZE'
    CONNECT = 'DRIVER_EVENT_CONNECT'
    CONNECTION_LOST = 'DRIVER_CONNECTION_LOST'
    DISCONNECT = 'DRIVER_EVENT_DISCONNECT'
    SET = 'DRIVER_EVENT_SET'
    GET = 'DRIVER_EVENT_GET'
    DISCOVER = 'DRIVER_EVENT_DISCOVER'
    EXECUTE = 'DRIVER_EVENT_EXECUTE'
    EXECUTE_DIRECT = 'DRIVER_EVENT_EXECUTE_DIRECT'
    ACQUIRE_SAMPLE = 'DRIVER_EVENT_ACQUIRE_SAMPLE'
    START_AUTOSAMPLE = 'DRIVER_EVENT_START_AUTOSAMPLE'
    STOP_AUTOSAMPLE = 'DRIVER_EVENT_STOP_AUTOSAMPLE'
    TEST = 'DRIVER_EVENT_TEST'
    RUN_TEST = 'DRIVER_EVENT_RUN_TEST'
    STOP_TEST = 'DRIVER_EVENT_STOP_TEST'
    CALIBRATE = 'DRIVER_EVENT_CALIBRATE'
    RESET = 'DRIVER_EVENT_RESET'
    ENTER = 'DRIVER_EVENT_ENTER'
    EXIT = 'DRIVER_EVENT_EXIT'
    UPDATE_PARAMS = 'DRIVER_EVENT_UPDATE_PARAMS'

class DriverAsyncEvent(BaseEnum):
    """
    Asynchronous driver event types.
    """
    STATE_CHANGE = 'DRIVER_ASYNC_EVENT_STATE_CHANGE'
    CONFIG_CHANGE = 'DRIVER_ASYNC_EVENT_CONFIG_CHANGE'
    SAMPLE = 'DRIVER_ASYNC_EVENT_SAMPLE'
    ERROR = 'DRIVER_ASYNC_EVENT_ERROR'
    TEST_RESULT = 'DRIVER_ASYNC_TEST_RESULT'

class DriverParameter(BaseEnum):
    """
    Base driver parameters. Subclassed by specific drivers with device
    specific parameters.
    """
    ALL = 'DRIVER_PARAMETER_ALL'

class InstrumentDriver(object):
    """
    Base class for instrument drivers.
    """
    
    def __init__(self, event_callback):
        """
        Constructor.
        @param event_callback The driver process callback used to send
        asynchrous driver events to the agent.
        """
        self._send_event = event_callback

    #############################################################
    # Device connection interface.
    #############################################################
    
    def initialize(self, *args, **kwargs):
        """
        Initialize driver connection, bringing communications parameters
        into unconfigured state (no connection object).
        @raises StateError if command not allowed in current state        
        @raises NotImplementedError if not implemented by subclass.
        """
        raise NotImplementedError('initialize() not implemented.')
        
    def configure(self, *args, **kwargs):
        """
        Configure the driver for communications with the device via
        port agent / logger (valid but unconnected connection object).
        @param arg[0] comms config dict.
        @raises StateError if command not allowed in current state        
        @throws ParameterError if missing comms or invalid config dict.
        @raises NotImplementedError if not implemented by subclass.
        """
        raise NotImplementedError('configure() not implemented.')
        
    def connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger
        (connected connection object).
        @raises StateError if command not allowed in current state
        @throws ConnectionError if the connection failed.
        @raises NotImplementedError if not implemented by subclass.
        """
        raise NotImplementedError('connect() not implemented.')
    
    def disconnect(self, *args, **kwargs):
        """
        Disconnect from device via port agent / logger.
        @raises StateError if command not allowed in current state
        @raises NotImplementedError if not implemented by subclass.
        """
        raise NotImplementedError('disconnect() not implemented.')

    #############################################################
    # Command and control interface.
    #############################################################

    def discover(self, *args, **kwargs):
        """
        Determine initial state upon establishing communications.
        @param timeout=timeout Optional command timeout.        
        @retval Current device state.
        @raises TimeoutError if could not wake device.
        @raises StateError if command not allowed in current state or if
        device state not recognized.
        @raises NotImplementedError if not implemented by subclass.
        """
        raise NotImplementedError('discover() is not implemented.')

    def get(self, *args, **kwargs):
        """
        Retrieve device parameters.
        @param args[0] DriverParameter.ALL or a list of parameters to retrive.
        @retval parameter : value dict.
        @raises ParameterError if missing or invalid get parameters.
        @raises StateError if command not allowed in current state
        @raises NotImplementedError if not implemented by subclass.
        """
        raise NotImplementedError('get() is not implemented.')

    def set(self, *args, **kwargs):
        """
        Set device parameters.
        @param args[0] parameter : value dict of parameters to set.
        @param timeout=timeout Optional command timeout.
        @raises ParameterError if missing or invalid set parameters.
        @riases TimeoutError if could not wake device or no response.
        @raises ProtocolError if set command not recognized.
        @raises StateError if command not allowed in current state.
        @raises NotImplementedError if not implemented by subclass.
        """
        raise NotImplementedError('set() not implemented.')

    def execute_acquire_sample(self, *args, **kwargs):
        """
        Poll for a sample.
        @param timeout=timeout Optional command timeout.        
        @ retval Device sample dict.
        @riases TimeoutError if could not wake device or no response.
        @raises ProtocolError if acquire command not recognized.
        @raises StateError if command not allowed in current state.
        @raises NotImplementedError if not implemented by subclass.        
        """
        raise NotImplementedError('execute_acquire_sample() not implemented.')

    def execute_start_autosample(self, *args, **kwargs):
        """
        Switch to autosample mode.
        @param timeout=timeout Optional command timeout.        
        @riases TimeoutError if could not wake device or no response.
        @raises StateError if command not allowed in current state.
        @raises NotImplementedError if not implemented by subclass.                
        """
        raise NotImplementedError('execute_start_autosample() not implemented.')

    def execute_stop_autosample(self, *args, **kwargs):
        """
        Leave autosample mode.
        @param timeout=timeout Optional command timeout.        
        @riases TimeoutError if could not wake device or no response.
        @raises ProtocolError if stop command not recognized.
        @raises StateError if command not allowed in current state.
        @raises NotImplementedError if not implemented by subclass.                
        """
        raise NotImplementedError('execute_stop_autosample() not implemented.')

    def execute_test(self, *args, **kwargs):
        """
        Execute device tests.
        @param timeout=timeout Optional command timeout (for wakeup only --
        device specific timeouts for internal test commands).
        @riases TimeoutError if could not wake device or no response.
        @raises ProtocolError if test commands not recognized.
        @raises StateError if command not allowed in current state.
        @raises NotImplementedError if not implemented by subclass.                        
        """
        raise NotImplementedError('execute_test() not implemented.')

    def execute_calibrate(self, *args, **kwargs):
        """
        Execute device calibration.
        @param timeout=timeout Optional command timeout (for wakeup only --
        device specific timeouts for internal calibration commands).
        @riases TimeoutError if could not wake device or no response.
        @raises ProtocolError if test commands not recognized.
        @raises StateError if command not allowed in current state.
        @raises NotImplementedError if not implemented by subclass.                        
        """
        raise NotImplementedError('execute_calibrate() not implemented.')

    def execute_direct(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('execute_direct() not implemented.')

    ########################################################################
    # Resource query interface.
    ########################################################################    
    
    def get_resource_commands(self):
        """
        Retrun list of device execute commands available.
        """
        return [cmd for cmd in dir(self) if cmd.startswith('execute_')]    
    
    def get_resource_params(self):
        """
        Return list of device parameters available.
        """
        return self.get(DriverParameter.ALL)
            
    def get_current_state(self):
        """
        Return current device state. Implemented in connection specific
        subclasses.
        """
        raise NotImplementedError('get_current_state() is not implemented.')

    ########################################################################
    # Event interface.
    ########################################################################

    def _driver_event(self, type, val=None):
        """
        Construct and send an asynchronous driver event.
        @param type a DriverAsyncEvent type specifier.
        @param val event value for sample and test result events.
        """
        event = {
            'type' : type,
            'value' : None,
            'time' : time.time()
        }
        if type == DriverAsyncEvent.STATE_CHANGE:
            state = self.get_current_state()
            event['value'] = state
            self._send_event(event)
            
        elif type == DriverAsyncEvent.CONFIG_CHANGE:
            config = self.get(DriverParameter.ALL)
            event['value'] = config
            self._send_event(event)
        
        elif type == DriverAsyncEvent.SAMPLE:
            event['value'] = val
            self._send_event(event)
            
        elif type == DriverAsyncEvent.ERROR:
            # Error caught at driver process level.
            pass

        elif type == DriverAsyncEvent.TEST_RESULT:
            event['value'] = val
            self._send_event(event)

    ########################################################################
    # Test interface.
    ########################################################################

    def driver_echo(self, msg):
        """
        Echo a message.
        @param msg the message to prepend and echo back to the caller.
        """
        reply = 'driver_echo: '+msg
        return reply
    
    def test_exceptions(self, msg):
        """
        Test exception handling in the driver process.
        @param msg message string to put in a raised exception to be caught in
        a test.
        @raises InstrumentExeption always.
        """
        raise InstrumentException(msg)
        