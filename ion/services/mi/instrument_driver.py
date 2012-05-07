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

import time
import logging

from ion.services.mi.common import BaseEnum
from ion.services.mi.exceptions import NotImplementedException 
from ion.services.mi.exceptions import InstrumentException
from ion.services.mi.exceptions import InstrumentParameterException
from ion.services.mi.logger_process import LoggerClient
from ion.services.mi.instrument_fsm import InstrumentFSM


#import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')

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
    POLL = 'DRIVER_STATE_POLL'

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
    BREAK = 'DRIVER_EVENT_BREAK'

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
        @raises InstrumentStateException if command not allowed in current state        
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('initialize() not implemented.')
        
    def configure(self, *args, **kwargs):
        """
        Configure the driver for communications with the device via
        port agent / logger (valid but unconnected connection object).
        @param arg[0] comms config dict.
        @raises InstrumentStateException if command not allowed in current state        
        @throws InstrumentParameterException if missing comms or invalid config dict.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('configure() not implemented.')
        
    def connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger
        (connected connection object).
        @raises InstrumentStateException if command not allowed in current state
        @throws InstrumentConnectionException if the connection failed.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('connect() not implemented.')
    
    def disconnect(self, *args, **kwargs):
        """
        Disconnect from device via port agent / logger.
        @raises InstrumentStateException if command not allowed in current state
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('disconnect() not implemented.')

    #############################################################
    # Command and control interface.
    #############################################################

    def discover(self, *args, **kwargs):
        """
        Determine initial state upon establishing communications.
        @param timeout=timeout Optional command timeout.        
        @retval Current device state.
        @raises InstrumentTimeoutException if could not wake device.
        @raises InstrumentStateException if command not allowed in current state or if
        device state not recognized.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('discover() is not implemented.')

    def get(self, *args, **kwargs):
        """
        Retrieve device parameters.
        @param args[0] DriverParameter.ALL or a list of parameters to retrive.
        @retval parameter : value dict.
        @raises InstrumentParameterException if missing or invalid get parameters.
        @raises InstrumentStateException if command not allowed in current state
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('get() is not implemented.')

    def set(self, *args, **kwargs):
        """
        Set device parameters.
        @param args[0] parameter : value dict of parameters to set.
        @param timeout=timeout Optional command timeout.
        @raises InstrumentParameterException if missing or invalid set parameters.
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if set command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.
        """
        raise NotImplementedException('set() not implemented.')

    def execute_acquire_sample(self, *args, **kwargs):
        """
        Poll for a sample.
        @param timeout=timeout Optional command timeout.        
        @ retval Device sample dict.
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if acquire command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.        
        """
        raise NotImplementedException('execute_acquire_sample() not implemented.')

    def execute_start_autosample(self, *args, **kwargs):
        """
        Switch to autosample mode.
        @param timeout=timeout Optional command timeout.        
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                
        """
        raise NotImplementedException('execute_start_autosample() not implemented.')

    def execute_stop_autosample(self, *args, **kwargs):
        """
        Leave autosample mode.
        @param timeout=timeout Optional command timeout.        
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if stop command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                
        """
        raise NotImplementedException('execute_stop_autosample() not implemented.')

    def execute_test(self, *args, **kwargs):
        """
        Execute device tests.
        @param timeout=timeout Optional command timeout (for wakeup only --
        device specific timeouts for internal test commands).
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if test commands not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                        
        """
        raise NotImplementedException('execute_test() not implemented.')

    def execute_calibrate(self, *args, **kwargs):
        """
        Execute device calibration.
        @param timeout=timeout Optional command timeout (for wakeup only --
        device specific timeouts for internal calibration commands).
        @riases InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if test commands not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                        
        """
        raise NotImplementedException('execute_calibrate() not implemented.')

    def execute_direct(self, *args, **kwargs):
        """
        """
        raise NotImplementedException('execute_direct() not implemented.')

    ########################################################################
    # Resource query interface.
    ########################################################################    

    def get_resource_commands(self):
        """
        Retrun list of device execute commands available.
        """
        cmds = [cmd for cmd in dir(self) if cmd.startswith('execute_')]
        cmds = [item.replace('execute_','') for item in cmds]
        return cmds
    
    def get_resource_params(self):
        """
        Return list of device parameters available. Implemented in
        device specific subclass.
        """
        raise NotImplementedError('get_resource_params() is not implemented.')

    def get_current_state(self):
        """
        Return current device state. Implemented in connection specific
        subclasses.
        """
        raise NotImplementedException('get_current_state() is not implemented.')

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
        
class SingleConnectionInstrumentDriver(InstrumentDriver):
    """
    Base class for instrument drivers with a single device connection.
    Provides connenction state logic for single connection drivers. This is
    the base class for the majority of driver implementation classes.
    """
    
    def __init__(self, event_callback):
        """
        Constructor for singly connected instrument drivers.
        @param event_callback Callback to the driver process to send asynchronous
        driver events back to the agent.
        """
        InstrumentDriver.__init__(self, event_callback)
        
        # The only and only instrument connection.
        # Exists in the connected state.
        self._connection = None

        # The one and only instrument protocol.
        self._protocol = None
        
        # Build connection state machine.
        self._connection_fsm = InstrumentFSM(DriverConnectionState,
                                                DriverEvent,
                                                DriverEvent.ENTER,
                                                DriverEvent.EXIT)
        
        # Add handlers for all events.
        self._connection_fsm.add_handler(DriverConnectionState.UNCONFIGURED, DriverEvent.ENTER, self._handler_unconfigured_enter)
        self._connection_fsm.add_handler(DriverConnectionState.UNCONFIGURED, DriverEvent.EXIT, self._handler_unconfigured_exit)
        self._connection_fsm.add_handler(DriverConnectionState.UNCONFIGURED, DriverEvent.INITIALIZE, self._handler_unconfigured_initialize)
        self._connection_fsm.add_handler(DriverConnectionState.UNCONFIGURED, DriverEvent.CONFIGURE, self._handler_unconfigured_configure)
        self._connection_fsm.add_handler(DriverConnectionState.DISCONNECTED, DriverEvent.ENTER, self._handler_disconnected_enter)
        self._connection_fsm.add_handler(DriverConnectionState.DISCONNECTED, DriverEvent.EXIT, self._handler_disconnected_exit)
        self._connection_fsm.add_handler(DriverConnectionState.DISCONNECTED, DriverEvent.INITIALIZE, self._handler_disconnected_initialize)
        self._connection_fsm.add_handler(DriverConnectionState.DISCONNECTED, DriverEvent.CONFIGURE, self._handler_disconnected_configure)
        self._connection_fsm.add_handler(DriverConnectionState.DISCONNECTED, DriverEvent.CONNECT, self._handler_disconnected_connect)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.ENTER, self._handler_connected_enter)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.EXIT, self._handler_connected_exit)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.DISCONNECT, self._handler_connected_disconnect)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.CONNECTION_LOST, self._handler_connected_connection_lost)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.DISCOVER, self._handler_connected_protocol_event)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.GET, self._handler_connected_protocol_event)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.SET, self._handler_connected_protocol_event)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.ACQUIRE_SAMPLE, self._handler_connected_protocol_event)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.START_AUTOSAMPLE, self._handler_connected_protocol_event)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.STOP_AUTOSAMPLE, self._handler_connected_protocol_event)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.TEST, self._handler_connected_protocol_event)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.CALIBRATE, self._handler_connected_protocol_event)
        self._connection_fsm.add_handler(DriverConnectionState.CONNECTED, DriverEvent.EXECUTE_DIRECT, self._handler_connected_protocol_event)
            
        # Start state machine.
        self._connection_fsm.start(DriverConnectionState.UNCONFIGURED)
                
    #############################################################
    # Device connection interface.
    #############################################################
    
    def initialize(self, *args, **kwargs):
        """
        Initialize driver connection, bringing communications parameters
        into unconfigured state (no connection object).
        @raises InstrumentStateException if command not allowed in current state        
        """
        # Forward event and argument to the connection FSM.
        return self._connection_fsm.on_event(DriverEvent.INITIALIZE, *args, **kwargs)
        
    def configure(self, *args, **kwargs):
        """
        Configure the driver for communications with the device via
        port agent / logger (valid but unconnected connection object).
        @param arg[0] comms config dict.
        @raises InstrumentStateException if command not allowed in current state        
        @throws InstrumentParameterException if missing comms or invalid config dict.
        """
        # Forward event and argument to the connection FSM.
        return self._connection_fsm.on_event(DriverEvent.CONFIGURE, *args, **kwargs)
        
    def connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger
        (connected connection object).
        @raises InstrumentStateException if command not allowed in current state
        @throws InstrumentConnectionException if the connection failed.
        """
        # Forward event and argument to the connection FSM.
        return self._connection_fsm.on_event(DriverEvent.CONNECT, *args, **kwargs)
    
    def disconnect(self, *args, **kwargs):
        """
        Disconnect from device via port agent / logger.
        @raises InstrumentStateException if command not allowed in current state
        """
        # Forward event and argument to the connection FSM.
        return self._connection_fsm.on_event(DriverEvent.DISCONNECT, *args, **kwargs)

    #############################################################
    # Commande and control interface.
    #############################################################

    def discover(self, *args, **kwargs):
        """
        Determine initial state upon establishing communications.
        @param timeout=timeout Optional command timeout.        
        @retval Current device state.
        @raises InstrumentTimeoutException if could not wake device.
        @raises InstrumentStateException if command not allowed in current state or if
        device state not recognized.
        @raises NotImplementedException if not implemented by subclass.
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.DISCOVER, DriverEvent.DISCOVER, *args, **kwargs)

    def get(self, *args, **kwargs):
        """
        Retrieve device parameters.
        @param args[0] DriverParameter.ALL or a list of parameters to retrive.
        @retval parameter : value dict.
        @raises InstrumentParameterException if missing or invalid get parameters.
        @raises InstrumentStateException if command not allowed in current state
        @raises NotImplementedException if not implemented by subclass.                        
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.GET, DriverEvent.GET, *args, **kwargs)

    def set(self, *args, **kwargs):
        """
        Set device parameters.
        @param args[0] parameter : value dict of parameters to set.
        @param timeout=timeout Optional command timeout.
        @raises InstrumentParameterException if missing or invalid set parameters.
        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if set command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                        
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.SET, DriverEvent.SET, *args, **kwargs)

    def execute_acquire_sample(self, *args, **kwargs):
        """
        Poll for a sample.
        @param timeout=timeout Optional command timeout.        
        @ retval Device sample dict.
        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if acquire command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                        
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.ACQUIRE_SAMPLE, DriverEvent.ACQUIRE_SAMPLE, *args, **kwargs)

    def execute_start_autosample(self, *args, **kwargs):
        """
        Switch to autosample mode.
        @param timeout=timeout Optional command timeout.        
        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                        
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.START_AUTOSAMPLE, DriverEvent.START_AUTOSAMPLE, *args, **kwargs)

    def execute_stop_autosample(self, *args, **kwargs):
        """
        Leave autosample mode.
        @param timeout=timeout Optional command timeout.        
        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if stop command not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                        
         """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.STOP_AUTOSAMPLE, DriverEvent.STOP_AUTOSAMPLE, *args, **kwargs)

    def execute_test(self, *args, **kwargs):
        """
        Execute device tests.
        @param timeout=timeout Optional command timeout (for wakeup only --
        device specific timeouts for internal test commands).
        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if test commands not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                        
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.TEST, DriverEvent.TEST, *args, **kwargs)

    def execute_calibrate(self, *args, **kwargs):
        """
        Execute device calibration.
        @param timeout=timeout Optional command timeout (for wakeup only --
        device specific timeouts for internal calibration commands).
        @raises InstrumentTimeoutException if could not wake device or no response.
        @raises InstrumentProtocolException if test commands not recognized.
        @raises InstrumentStateException if command not allowed in current state.
        @raises NotImplementedException if not implemented by subclass.                        
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.CALIBRATE, DriverEvent.CALIBRATE, *args, **kwargs)

    def execute_direct(self, *args, **kwargs):
        """
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.EXECUTE_DIRECT, DriverEvent.EXECUTE_DIRECT, *args, **kwargs)


    ########################################################################
    # Resource query interface.
    ########################################################################

    def get_current_state(self):
        """
        Return current device state. For single connection devices, return
        a single connection state if not connected, and protocol state if connected.
        """
        connection_state = self._connection_fsm.get_current_state()
        if connection_state == DriverConnectionState.CONNECTED:
            return self._protocol.get_current_state()
        else:
            return connection_state

    ########################################################################
    # Unconfigured handlers.
    ########################################################################

    def _handler_unconfigured_enter(self, *args, **kwargs):
        """
        Enter unconfigured state.
        """
        # Send state change event to agent.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
    
    def _handler_unconfigured_exit(self, *args, **kwargs):
        """
        Exit unconfigured state.
        """
        pass

    def _handler_unconfigured_initialize(self, *args, **kwargs):
        """
        Initialize handler. We are already in unconfigured state, do nothing.
        @retval (next_state, result) tuple, (None, None).
        """
        next_state = None
        result = None
        
        return (next_state, result)

    def _handler_unconfigured_configure(self, *args, **kwargs):
        """
        Configure driver for device comms.
        @param args[0] Communiations config dictionary.
        @retval (next_state, result) tuple, (DriverConnectionState.DISCONNECTED,
        None) if successful, (None, None) otherwise.
        @raises InstrumentParameterException if missing or invalid param dict.        
        """
        next_state = None
        result = None
        
        # Get the required param dict.
        try:
            config = args[0]
        
        except IndexError:
            raise InstrumentParameterException('Missing comms config parameter.')
        
        # Verify dict and construct connection client.
        try:
            addr = config['addr']
            port = config['port']
            
            if isinstance(addr, str) and isinstance(port, int) and len(addr)>0:                
                self._connection = LoggerClient(addr, port)
                next_state = DriverConnectionState.DISCONNECTED
                            
            else:
                raise InstrumentParameterException('Invalid comms config dict.')
                
        except (TypeError, KeyError):
            raise InstrumentParameterException('Invalid comms config dict.')        
        
        return (next_state, result)

    ########################################################################
    # Disconnected handlers.
    ########################################################################

    def _handler_disconnected_enter(self, *args, **kwargs):
        """
        Enter disconnected state.
        """
        # Send state change event to agent.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_disconnected_exit(self, *args, **kwargs):
        """
        Exit disconnected state.
        """
        pass

    def _handler_disconnected_initialize(self, *args, **kwargs):
        """
        Initialize device communications. Causes the connection parameters to
        be reset.
        @retval (next_state, result) tuple, (DriverConnectionState.UNCONFIGURED,
        None).
        """
        next_state = None
        result = None
        
        self._connection = None
        next_state = DriverConnectionState.UNCONFIGURED
        
        return (next_state, result)

    def _handler_disconnected_configure(self, *args, **kwargs):
        """
        Configure driver for device comms.
        @param args[0] Communiations config dictionary.
        @retval (next_state, result) tuple, (None, None).
        @raises InstrumentParameterException if missing or invalid param dict.
        """
        next_state = None
        result = None
        
        # Get required config param dict.
        try:
            config = args[0]
        
        except IndexError:
            raise InstrumentParameterException('Missing comms config parameter.')

        # Verify configuration dict, and update connection if possible.        
        try:
            addr = config['addr']
            port = config['port']
            
            if isinstance(addr, str) and isinstance(port, int) and len(addr)>0:                
                self._connection = LoggerClient(addr, port)
                            
            else:
                raise InstrumentParameterException('Invalid comms config dict.')
                
        except (TypeError, KeyError):
            raise InstrumentParameterException('Invalid comms config dict.')        
        
        return (next_state, result)

    def _handler_disconnected_connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger and
        construct and intialize a protocol FSM for device interaction.
        @retval (next_state, result) tuple, (DriverConnectionState.CONNECTED,
        None) if successful.
        @raises InstrumentConnectionException if the attempt to connect failed.
        """
        next_state = None
        result = None
        
        self._build_protocol()
        self._connection.init_comms(self._protocol.got_data)
        self._protocol._connection = self._connection
        next_state = DriverConnectionState.CONNECTED
        
        return (next_state, result)

    ########################################################################
    # Connected handlers.
    ########################################################################

    def _handler_connected_enter(self, *args, **kwargs):
        """
        Enter connected state.
        """
        # Send state change event to agent.
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)

    def _handler_connected_exit(self, *args, **kwargs):
        """
        Exit connected state.
        """
        pass

    def _handler_connected_disconnect(self, *args, **kwargs):
        """
        Disconnect to the device via port agent / logger and destroy the
        protocol FSM.
        @retval (next_state, result) tuple, (DriverConnectionState.DISCONNECTED,
        None) if successful.
        """
        next_state = None
        result = None
        
        self._connection.stop_comms()
        self._protocol = None
        next_state = DriverConnectionState.DISCONNECTED
        
        return (next_state, result)

    def _handler_connected_connection_lost(self, *args, **kwargs):
        """
        The device connection was lost. Stop comms, destroy protocol FSM and
        revert to disconnected state.
        @retval (next_state, result) tuple, (DriverConnectionState.DISCONNECTED,
        None).
        """
        next_state = None
        result = None

        self._connection.stop_comms()
        self._protocol = None
        next_state = DriverConnectionState.DISCONNECTED
        
        return (next_state, result)

    def _handler_connected_protocol_event(self, event, *args, **kwargs):
        """
        Forward a driver command event to the protocol FSM.
        @param args positional arguments to pass on.
        @param kwargs keyword arguments to pass on.
        @retval (next_state, result) tuple, (None, protocol result).
        """
        next_state = None
        result = self._protocol._protocol_fsm.on_event(event, *args, **kwargs)
        
        return (next_state, result)


    ########################################################################
    # Helpers.
    ########################################################################

    def _build_protocol(self):
        """
        Construct device specific single connection protocol FSM.
        Overridden in device specific subclasses.
        """
        pass
