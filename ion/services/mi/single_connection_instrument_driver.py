#!/usr/bin/env python

"""
@package ion.services.mi.single_connection_instrument_driver 
@file ion/services/mi/single_connection_instrument_driver.py
@author Edward Hunter
@brief Instrument driver base class that provides state model for
devices with a single connection. Base class for the majority of
driver implementation classes.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import logging

from ion.services.mi.common import BaseEnum
from ion.services.mi.exceptions import NotImplementedError
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverEvent
from ion.services.mi.instrument_driver import DriverConnectionState
from ion.services.mi.instrument_driver import DriverAsyncEvent
from ion.services.mi.instrument_fsm import InstrumentFSM
from ion.services.mi.logger_process import LoggerClient
from ion.services.mi.exceptions import ParameterError

#import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')

class SingleConnectionInstrumentDriver(InstrumentDriver):
    """
    Base class for instrument drivers with a single device connection.
    Provides connenction state logic for single connection drivers.
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
        @raises StateError if command not allowed in current state        
        """
        # Forward event and argument to the connection FSM.
        return self._connection_fsm.on_event(DriverEvent.INITIALIZE, *args, **kwargs)
        
    def configure(self, *args, **kwargs):
        """
        Configure the driver for communications with the device via
        port agent / logger (valid but unconnected connection object).
        @param arg[0] comms config dict.
        @raises StateError if command not allowed in current state        
        @throws ParameterError if missing comms or invalid config dict.
        """
        # Forward event and argument to the connection FSM.
        return self._connection_fsm.on_event(DriverEvent.CONFIGURE, *args, **kwargs)
        
    def connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger
        (connected connection object).
        @raises StateError if command not allowed in current state
        @throws ConnectionError if the connection failed.
        """
        # Forward event and argument to the connection FSM.
        return self._connection_fsm.on_event(DriverEvent.CONNECT, *args, **kwargs)
    
    def disconnect(self, *args, **kwargs):
        """
        Disconnect from device via port agent / logger.
        @raises StateError if command not allowed in current state
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
        @raises TimeoutError if could not wake device.
        @raises StateError if command not allowed in current state or if
        device state not recognized.
        @raises NotImplementedError if not implemented by subclass.
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.DISCOVER, DriverEvent.DISCOVER, *args, **kwargs)

    def get(self, *args, **kwargs):
        """
        Retrieve device parameters.
        @param args[0] DriverParameter.ALL or a list of parameters to retrive.
        @retval parameter : value dict.
        @raises ParameterError if missing or invalid get parameters.
        @raises StateError if command not allowed in current state
        @raises NotImplementedError if not implemented by subclass.                        
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.GET, DriverEvent.GET, *args, **kwargs)

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
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.SET, DriverEvent.SET, *args, **kwargs)

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
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.ACQUIRE_SAMPLE, DriverEvent.ACQUIRE_SAMPLE, *args, **kwargs)

    def execute_start_autosample(self, *args, **kwargs):
        """
        Switch to autosample mode.
        @param timeout=timeout Optional command timeout.        
        @riases TimeoutError if could not wake device or no response.
        @raises StateError if command not allowed in current state.
        @raises NotImplementedError if not implemented by subclass.                        
        """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.START_AUTOSAMPLE, DriverEvent.START_AUTOSAMPLE, *args, **kwargs)

    def execute_stop_autosample(self, *args, **kwargs):
        """
        Leave autosample mode.
        @param timeout=timeout Optional command timeout.        
        @riases TimeoutError if could not wake device or no response.
        @raises ProtocolError if stop command not recognized.
        @raises StateError if command not allowed in current state.
        @raises NotImplementedError if not implemented by subclass.                        
         """
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.STOP_AUTOSAMPLE, DriverEvent.STOP_AUTOSAMPLE, *args, **kwargs)

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
        # Forward event and argument to the protocol FSM.
        return self._connection_fsm.on_event(DriverEvent.TEST, DriverEvent.TEST, *args, **kwargs)

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
        @raises Parameter error if missing or invalid param dict.        
        """
        next_state = None
        result = None
        
        # Get the required param dict.
        try:
            config = args[0]
        
        except IndexError:
            raise ParameterError('Missing comms config parameter.')
        
        # Verify dict and construct connection client.
        try:
            addr = config['addr']
            port = config['port']
            
            if isinstance(addr, str) and isinstance(port, int) and len(addr)>0:                
                self._connection = LoggerClient(addr, port)
                next_state = DriverConnectionState.DISCONNECTED
                            
            else:
                raise ParameterError('Invalid comms config dict.')
                
        except (TypeError, KeyError):
            raise ParameterError('Invalid comms config dict.')        
        
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
        @raises Parameter error if missing or invalid param dict.
        """
        next_state = None
        result = None
        
        # Get required config param dict.
        try:
            config = args[0]
        
        except IndexError:
            raise ParameterError('Missing comms config parameter.')

        # Verify configuration dict, and update connection if possible.        
        try:
            addr = config['addr']
            port = config['port']
            
            if isinstance(addr, str) and isinstance(port, int) and len(addr)>0:                
                self._connection = LoggerClient(addr, port)
                            
            else:
                raise ParameterError('Invalid comms config dict.')
                
        except (TypeError, KeyError):
            raise ParameterError('Invalid comms config dict.')        
        
        return (next_state, result)

    def _handler_disconnected_connect(self, *args, **kwargs):
        """
        Establish communications with the device via port agent / logger and
        construct and intialize a protocol FSM for device interaction.
        @retval (next_state, result) tuple, (DriverConnectionState.CONNECTED,
        None) if successful.
        @raises ConnectionError if the attempt to connect failed.
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
    