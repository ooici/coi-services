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

from ion.services.mi.common import BaseEnum
from ion.services.mi.exceptions import NotImplementedError
from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverEvent
from ion.services.mi.instrument_driver import DriverConnectionState
from ion.services.mi.instrument_fsm import InstrumentFSM


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
        """
        return self._connection_fsm.on_event(DriverEvent.INITIALIZE, *args, **kwargs)
        
    def configure(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.CONFIGURE, *args, **kwargs)
        
    def connect(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.CONNECT, *args, **kwargs)
    
    def disconnect(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.DISCONNECT, *args, **kwargs)

    #############################################################
    # Commande and control interface.
    #############################################################

    def get(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.GET, DriverEvent.GET, *args, **kwargs)

    def set(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.SET, DriverEvent.SET, *args, **kwargs)

    def execute_acquire_sample(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.ACQUIRE_SAMPLE, DriverEvent.ACQUIRE_SAMPLE, *args, **kwargs)

    def execute_start_autosample(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.START_AUTOSAMPLE, DriverEvent.START_AUTOSAMPLE, *args, **kwargs)

    def execute_stop_autosample(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.STOP_AUTOSAMPLE, DriverEvent.STOP_AUTOSAMPLE, *args, **kwargs)

    def execute_test(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.TEST, DriverEvent.TEST, *args, **kwargs)

    def execute_calibrate(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.CALIBRATE, DriverEvent.CALIBRATE, *args, **kwargs)

    def execute_direct(self, *args, **kwargs):
        """
        """
        return self._connection_fsm.on_event(DriverEvent.EXECUTE_DIRECT, DriverEvent.EXECUTE_DIRECT, *args, **kwargs)


    ########################################################################
    # Resource query interface.
    ########################################################################

    def get_resource_params(self):
        """
        """
        return self._protocol._param_dict.get_keys()

    def get_current_state(self):
        """
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
        """
        self._driver_event('state_change')
    
    def _handler_unconfigured_exit(self, *args, **kwargs):
        """
        """
        pass

    def _handler_unconfigured_initialize(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)

    def _handler_unconfigured_initialize(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)

    def _handler_unconfigured_configure(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)

    ########################################################################
    # Disconnected handlers.
    ########################################################################

    def _handler_disconnected_enter(self, *args, **kwargs):
        """
        """
        self._driver_event('state_change')

    def _handler_disconnected_exit(self, *args, **kwargs):
        """
        """
        pass

    def _handler_disconnected_initialize(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)

    def _handler_disconnected_configure(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)

    def _handler_disconnected_connect(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)

    ########################################################################
    # Connected handlers.
    ########################################################################

    def _handler_connected_enter(self, *args, **kwargs):
        """
        """
        self._driver_event('state_change')
        # Send protocol event to discover state.

    def _handler_connected_exit(self, *args, **kwargs):
        """
        """
        pass

    def _handler_connected_disconnect(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)

    def _handler_connected_connection_lost(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        return (next_state, result)

    def _handler_connected_protocol_event(self, event, *args, **kwargs):
        """
        """
        next_state = None
        result = self._protocol._protocol_fsm.on_event(event, *args, **kwargs)
        
        return (next_state, result)

