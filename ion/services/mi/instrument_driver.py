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
from ion.services.mi.exceptions import NotImplementedError 

class DriverProtocolState(BaseEnum):
    """
    """
    AUTOSAMPLE = 'DRIVER_STATE_AUTOSAMPLE'
    TEST = 'DRIVER_STATE_TEST'
    CALIBRATE = 'DRIVER_STATE_CALIBRATE'
    COMMAND = 'DRIVER_STATE_COMMAND'
    DIRECT_ACCESS = 'DRIVER_STATE_DIRECT_ACCESS'
    UNKNOWN = 'DRIVER_STATE_UNKNOWN'

class DriverConnectionState(BaseEnum):
    """
    """
    UNCONFIGURED = 'DRIVER_STATE_UNCONFIGURED'
    DISCONNECTED = 'DRIVER_STATE_DISCONNECTED'
    CONNECTED = 'DRIVER_STATE_CONNECTED'
    
class DriverEvent(BaseEnum):
    """
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
    STOP_TEST = 'DRIVER_EVENT_STOP_TEST'
    CALIBRATE = 'DRIVER_EVENT_CALIBRATE'
    RESET = 'DRIVER_EVENT_RESET'
    ENTER = 'DRIVER_EVENT_ENTER'
    EXIT = 'DRIVER_EVENT_EXIT'
    UPDATE_PARAMS = 'DRIVER_EVENT_UPDATE_PARAMS'

class DriverAsyncEvent(BaseEnum):
    """
    """
    STATE_CHANGE = 'DRIVER_ASYNC_EVENT_STATE_CHANGE'
    CONFIG_CHANGE = 'DRIVER_ASYNC_EVENT_CONFIG_CHANGE'
    SAMPLE = 'DRIVER_ASYNC_EVENT_SAMPLE'
    ERROR = 'DRIVER_ASYNC_EVENT_ERROR'

class DriverParameter(BaseEnum):
    """
    """
    ALL = 'DRIVER_PARAMETER_ALL'

class InstrumentDriver(object):
    """
    """
    
    def __init__(self, event_callback):
        """
        """
        self._send_event = event_callback

    #############################################################
    # Device connection interface.
    #############################################################
    
    def initialize(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('initialize() not implemented.')
        
    def configure(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('configure() not implemented.')
        
    def connect(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('connect() not implemented.')
    
    def disconnect(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('disconnect() not implemented.')

    #############################################################
    # Commande and control interface.
    #############################################################

    def discover(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('discover() is not implemented.')

    def get(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('get() is not implemented.')

    def set(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('set() not implemented.')

    def execute_acquire_sample(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('execute_acquire_sample() not implemented.')

    def execute_start_autosample(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('execute_start_autosample() not implemented.')

    def execute_stop_autosample(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('execute_stop_autosample() not implemented.')

    def execute_test(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('execute_test() not implemented.')

    def execute_calibrate(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('execute_calibrate() not implemented.')

    def execute_calibrate(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('execute_calibrate() is not implemented.')

    def execute_direct(self, *args, **kwargs):
        """
        """
        raise NotImplementedError('execute_direct() not implemented.')

    ########################################################################
    # Resource query interface.
    ########################################################################    
    
    def get_resource_commands(self):
        """
        """
        return [cmd for cmd in dir(self) if cmd.startswith('execute_')]    
    
    def get_resource_params(self):
        """
        """
        raise NotImplementedError('get_resource_params() is not implemented.')
            
    def get_current_state(self, channels):
        """
        """
        raise NotImplementedError('get_current_state() is not implemented.')

    ########################################################################
    # Event interface.
    ########################################################################

    def _driver_event(self, type, val=None):
        """
        """
        event = {
            'type' : type,
            'value' : None
        }
        if type == DriverAsyncEvent.STATE_CHANGE:
            state = self.get_current_state()
            event['value'] = state
            self._send_event(event)
            
        elif type == DriverAsyncEvent.CONFIG_CHANGE:
            config = self.get('ALL')
            event['value'] = config
            self._send_event(event)
        
        elif type == DriverAsyncEvent.SAMPLE:
            event['value'] = val
            self._send_event(event)
            
        elif type == DriverAsyncEvent.ERROR:
            pass


    def driver_echo(self, msg):
        """
        """
        reply = 'driver_echo: '+msg
        return reply