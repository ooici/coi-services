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

from ion.services.mi.exceptions import InstrumentConnectionException 
from ion.services.mi.common import DEFAULT_TIMEOUT

class InstrumentDriver(object):
    '''The base instrument driver class
    
    This is intended to be extended where necessary to provide a coherent
    driver for interfacing between the instrument and the instrument agent.
    Think of this class as encapsulating the session layer of the instrument
    interaction.
    
    @see https://confluence.oceanobservatories.org/display/syseng/CIAD+SA+SV+Instrument+Driver+Interface
    '''

    def __init__(self):
        # Setup instance variables with instrument-specific instances.
        # Some may be fed from the instrument protocol subclass.
        
        '''An object for manipulating connect and disconnect to an instrument'''
        self.instrument_connection = None
    
        '''The instrument-specific protocol object'''
        self.instrument_protocol = None
    
        '''The communications method formatting object'''
        self.instrument_comms_method = None
    
        '''The instrument-specific command list'''
        self.instrument_commands = None
    
        '''The instrument-specific metadata parameter list'''
        self.instrument_metadata_parameters = None
    
        '''The instrument-specific parameter list'''
        self.instrument_parameters = None
    
        '''The instrument-specific channel list'''
        self.instrument_channels = None
    
        '''The instrument-specific error list'''
        self.instrument_errors = None
    
        '''The instrument-specific capabilities list'''
        self.instrument_capabilities = None
    
        '''The instrument-specific status list'''
        self.instrument_status = None
    
    def configure(self, params={}, timeout=DEFAULT_TIMEOUT):
        '''Configure the driver's parameters
        
        Some parameters are needed soley by the driver to interact with an
        instrument. These parameters can be set here by the instantiating
        class, likely based on some registry entries for the individual
        instrument being interacted with 
        
        @param params A dictionary of the parameters for configuring the driver
        @param timeout Number of seconds before this operation times out
        '''
        assert(isinstance(params, dict))
        
    def initialize(self, timeout=DEFAULT_TIMEOUT):
        '''
        '''
        
    def connect(self, timeout=DEFAULT_TIMEOUT):
        ''' Connect to the device
        @param timeout Number of seconds before this operation times out
        @retval result Success/failure result
        @throws InstrumentConnectionException
        @todo determine result if already connected
        '''
        # Something like self.InstrumentConnection.connect(), then set state
    
    def disconnect(self, timeout=DEFAULT_TIMEOUT):
        '''
        @param timeout Number of seconds before this operation times out
        @retval result Success/failure result
        @throws InstrumentConnectionException
        @todo determine result if already disconnected
        '''
        # Something like self.InstrumentConnection.disconnect(), then set state
           
           
    def execute(self, channels, command, timeout):
        '''
        '''
    
    def execute_direct(self, bytes, timeout):
        '''
        '''
    
    def get(self, params, timeout):
        '''
        '''
        
    def set(self, params, timeout):
        '''
        '''
        
    def get_metadata(self, params, timeout):
        '''
        '''
        
    def get_status(self, params, timeout):
        '''
        '''
        
    def get_capabilities(self, params, timeout):
        '''
        '''