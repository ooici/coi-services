#!/usr/bin/env python

"""
@package ion.services.mi.instrument_agent Instrument resource agent
@file ion/services/mi/instrument_agent.py
@author Edward Hunter
@brief Resource agent derived class providing an instrument agent as a resource.
This resource fronts instruments and instrument drivers one-to-one in ION.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

from pyon.core.exception import BadRequest, NotFound
from pyon.public import IonObject, AT, log
from pyon.agent.agent import ResourceAgent


class InstrumentAgent(ResourceAgent):
    '''The base instrument agent class
    
    The Instrument Agent is responsible for interfacing the ION infrastructure
    with the individual instrument drivers that then talk to the instruments.
    '''
    
    def __init__(self):
    
        '''A list of errors that the Instrument Agent can return'''
        self.instrument_agent_errors = None
    
        '''The instrument driver used to communicate with the instrument'''
        self.driver = None
    
    # May need some endpoint information in here somewhere.    
        # Build an instrument driver when instantiated
        
    # Methods are placeholders for now. To be improved/documented during port
    def execute_observatory(self, command, transaction_id):
        '''
        '''
    
    def get_observatory(self, params, transaction_id):
        '''
        '''

    def set_observatory(self, params, transaction_id):
        '''
        '''

    def get_observatory_metadata(self, params, transaction_id):
        '''
        '''

    def get_observatory_status(self, params, transaction_id):
        '''
        '''

    def get_observatory_capabilities(self, params, transaction_id):
        '''
        '''

    def execute_device(self, channels, command, transaction_id):
        '''
        '''

    def get_device(self, params, transaction_id):
        '''
        '''

    def set_device(self, params, transaction_id):
        '''
        '''

    def execute_device_direct(self, bytes, transaction_id):
        '''
        '''

    def get_device_metadata(self, params, transaction_id):
        '''
        '''

    def get_device_status(self, params, transaction_id):
        '''
        '''

    def start_transaction(self, acq_timeout, exp_timeout):
        '''
        '''

    def end_transaction(self, transaction_id):
        '''
        '''

    def driver_event_occurred(self, content):
        '''
        '''


