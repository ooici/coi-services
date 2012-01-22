#!/usr/bin/env python

"""
@package ion.services.mi.sbe37_driver
@file ion/services/mi/sbe37_driver.py
@author Edward Hunter
@brief Driver class for sbe37 CTD instrument.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import logging

from ion.services.mi.instrument_driver_eh import InstrumentDriver
from ion.services.mi.instrument_protocol_eh \
                        import CommandResponseInstrumentProtocol
from ion.services.mi.fsm import FSM, ExceptionFSM

mi_logger = logging.getLogger('mi_logger')


class SBE37Protocol(CommandResponseInstrumentProtocol):
    """
    """
    def __init__(self):
        """
        """
        CommandResponseInstrumentProtocol.__init__(self)
        # Set up fsm here.

    def initialize(self):
        """
        """
        pass
    
    def configure(self, params):
        """
        """
        pass
    
    def connect(self):
        """
        """
        pass
    
    def disconnect(self):
        """
        """
        pass

class SBE37Driver(InstrumentDriver):
    """
    class docstring
    """
    def __init__(self):
        """
        method docstring
        """
        InstrumentDriver.__init__(self)
        
    def initialize(self, chan_list):
        """
        """
        pass
    
    def configure(self, config):
        """
        """
        mi_logger.info('Inside SBE37Driver.configure')        
    
    def connect(self, chan_list):
        """
        """
        pass
    
    def disconnect(self, chan_list):
        """
        """
        pass
    
    def test_driver_messaging(self):
        """
        """
        result = 'random float %f' % random.random()
        return result
        


            

