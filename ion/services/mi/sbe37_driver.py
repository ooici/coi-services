#!/usr/bin/env python

"""
@package ion.services.mi.sbe37_driver
@file ion/services/mi/sbe37_driver.py
@author Edward Hunter
@brief Driver class for sbe37 CTD instrument.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import time
import random

from ion.services.mi.driver_process import DriverProcess
from ion.services.mi.instrument_driver import InstrumentDriver
#from pyon.core.exception import BadRequest, NotFound
#from pyon.public import IonObject, AT, log
#from pyon.agent.agent import ResourceAgent

#from zope.interface import Interface, implements

class SBE37Driver(InstrumentDriver):
    """
    class docstring
    """
    def __init__(self):
        """
        method docstring
        """
        InstrumentDriver.__init__(self)
        self.listen_thread = None
        self.stop_listen_thread = True
        
    def rcmd_initialize(self):
        """
        """
        pass
    
    def rcmd_configure(self, params):
        """
        """
        pass
    
    def rcmd_connect(self, timeout):
        """
        """
        pass
    
    def rcmd_disconnect(self, timeout):
        """
        """
        pass
    
    def rcmd_test_driver_messaging(self):
        """
        """
        result = 'random float %f' % random.random()
        return result
        


            

