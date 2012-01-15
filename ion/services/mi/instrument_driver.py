#!/usr/bin/env python

"""
@package ion.services.mi.iinstrument_driver Instrument driver base class
@file ion/services/mi/iinstrument_driver.py
@author Edward Hunter
@brief Base class for instrument drivers.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'


class InstrumentDriver(object):
    """
    class docstring
    """
    
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
    
    
