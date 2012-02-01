#!/usr/bin/env python

"""
@package ion.services.mi.parameter_dict 
@file ion/services/mi/parameter_dict.py
@author 
@brief 
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import re
import logging

from ion.services.mi.common import InstErrorCode

mi_logger = logging.getLogger('mi_logger')

class ParameterDictVal(object):
    """
    """
    def __init__(self, name, pattern, f_getval, f_format, value=None):
        """
        """
        self.name = name
        self.pattern = pattern
        self.regex = re.compile(pattern)
        self.f_getval = f_getval
        self.f_format = f_format
        self.value = value

    def update(self, input):
        """
        """
        match = self.regex.match(input)
        if match:
            self.value = self.f_getval(match)
            mi_logger.debug('Updated parameter %s=%s', self.name, str(self.value))
            return True
        else: return False

class ParameterDict(object):
    """
    """
    
    def __init__(self):
        """
        """
        self.d = {}
    
    def add(self, name, pattern, f_getval, f_format, value=None):
        """
        """
        self.d[name] = ParameterDictVal(name, pattern, f_getval, f_format, value)
        
    def getval(self, name):
        """
        """
        return self.d[name].value
        
    
    def setval(self, name, value):
        """
        """        
        self.d[name].value = value
            
    
    def update(self, input):
        """
        """
        for (name, val) in self.d.iteritems():
            if val.update(input):
                break
            
