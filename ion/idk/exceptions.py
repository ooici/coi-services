#!/usr/bin/env python

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from ion.idk.logger import Log

class IDKException(Exception):
    """Base class for an exception related to IDK processes
    """
    
    def __init__ (self, msg=None, error_code=None):
        self.args = (error_code, msg)
        self.error_code = error_code
        self.msg = msg
        
        Log.error(self)
    
class IDKWrongRunningDirectory(IDKException):
    """Some IDK processes need to be run from the base of the MI repo"""
    pass

class IDKConfigMissing(IDKException):
    """the default IDK configuration file could not be found"""
    pass

class DriverParameterUndefined(IDKException):
    """A driver parameter is undefined in the metadata file"""
    pass

class MissingTemplate(IDKException):
    """An IDK template is missing for code generation"""
    pass

