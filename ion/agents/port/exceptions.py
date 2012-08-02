#!/usr/bin/env python

"""
@package ion.agents.port.exceptions Exception classes for the port agent
@file ion/agents/port/exceptions.py
@author Bill French
@brief Common exceptions used in port agent. Specific ones can be subclassed
in the driver code.
"""
from pyon.core.exception import IonException

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

from ion.agents.instrument.common import InstErrorCode
import traceback

class PortAgentException(IonException):
    """Base class for an exception related to the port agent
    """
    def __init__ (self, msg, error_code=None):
        super(PortAgentException,self).__init__(msg)
        self.status_code = error_code

class PortAgentLaunchException(PortAgentException):
    """
    A port agnet process failed to launch
    """
    pass

class PortAgentMissingConfig(PortAgentException):
    """
    A port agnet process failed to launch
    """
    pass

class PortAgentTimeout(PortAgentException):
    """
    A port agnet process failed to launch
    """
    pass

class NotImplementedException(PortAgentException):
    """
    A port agnet function is not implemented.
    """
    pass

