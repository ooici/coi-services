#!/usr/bin/env python

"""
@package ion.services.mi.exceptions Exception classes for MI work
@file ion/services/mi/exceptions.py
@author Edward Hunter
@brief Common exceptions used in the MI work. Specific ones can be subclassed
in the driver code.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'


class InstrumentException(Exception):
    """
    Base class for exceptions raised by instrument drivers.
    """
    pass

class NotImplementedError(InstrumentException):
    """
    A driver function is not implemented.
    """
    pass

class TimeoutError(InstrumentException):
    """
    Could not wake device.
    Response did not return in time.
    """
    pass

class ProtocolError(InstrumentException):
    """
    Command can't be built, no handler.
    Bad response.
    """
    pass

class ParameterError(InstrumentException):
    """
    Missing required parameter.
    Parameter wrong type.
    Parameter unknown to device.
    The value passed to a command could not be formatted properly.    
    """
    pass

class SampleError(InstrumentException):
    """
    An expected sample could not be extracted.
    """
    pass

class StateError(InstrumentException):
    """
    Response from device does not match an expected state.
    Command not handled by current state.
    """
    pass

class UnknownCommandError(InstrumentException):
    """
    The driver does not have the command callable.
    """
    pass

class ConnectionError(InstrumentException):
    """
    Could not connect to the logger.
    Device connection lost.
    """
    pass