#!/usr/bin/env python

"""
@package ion.services.mi.exceptions Exception classes for MI work
@file ion/services/mi/exceptions.py
@author Steve Foley
@brief Common exceptions used in the MI work. Specific ones can be subclassed
in the driver code
"""

__author__ = 'Steve Foley'
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


class InstrumentTimeoutError(InstrumentException):
    """
    """
    pass

class InstrumentProtocolError(InstrumentException):
    """
    """
    pass