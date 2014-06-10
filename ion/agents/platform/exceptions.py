#!/usr/bin/env python

"""
@package ion.agents.platform.exceptions
@file    ion/agents/platform/exceptions.py
@author  Carlos Rueda
@brief   platform related exceptions
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.core.exception import IonException


class PlatformException(IonException):
    """
    Base class for platform related exceptions.
    """

    def __init__(self, msg=None, error_code=None, reason=None):
        super(PlatformException, self).__init__()
        self.msg = msg if msg else str(reason) if reason else None
        self.args = (error_code, self.msg)
        self.error_code = error_code
        self.reason = reason


class PlatformConnectionException(PlatformException):
    """
    Exception related to connection with a physical platform
    """
    pass


class PlatformConfigurationException(PlatformException):
    """
    Exception related with the configuration of a platform agent.
    """
    pass


class PlatformDriverException(PlatformException):
    """
    Exception related to basic PlatformDriver functionality or configuration.
    """
    pass


class CannotInstantiateDriverException(PlatformDriverException):
    """
    Platform agent could not create driver.
    """
    pass
