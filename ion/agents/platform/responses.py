#!/usr/bin/env python

"""
@package ion.agents.platform.responses
@file    ion/agents/platform/responses.py
@author  Carlos Rueda
@brief   Some constants for responses from platform agents/drivers.
"""

from ion.agents.instrument.common import BaseEnum

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


class NormalResponse(BaseEnum):
    PORT_TURNED_ON                = 'OK_PORT_TURNED_ON'
    PORT_ALREADY_ON               = 'OK_PORT_ALREADY_ON'
    PORT_TURNED_OFF               = 'OK_PORT_TURNED_OFF'
    PORT_ALREADY_OFF              = 'OK_PORT_ALREADY_OFF'
    PORT_SET_OVER_CURRENT         = 'OK_PORT_SET_OVER_CURRENT'


class InvalidResponse(BaseEnum):
    PLATFORM_ID                   = 'ERROR_INVALID_PLATFORM_ID'
    ATTRIBUTE_ID                  = 'ERROR_INVALID_ATTRIBUTE_ID'
    ATTRIBUTE_VALUE_OUT_OF_RANGE  = 'ERROR_ATTRIBUTE_VALUE_OUT_OF_RANGE'
    ATTRIBUTE_NOT_WRITABLE        = 'ERROR_ATTRIBUTE_NOT_WRITABLE'
    PORT_ID                       = 'ERROR_INVALID_PORT_ID'
    EVENT_LISTENER_URL            = 'ERROR_INVALID_EVENT_LISTENER_URL'
