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
    INSTRUMENT_DISCONNECTED       = 'OK_INSTRUMENT_DISCONNECTED'
    PORT_TURNED_ON                = 'OK_PORT_TURNED_ON'
    PORT_ALREADY_ON               = 'OK_PORT_ALREADY_ON'
    PORT_TURNED_OFF               = 'OK_PORT_TURNED_OFF'
    PORT_ALREADY_OFF              = 'OK_PORT_ALREADY_OFF'


class InvalidResponse(BaseEnum):
    PLATFORM_ID                   = 'INVALID_PLATFORM_ID'
    ATTRIBUTE_ID                  = 'INVALID_ATTRIBUTE_ID'
    ATTRIBUTE_VALUE_OUT_OF_RANGE  = 'ERROR_ATTRIBUTE_VALUE_OUT_OF_RANGE'
    ATTRIBUTE_NOT_WRITABLE        = 'ERROR_ATTRIBUTE_NOT_WRITABLE'
    PORT_ID                       = 'INVALID_PORT_ID'
    PORT_IS_ON                    = 'ERROR_PORT_IS_ON'

    INSTRUMENT_ALREADY_CONNECTED  = 'ERROR_INSTRUMENT_ALREADY_CONNECTED'
    INSTRUMENT_NOT_CONNECTED      = 'ERROR_INSTRUMENT_NOT_CONNECTED'
    MISSING_INSTRUMENT_ATTRIBUTE  = 'MISSING_INSTRUMENT_ATTRIBUTE'
    INVALID_INSTRUMENT_ATTRIBUTE  = 'INVALID_INSTRUMENT_ATTRIBUTE'

    PLATFORM_TYPE                 = 'INVALID_PLATFORM_TYPE'
    EVENT_LISTENER_URL            = 'INVALID_EVENT_LISTENER_URL'
    EVENT_TYPE                    = 'INVALID_EVENT_TYPE'
