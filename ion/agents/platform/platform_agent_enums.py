#!/usr/bin/env python

"""
@package ion.agents.platform.platform_agent_enums
@file    ion/agents/platform/platform_agent_enums.py
@author  Carlos Rueda
@brief   Basic platform agent enums and definitions
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.agent.agent import ResourceAgentState
from pyon.agent.agent import ResourceAgentEvent

from pyon.agent.common import BaseEnum


class PlatformAgentState(BaseEnum):
    """
    Platform agent state enum.
    """
    UNINITIALIZED     = ResourceAgentState.UNINITIALIZED
    INACTIVE          = ResourceAgentState.INACTIVE
    IDLE              = ResourceAgentState.IDLE
    STOPPED           = ResourceAgentState.STOPPED
    COMMAND           = ResourceAgentState.COMMAND
    MONITORING        = 'PLATFORM_AGENT_STATE_AUTOSAMPLE'
    LAUNCHING         = 'PLATFORM_AGENT_STATE_LAUNCHING'
    LOST_CONNECTION   = ResourceAgentState.LOST_CONNECTION
    MISSION_STREAMING = 'PLATFORM_AGENT_STATE_MISSION_STREAMING'
    MISSION_COMMAND   = 'PLATFORM_AGENT_STATE_MISSION_COMMAND'


class PlatformAgentEvent(BaseEnum):
    ENTER                     = ResourceAgentEvent.ENTER
    EXIT                      = ResourceAgentEvent.EXIT

    INITIALIZE                = ResourceAgentEvent.INITIALIZE
    RESET                     = ResourceAgentEvent.RESET
    GO_ACTIVE                 = ResourceAgentEvent.GO_ACTIVE
    GO_INACTIVE               = ResourceAgentEvent.GO_INACTIVE
    RUN                       = ResourceAgentEvent.RUN
    SHUTDOWN                  = 'PLATFORM_AGENT_SHUTDOWN_CHILDREN'

    CLEAR                     = ResourceAgentEvent.CLEAR
    PAUSE                     = ResourceAgentEvent.PAUSE
    RESUME                    = ResourceAgentEvent.RESUME

    GET_RESOURCE_CAPABILITIES = ResourceAgentEvent.GET_RESOURCE_CAPABILITIES
    PING_RESOURCE             = ResourceAgentEvent.PING_RESOURCE
    GET_RESOURCE              = ResourceAgentEvent.GET_RESOURCE
    SET_RESOURCE              = ResourceAgentEvent.SET_RESOURCE
    EXECUTE_RESOURCE          = ResourceAgentEvent.EXECUTE_RESOURCE
    GET_RESOURCE_STATE        = ResourceAgentEvent.GET_RESOURCE_STATE

    START_MONITORING          = 'PLATFORM_AGENT_START_AUTOSAMPLE'
    STOP_MONITORING           = 'PLATFORM_AGENT_STOP_AUTOSAMPLE'
    LAUNCH_COMPLETE           = 'PLATFORM_AGENT_LAUNCH_COMPLETE'

    LOST_CONNECTION           = ResourceAgentEvent.LOST_CONNECTION
    AUTORECONNECT             = ResourceAgentEvent.AUTORECONNECT

    RUN_MISSION               = 'PLATFORM_AGENT_RUN_MISSION'
    EXIT_MISSION              = 'PLATFORM_AGENT_EXIT_MISSION'
    ABORT_MISSION             = 'PLATFORM_AGENT_ABORT_MISSION'
    KILL_MISSION              = 'PLATFORM_AGENT_KILL_MISSION'


class PlatformAgentCapability(BaseEnum):
    INITIALIZE                = PlatformAgentEvent.INITIALIZE
    RESET                     = PlatformAgentEvent.RESET
    GO_ACTIVE                 = PlatformAgentEvent.GO_ACTIVE
    GO_INACTIVE               = PlatformAgentEvent.GO_INACTIVE
    RUN                       = PlatformAgentEvent.RUN
    SHUTDOWN                  = PlatformAgentEvent.SHUTDOWN

    CLEAR                     = PlatformAgentEvent.CLEAR
    PAUSE                     = PlatformAgentEvent.PAUSE
    RESUME                    = PlatformAgentEvent.RESUME

    # These are not agent capabilities but interface capabilities.
    #GET_RESOURCE_CAPABILITIES = PlatformAgentEvent.GET_RESOURCE_CAPABILITIES
    #PING_RESOURCE             = PlatformAgentEvent.PING_RESOURCE
    #GET_RESOURCE              = PlatformAgentEvent.GET_RESOURCE
    #SET_RESOURCE              = PlatformAgentEvent.SET_RESOURCE
    #EXECUTE_RESOURCE          = PlatformAgentEvent.EXECUTE_RESOURCE
    #GET_RESOURCE_STATE        = PlatformAgentEvent.GET_RESOURCE_STATE

    START_MONITORING          = PlatformAgentEvent.START_MONITORING
    STOP_MONITORING           = PlatformAgentEvent.STOP_MONITORING

    RUN_MISSION               = PlatformAgentEvent.RUN_MISSION
    ABORT_MISSION             = PlatformAgentEvent.ABORT_MISSION
    KILL_MISSION              = PlatformAgentEvent.KILL_MISSION


class ResourceInterfaceCapability(BaseEnum):
    #GET_RESOURCE_CAPABILITIES = PlatformAgentEvent.GET_RESOURCE_CAPABILITIES
    PING_RESOURCE             = PlatformAgentEvent.PING_RESOURCE
    GET_RESOURCE              = PlatformAgentEvent.GET_RESOURCE
    SET_RESOURCE              = PlatformAgentEvent.SET_RESOURCE
    EXECUTE_RESOURCE          = PlatformAgentEvent.EXECUTE_RESOURCE
    GET_RESOURCE_STATE        = PlatformAgentEvent.GET_RESOURCE_STATE
