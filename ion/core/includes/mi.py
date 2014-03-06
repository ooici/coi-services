#!/usr/bin/env python

__author__ = 'Roger Unwin'
__license__ = 'Apache 2.0'

from ion.agents.instrument.common import BaseEnum

#####
##  For goodish reasons this file is maintained in both the marine-integration
##  repository AND coi-services (HERE).  It is required to keep these files
##  synced.
##
##  To maintain backwards capability definitions in this file should not be
##  changed or remove.  We should only add.
#####

class DriverAsyncEvent(BaseEnum):
    """
    Asynchronous driver event types.
    """
    STATE_CHANGE = 'DRIVER_ASYNC_EVENT_STATE_CHANGE'
    CONFIG_CHANGE = 'DRIVER_ASYNC_EVENT_CONFIG_CHANGE'
    SAMPLE = 'DRIVER_ASYNC_EVENT_SAMPLE'
    ERROR = 'DRIVER_ASYNC_EVENT_ERROR'
    RESULT = 'DRIVER_ASYNC_RESULT'
    DIRECT_ACCESS = 'DRIVER_ASYNC_EVENT_DIRECT_ACCESS'
    AGENT_EVENT = 'DRIVER_ASYNC_EVENT_AGENT_EVENT'


# The following ENUMs are used in DM and hopefully can be removed
class DriverParameter(BaseEnum):
    """
    Base driver parameters. Subclassed by specific drivers with device
    specific parameters.
    """
    ALL = 'DRIVER_PARAMETER_ALL'

class DriverEvent(BaseEnum):
    """
    Base events for driver state machines. Commands and other events
    are transformed into state machine events for handling.
    """
    ENTER = 'DRIVER_EVENT_ENTER'
    EXIT = 'DRIVER_EVENT_EXIT'
    INITIALIZE = 'DRIVER_EVENT_INITIALIZE'
    CONFIGURE = 'DRIVER_EVENT_CONFIGURE'
    CONNECT = 'DRIVER_EVENT_CONNECT'
    CONNECTION_LOST = 'DRIVER_CONNECTION_LOST'
    DISCONNECT = 'DRIVER_EVENT_DISCONNECT'
    SET = 'DRIVER_EVENT_SET'
    GET = 'DRIVER_EVENT_GET'
    DISCOVER = 'DRIVER_EVENT_DISCOVER'
    EXECUTE = 'DRIVER_EVENT_EXECUTE'
    ACQUIRE_SAMPLE = 'DRIVER_EVENT_ACQUIRE_SAMPLE'
    START_AUTOSAMPLE = 'DRIVER_EVENT_START_AUTOSAMPLE'
    STOP_AUTOSAMPLE = 'DRIVER_EVENT_STOP_AUTOSAMPLE'
    TEST = 'DRIVER_EVENT_TEST'
    RUN_TEST = 'DRIVER_EVENT_RUN_TEST'
    STOP_TEST = 'DRIVER_EVENT_STOP_TEST'
    CALIBRATE = 'DRIVER_EVENT_CALIBRATE'
    RESET = 'DRIVER_EVENT_RESET'
    ENTER = 'DRIVER_EVENT_ENTER'
    EXIT = 'DRIVER_EVENT_EXIT'
    UPDATE_PARAMS = 'DRIVER_EVENT_UPDATE_PARAMS'
    BREAK = 'DRIVER_EVENT_BREAK'
    EXECUTE_DIRECT = 'EXECUTE_DIRECT'
    START_DIRECT = 'DRIVER_EVENT_START_DIRECT'
    STOP_DIRECT = 'DRIVER_EVENT_STOP_DIRECT'
    PING_DRIVER = 'DRIVER_EVENT_PING_DRIVER'
    FORCE_STATE = 'DRIVER_FORCE_STATE'
    CLOCK_SYNC = 'DRIVER_EVENT_CLOCK_SYNC'
    ACQUIRE_STATUS = 'DRIVER_EVENT_ACQUIRE_STATUS'
    GAP_RECOVERY = 'DRIVER_EVENT_GAP_RECOVERY'
