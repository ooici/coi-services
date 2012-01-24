#!/usr/bin/env python

"""
@package ion.services.mi.common Common classes for MI work
@file ion/services/mi/common.py
@author Steve Foley
@brief Common enumerations, constants, utilities used in the MI work
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

# imports could go here

"""Default timeout value in seconds"""
DEFAULT_TIMEOUT = 30

class BaseEnum(object):
    """Base class for enums.
    
    Used to code agent and instrument states, events, commands and errors.
    To use, derive a class from this subclass and set values equal to it
    such as:
    @code
    class FooEnum(BaseEnum):
       VALUE1 = "Value 1"
       VALUE2 = "Value 2"
    @endcode
    and address the values as FooEnum.VALUE1 after you import the
    class/package.
    
    Enumerations are part of the code in the MI modules since they are tightly
    coupled with what the drivers can do. By putting the values here, they
    are quicker to execute and more compartmentalized so that code can be
    re-used more easily outside of a capability container as needed.
    """
    
    @classmethod
    def list(cls):
        """List the values of this enum."""
        return [getattr(cls,attr) for attr in dir(cls) if \
            not callable(getattr(cls,attr)) and not attr.startswith('__')]


    @classmethod
    def has(cls, item):
        """Is the object defined in the class?
        
        Use this function to test
        a variable for enum membership. For example,
        @code
        if not FooEnum.has(possible_value)
        @endcode
        
        @param item The attribute value to test for.
        @retval True if one of the class attributes has value item, false
        otherwise.
        """
        return item in cls.list()

###############################################################################
# Common driver elements. Below are the constants intended for all instrument
# specific driver implementations, and part of the driver implementation
# framework. 
##############################################################################


###############################################################################
# Instrument agent constants.
##############################################################################

class AgentState(BaseEnum):
    """Common agent state enum.
    
    Includes aggregate states of the agent state machine.
    """
    UNKNOWN = 'AGENT_STATE_UNKNOWN'
    POWERED_DOWN = 'AGENT_STATE_POWERED_DOWN'
    POWERED_UP = 'AGENT_STATE_POWERED_UP'
    UNINITIALIZED = 'AGENT_STATE_UNINITIALIZED'
    ACTIVE = 'AGENT_STATE_ACTIVE'
    INACTIVE = 'AGENT_STATE_INACTIVE'
    STOPPED = 'AGENT_STATE_STOPPED'
    IDLE = 'AGENT_STATE_IDLE'
    RUNNING = 'AGENT_STATE_RUNNING'
    OBSERVATORY_MODE = 'AGENT_STATE_OBSERVATORY_MODE'
    DIRECT_ACCESS_MODE = 'AGENT_STATE_DIRECT_ACCESS_MODE'


class AgentEvent(BaseEnum):
    """Common agent event enum"""
    
    GO_POWER_UP = 'AGENT_EVENT_GO_POWER_DOWN'
    GO_POWER_DOWN = 'AGENT_EVENT_GO_POWER_UP'
    INITIALIZE = 'AGENT_EVENT_INITIALIZE'
    RESET = 'AGENT_EVENT_RESET'
    GO_ACTIVE = 'AGENT_EVENT_GO_ACTIVE'
    GO_INACTIVE = 'AGENT_EVENT_GO_INACTIVE'
    CLEAR = 'AGENT_EVENT_CLEAR'
    RESUME = 'AGENT_EVENT_RESUME'
    RUN = 'AGENT_EVENT_RUN'
    PAUSE = 'AGENT_EVENT_PAUSE'
    GO_OBSERVATORY_MODE = 'AGENT_EVENT_GO_OBSERVATORY_MODE'
    GO_DIRECT_ACCESS_MODE = 'AGENT_EVENT_GO_DIRECT_ACCESS_MODE'
    ENTER = 'AGENT_EVENT_ENTER'
    EXIT = 'AGENT_EVENT_EXIT'
    

class AgentCommand(BaseEnum):
    """ Common agent commands enum"""
    
    TRANSITION = 'AGENT_CMD_TRANSITION'
    TRANSMIT_DATA = 'AGENT_CMD_TRANSMIT_DATA'
    SLEEP = 'AGENT_CMD_SLEEP'


class AgentParameter(BaseEnum):
    """Common agent parameters"""
    
    EVENT_PUBLISHER_ORIGIN = 'AGENT_PARAM_EVENT_PUBLISHER_ORIGIN'
    TIME_SOURCE = 'AGENT_PARAM_TIME_SOURCE'
    CONNECTION_METHOD = 'AGENT_PARAM_CONNECTION_METHOD'
    MAX_ACQ_TIMEOUT = 'AGENT_PARAM_MAX_ACQ_TIMEOUT'
    DEFAULT_EXP_TIMEOUT = 'AGENT_PARAM_DEFAULT_EXP_TIMEOUT'
    MAX_EXP_TIMEOUT = 'AGENT_PARAM_MAX_EXP_TIMEOUT'    
    DRIVER_DESC = 'AGENT_PARAM_DRIVER_DESC'
    DRIVER_CLIENT_DESC = 'AGENT_PARAM_DRIVER_CLIENT_DESC'
    DRIVER_CONFIG = 'AGENT_PARAM_DRIVER_CONFIG'
    BUFFER_SIZE = 'AGENT_PARAM_BUFFER_SIZE'
    ALL = 'AGENT_PARAM_ALL'


class AgentStatus(BaseEnum):
    """Common agent status enum"""
    
    AGENT_STATE = 'AGENT_STATUS_AGENT_STATE'
    CONNECTION_STATE = 'AGENT_STATUS_CONNECTION_STATE'
    ALARMS = 'AGENT_STATUS_ALARMS'
    TIME_STATUS = 'AGENT_STATUS_TIME_STATUS'
    BUFFER_SIZE = 'AGENT_STATUS_BUFFER_SIZE'
    AGENT_VERSION = 'AGENT_STATUS_AGENT_VERSION'
    PENDING_TRANSACTIONS = 'AGENT_STATUS_PENDING_TRANSACTIONS'
    ALL = 'AGENT_STATUS_ALL'


class AgentConnectionState(BaseEnum):
    """Common agent connection state enum.
    
    Possible states of connection/disconnection an agent may be in, among the
    shore and wet side agent, the driver and the hardware iteself.
    """
    REMOTE_DISCONNECTED = 'AGENT_CONNECTION_STATE_REMOTE_DISCONNECTED'
    POWERED_DOWN = 'AGENT_CONNECTION_STATE_POWERED_DOWN'
    NO_DRIVER = 'AGENT_CONNECTION_STATE_NO_DRIVER'
    DISCONNECTED = 'AGENT_CONNECTION_STATE_DISCONNECTED'
    CONNECTED = 'AGENT_CONNECTION_STATE_CONNECTED'
    UNKOWN = 'AGENT_CONNECTION_STATE_UNKNOWN'


class Datatype(BaseEnum):
    """Common agent parameter and metadata types"""
    
    DATATYPE = 'TYPE_DATATYPE' # This type.
    INT = 'TYPE_INT' # int.
    FLOAT = 'TYPE_FLOAT' # float.
    BOOL = 'TYPE_BOOL' # bool.
    STRING = 'TYPE_STRING' # str.
    INT_RANGE = 'TYPE_INT_RANGE' # (int,int).
    FLOAT_RANGE = 'TYPE_FLOAT_RANGE' # (float,float).
    TIMESTAMP = 'TYPE_TIMESTAMP' # (int seconds,int nanoseconds).
    TIME_DURATION = 'TYPE_TIME_DURATION' # TBD.
    PUBSUB_TOPIC_DICT = 'TYPE_PUBSUB_TOPIC_DICT' # dict of topic strings.
    RESOURCE_ID = 'TYPE_RESOURCE_ID' # str (possible validation).
    ADDRESS = 'TYPE_ADDRESS' # str (possible validation).
    ENUM = 'TYPE_ENUM' # str with valid values.
    PUBSUB_ORIGIN = 'TYPE_PUBSUB_ORIGIN'

"""
@todo Used by the existing drivers...need to fix.
"""
publish_msg_type = {
    'Error':'Error',
    'StateChange':'StateChange',
    'ConfigChange':'ConfigChange',
    'Data':'Data',
    'Event':'Event'
}

driver_client = "PLACEHOLDER"


class DriverAnnouncement(BaseEnum):
    """Common publish message type enum"""
    
    ERROR = 'DRIVER_ANNOUNCEMENT_ERROR'          
    STATE_CHANGE = 'DRIVER_ANNOUNCEMENT_STATE_CHANGE'
    CONFIG_CHANGE = 'DRIVER_ANNOUNCEMENT_CONIFG_CHANGE'
    DATA_RECEIVED = 'DRIVER_ANNOUNCEMENT_DATA_RECEIVED'
    EVENT_OCCURRED = 'DRIVER_ANNOUNCEMENT_EVENT_OCCURRED'        
    
    
class TimeSource(BaseEnum):
    """Common time source enum for the device fronted by the agent"""
    
    NOT_SPECIFIED = 'TIME_SOURCE_NOT_SPECIFIED'
    PTP_DIRECT = 'TIME_SOURCE_PTP_DIRECT' # IEEE 1588 PTP connection directly supported.
    NTP_UNICAST = 'TIME_SOURCE_NTP_UNICAST' # NTP unicast to the instrument.
    NTP_BROADCAST = 'TIME_SOURCE_NTP_BROADCAST' # NTP broadcast to the instrument.
    LOCAL_OSCILLATOR = 'TIME_SOURCE_LOCAL_OSCILLATOR' # Device has own clock.
    DRIVER_SET_INTERVAL = 'TIME_SOURCE_DRIVER_SET_INTERVAL' # Driver sets clock at interval.
    

class ConnectionMethod(BaseEnum):
    """Common connection method to agent and device enum"""
    
    NOT_SPECIFIED = 'CONNECTION_METHOD_NOT_SPECIFIED'
    OFFLINE = 'CONNECTION_METHOD_OFFLINE' 
    CABLED_OBSERVATORY = 'CONNECTION_METHOD_CABLED_OBSERVATORY' 
    SHORE_NETWORK = 'CONNECTION_METHOD_SHORE_NETWORK' 
    PART_TIME_SCHEDULED = 'CONNECTION_METHOD_PART_TIME_SCHEDULED' 
    PART_TIME_RANDOM = 'CONNECTION_METHOD_PART_TIME_RANDOM'    
    

class AlarmType(BaseEnum):
    """Common agent observatory alarm enum"""
    
    CANNOT_PUBLISH = ('ALARM_CANNOT_PUBLISH','Attempted to publish but cannot.')
    INSTRUMENT_UNREACHABLE = ('ALARM_INSTRUMENT_UNREACHABLE',
                'Instrument cannot be contacted when it should be.')
    MESSAGING_ERROR = ('ALARM_MESSAGING_ERROR','Error when sending messages.')
    HARDWARE_ERROR = ('ALARM_HARDWARE_ERROR','Hardware problem detected.')
    UNKNOWN_ERROR = ('ALARM_UNKNOWN_ERROR','An unknown error has occurred.')       
   

class ObservatoryCapability(BaseEnum):
    """Common agent observatory capabilies enum"""
    
    OBSERVATORY_COMMANDS = 'CAP_OBSERVATORY_COMMANDS' 
    OBSERVATORY_PARAMS = 'CAP_OBSERVATORY_PARAMS' 
    OBSERVATORY_STATUSES = 'CAP_OBSERVATORY_STATUSES' 
    OBSERVATORY_METADATA = 'CAP_OBSERVATORY_METADATA' 
    OBSERVATORY_ALL = 'CAP_OBSERVATORY_ALL'
    

class DriverCapability(BaseEnum):
    """Common device capabilities enum"""
    
    DEVICE_METADATA = 'CAP_DEVICE_METADATA' 
    DEVICE_COMMANDS = 'CAP_DEVICE_COMMANDS' 
    DEVICE_PARAMS = 'CAP_DEVICE_PARAMS' 
    DEVICE_STATUSES = 'CAP_DEVICE_STATUSES' 
    DEVICE_CHANNELS = 'CAP_DEVICE_CHANNELS' 
    DEVICE_ALL = 'CAP_DEVICE_ALL'
    

class InstrumentCapability(ObservatoryCapability,DriverCapability):
    """Comination of agent and device capabilities enum"""
    
    ALL = 'CAP_ALL'


class MetadataParameter(BaseEnum):
    """Common metadata parameter names enum"""
    
    DATATYPE = 'META_DATATYPE'
    PHYSICAL_PARAMETER_TYPE = 'META_PHYSICAL_PARAMETER_TYPE'
    MINIMUM_VALUE = 'META_MINIMUM_VALUE'
    MAXIMUM_VALUE = 'META_MAXIMUM_VALUE'
    UNITS = 'META_UNITS'
    UNCERTAINTY = 'META_UNCERTAINTY'
    LAST_CHANGE_TIMESTAMP = 'META_LAST_CHANGE_TIMESTAMP'
    WRITABLE = 'META_WRITABLE'
    VALID_VALUES = 'META_VALID_VALUES'
    FRIENDLY_NAME = 'META_FRIENDLY_NAME'
    DESCRIPTION = 'META_DESCRIPTION'
    ALL = 'META_ALL'

###############################################################################
# Error constants.
##############################################################################

class InstErrorCode(BaseEnum):
    """Error codes generated by instrument drivers and agents"""
    
    OK = ['OK']
    INVALID_DESTINATION = ['ERROR_INVALID_DESTINATION','Intended destination for a message or operation is not valid.']
    TIMEOUT = ['ERROR_TIMEOUT','The message or operation timed out.']
    NETWORK_FAILURE = ['ERROR_NETWORK_FAILURE','A network failure has been detected.']
    NETWORK_CORRUPTION = ['ERROR_NETWORK_CORRUPTION','A message passing through the network has been determined to be corrupt.']
    OUT_OF_MEMORY = ['ERROR_OUT_OF_MEMORY','There is no more free memory to complete the operation.']
    LOCKED_RESOURCE = ['ERROR_LOCKED_RESOURCE','The resource being accessed is in use by another exclusive operation.']
    RESOURCE_NOT_LOCKED = ['ERROR_RESOURCE_NOT_LOCKED','Attempted to unlock a free resource.']
    RESOURCE_UNAVAILABLE = ['ERROR_RESOURCE_UNAVAILABLE','The resource being accessed is unavailable.']
    TRANSACTION_REQUIRED = ['ERROR_TRANSACTION_REQUIRED','The operation requires a transaction with the agent.']
    UNKNOWN_ERROR = ['ERROR_UNKNOWN_ERROR','An unknown error has been encountered.']
    PERMISSION_ERROR = ['ERROR_PERMISSION_ERROR','The user does not have the correct permission to access the resource in the desired way.']
    INVALID_TRANSITION = ['ERROR_INVALID_TRANSITION','The transition being requested does not apply for the current state.']
    INCORRECT_STATE = ['ERROR_INCORRECT_STATE','The operation being requested does not apply to the current state.']
    UNKNOWN_EVENT = ['ERROR_UNKNOWN_EVENT','The event is not defined for this driver.']
    UNHANDLED_EVENT = ['ERROR_UNHANDLED_EVENT','The event was not handled by the state.']
    UNKNOWN_TRANSITION = ['ERROR_UNKNOWN_TRANSITION','The specified state transition does not exist.']
    CANNOT_PUBLISH = ['ERROR_CANNOT_PUBLISH','An attempt to publish has failed.']
    INSTRUMENT_UNREACHABLE = ['ERROR_INSTRUMENT_UNREACHABLE','The agent cannot communicate with the device.']
    MESSAGING_ERROR = ['ERROR_MESSAGING_ERROR','An error has been encountered during a messaging operation.']
    HARDWARE_ERROR = ['ERROR_HARDWARE_ERROR','An error has been encountered with a hardware element.']
    WRONG_TYPE = ['ERROR_WRONG_TYPE','The type of operation is not valid in the current state.']
    INVALID_COMMAND = ['ERROR_INVALID_COMMAND','The command is not valid in the given context.']
    UNKNOWN_COMMAND = ['ERROR_UNKNOWN_COMMAND','The command is not recognized.']
    UNKNOWN_CHANNEL = ['ERROR_UNKNOWN_CHANNEL','The channel is not recognized.']
    INVALID_CHANNEL = ['ERROR_INVALID_CHANNEL','The channel is not valid for the requested command.']
    NOT_IMPLEMENTED = ['ERROR_NOT_IMPLEMENTED','The command is not implemented.']
    INVALID_TRANSACTION_ID = ['ERROR_INVALID_TRANSACTION_ID','The transaction ID is not a valid value.']
    INVALID_DRIVER = ['ERROR_INVALID_DRIVER','Driver or driver client invalid.']
    GET_OBSERVATORY_ERR = ['ERROR_GET_OBSERVATORY','Could not retrieve all parameters.']
    EXE_OBSERVATORY_ERR = ['ERROR_EXE_OBSERVATORY','Could not execute observatory command.']
    SET_OBSERVATORY_ERR = ['ERROR_SET_OBSERVATORY','Could not set all parameters.']
    PARAMETER_READ_ONLY = ['ERROR_PARAMETER_READ_ONLY','Parameter is read only.']
    INVALID_PARAMETER = ['ERROR_INVALID_PARAMETER','The parameter is not available.']
    REQUIRED_PARAMETER = ['ERROR_REQUIRED_PARAMETER','A required parameter was not specified.']
    INVALID_PARAM_VALUE = ['ERROR_INVALID_PARAM_VALUE','The parameter value is out of range.']
    INVALID_METADATA = ['ERROR_INVALID_METADATA','The metadata parameter is not available.']
    NO_PARAM_METADATA = ['ERROR_NO_PARAM_METADATA','The parameter has no associated metadata.']
    INVALID_STATUS = ['ERROR_INVALID_STATUS','The status parameter is not available.']
    INVALID_CAPABILITY = ['ERROR_INVALID_CAPABILITY','The capability parameter is not available.']
    BAD_DRIVER_COMMAND = ['ERROR_BAD_DRIVER_COMMAND','The driver did not recognize the command.']
    EVENT_NOT_HANDLED = ['ERROR_EVENT_NOT_HANDLED','The current state did not handle a received event.']
    GET_DEVICE_ERR = ['ERROR_GET_DEVICE','Could not retrieve all parameters from the device.']
    EXE_DEVICE_ERR = ['ERROR_EXE_DEVICE','Could not execute device command.']
    SET_DEVICE_ERR = ['ERROR_SET_DEVICE','Could not set all device parameters.']
    ACQUIRE_SAMPLE_ERR = ['ERROR_ACQUIRE_SAMPLE','Could not acquire a data sample.']
    DRIVER_NOT_CONFIGURED = ['ERROR_DRIVER_NOT_CONFIGURED','The driver could not be configured.']
    DISCONNECT_FAILED = ['ERROR_DISCONNECT_FAILED','The driver could not be properly disconnected.']    
    AGENT_INIT_FAILED = ['ERROR_AGENT_INIT_FAILED','The agent could not be initialized.']    
    AGENT_DEINIT_FAILED = ['ERROR_AGENT_DEINIT_FAILED','The agent could not be deinitialized.']    
    DRIVER_CONNECT_FAILED = ['ERROR_DRIVER_CONNECT_FAILED','The agent could not connect to the driver.']    
    DRIVER_DISCONNECT_FAILED = ['ERROR_DRIVER_DISCONNECT_FAILED_FAILED','The agent could not disconnect to the driver.']    
    INVALID_STATUS = ['ERROR_INVALID_STATUS','The given argument is not a valid status key.']    
    
    @classmethod
    def is_ok(cls,x):
        """Success test functional synonym. Will need iterable type checking
        if success codes get additional info in the future.

        @param x a str, tuple or list to match to an error code success value.
        @retval True if x is a success value, False otherwise.
        """
        
        x = cls.get_list_val(x)
        
        return x == cls.OK
    
    @classmethod
    def is_error(cls,x):
        """Generic error test.
        
        @param x a str, tuple or list to match to an error code error value.
        @retval True if x is an error value, False otherwise.
        """
        x = cls.get_list_val(x)
        
        return (cls.has(x) and x != cls.OK)
        
    @classmethod
    def is_equal(cls,val1,val2):
        """Compare error codes.
        
        Used so we are insulated against the framework
        converting error codes to tuples or other iterables.
        
        @param val1 str, tuple or list matching an error code value.
        @param val2 str, tuple or list matching an error code value.
        @retval True if val1 and val2 are equal and defined, False otherwise.
        """

        val1 = cls.get_list_val(val1)
        val2 = cls.get_list_val(val2)
        
        return cls.has(val1) and cls.has(val2) and (val1 == val2)

    @classmethod
    def get_list_val(cls,x):
        """Convert error code values to lists.
        
        The messaging framework can convert lists to tuples. Allow for simple
        strings to be compared also.
        """
        
        assert(isinstance(x,(str,tuple,list))), 'Expected a str, tuple or list \
        error code value.'
        
        # Object is a list, return unmodified.
        if isinstance(x,list):
            return x
        
        # Object is a string, return length 1 list with string as the value.
        elif isinstance(x,str):
            return list((x,))
        
        # Object is a tuple, return a tuple with same elements.
        else:
            return list(x)            

    @classmethod
    def get_string(cls,x):
        """Convert an error code to a printable string"""
        
        x = cls.get_list_val(x)
        if cls.has(x):
            strval = ''
            for item in x:
                strval += str(item) + ', '
            strval = strval[:-2]
            return strval

        else:
            return None