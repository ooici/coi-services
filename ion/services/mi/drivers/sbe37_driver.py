#!/usr/bin/env python

"""
@package ion.services.mi.sbe37_driver
@file ion/services/mi/sbe37_driver.py
@author Edward Hunter
@brief Driver class for sbe37 CTD instrument.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

import logging
import time
import re

from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverChannel
from ion.services.mi.instrument_driver import DriverCommand
from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.instrument_driver import DriverEvent
from ion.services.mi.instrument_driver import DriverParameter
from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentStateException
from ion.services.mi.exceptions import InstrumentConnectionException
from ion.services.mi.common import InstErrorCode
from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol import InstrumentProtocol
from ion.services.mi.instrument_protocol import CommandResponseInstrumentProtocol
from ion.services.mi.instrument_fsm import InstrumentFSM

#import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')


class SBE37State(BaseEnum):
    """
    """
    UNCONFIGURED = DriverState.UNCONFIGURED
    DISCONNECTED = DriverState.DISCONNECTED
    COMMAND = DriverState.COMMAND
    AUTOSAMPLE = DriverState.AUTOSAMPLE
    
class SBE37Event(BaseEnum):
    """
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    CONFIGURE = DriverEvent.CONFIGURE
    INITIALIZE = DriverEvent.INITIALIZE
    CONNECT = DriverEvent.CONNECT
    DISCONNECT = DriverEvent.DISCONNECT
    DETACH = DriverEvent.DETACH
    EXECUTE = DriverEvent.EXECUTE
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    UPDATE_PARAMS = DriverEvent.UPDATE_PARAMS

class SBE37Channel(BaseEnum):
    """
    """
    CTD = DriverChannel.CTD
    ALL = DriverChannel.ALL

class SBE37Command(DriverCommand):
    pass

# Device prompts.
class SBE37Prompt(BaseEnum):
    """
    SBE37 io prompts.
    """
    COMMAND = 'S>'
    BAD_COMMAND = '?cmd S>'
    AUTOSAMPLE = 'S>\r\n'

SBE37_NEWLINE = '\r\n'

SBE37_SAMPLE = 'SBE37_SAMPLE'

# Device specific parameters.
class SBE37Parameter(DriverParameter):
    """
    Add sbe37 specific parameters here.
    """
    OUTPUTSAL = 'OUTPUTSAL'
    OUTPUTSV = 'OUTPUTSV'
    NAVG = 'NAVG'
    SAMPLENUM = 'SAMPLENUM'
    INTERVAL = 'INTERVAL'
    STORETIME = 'STORETIME'
    TXREALTIME = 'TXREALTIME'
    SYNCMODE = 'SYNCMODE'
    SYNCWAIT = 'SYNCWAIT'
    TCALDATE = 'TCALDATE'
    TA0 = 'TA0'
    TA1 = 'TA1'
    TA2 = 'TA2'
    TA3 = 'TA3'
    CCALDATE = 'CCALDATE'
    CG = 'CG'
    CH = 'CH'
    CI = 'CI'
    CJ = 'CJ'
    WBOTC = 'WBOTC'
    CTCOR = 'CTCOR'
    CPCOR = 'CPCOR'
    PCALDATE = 'PCALDATE'
    PA0 = 'PA0'
    PA1 = 'PA1'
    PA2 = 'PA2'
    PTCA0 = 'PTCA0'
    PTCA1 = 'PTCA1'
    PTCA2 = 'PTCA2'
    PTCB0 = 'PTCB0'
    PTCB1 = 'PTCB1'
    PTCB2 = 'PTCB2'
    POFFSET = 'POFFSET'
    RCALDATE = 'RCALDATE'
    RTCA0 = 'RTCA0'
    RTCA1 = 'RTCA1'
    RTCA2 = 'RTCA2'
        
###############################################################################
# Seabird Electronics 37-SMP MicroCAT protocol.
###############################################################################

class SBE37Protocol(CommandResponseInstrumentProtocol):
    """
    """
    def __init__(self, prompts, newline, evt_callback):
        """
        """
        CommandResponseInstrumentProtocol.__init__(self, evt_callback, prompts, newline)
        
        # Build protocol state machine.
        self._fsm = InstrumentFSM(SBE37State, SBE37Event, SBE37Event.ENTER,
                            SBE37Event.EXIT, InstErrorCode.UNHANDLED_EVENT)
        
        # Add handlers for all events.
        self._fsm.add_handler(SBE37State.UNCONFIGURED, SBE37Event.ENTER, self._handler_unconfigured_enter)
        self._fsm.add_handler(SBE37State.UNCONFIGURED, SBE37Event.EXIT, self._handler_unconfigured_exit)
        self._fsm.add_handler(SBE37State.UNCONFIGURED, SBE37Event.INITIALIZE, self._handler_unconfigured_initialize)
        self._fsm.add_handler(SBE37State.UNCONFIGURED, SBE37Event.CONFIGURE, self._handler_unconfigured_configure)
        self._fsm.add_handler(SBE37State.DISCONNECTED, SBE37Event.ENTER, self._handler_disconnected_enter)
        self._fsm.add_handler(SBE37State.DISCONNECTED, SBE37Event.EXIT, self._handler_disconnected_exit)
        self._fsm.add_handler(SBE37State.DISCONNECTED, SBE37Event.INITIALIZE, self._handler_disconnected_initialize)
        self._fsm.add_handler(SBE37State.DISCONNECTED, SBE37Event.CONFIGURE, self._handler_disconnected_configure)
        self._fsm.add_handler(SBE37State.DISCONNECTED, SBE37Event.CONNECT, self._handler_disconnected_connect)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.ENTER, self._handler_command_enter)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.EXIT, self._handler_command_exit)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.DISCONNECT, self._handler_command_disconnect)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.EXECUTE, self._handler_command_execute)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.GET, self._handler_command_autosample_get)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.SET, self._handler_command_set)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.UPDATE_PARAMS, self._handler_command_update_params)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.ENTER, self._handler_autosample_enter)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.EXIT, self._handler_autosample_exit)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.EXECUTE, self._handler_autosample_execute)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.GET, self._handler_command_autosample_get)

        # Start state machine.
        self._fsm.start(SBE37State.UNCONFIGURED)

        # Add build command handlers.
        self._add_build_handler('ds', self._build_simple_command)
        self._add_build_handler('dc', self._build_simple_command)
        self._add_build_handler('ts', self._build_simple_command)
        self._add_build_handler('startnow', self._build_simple_command)
        self._add_build_handler('stop', self._build_simple_command)
        self._add_build_handler('set', self._build_set_command)

        # Add parse response handlers.
        self._add_response_handler('ds', self._parse_dsdc_response)
        self._add_response_handler('dc', self._parse_dsdc_response)
        self._add_response_handler('ts', self._parse_ts_response)
        self._add_response_handler('set', self._parse_set_response)

        # Add sample handlers.
        self._sample_pattern = r'^#? *(-?\d+\.\d+), *(-?\d+\.\d+), *(-?\d+\.\d+)'
        self._sample_pattern += r'(, *(-?\d+\.\d+))?(, *(-?\d+\.\d+))?'
        self._sample_pattern += r'(, *(\d+) +([a-zA-Z]+) +(\d+), *(\d+):(\d+):(\d+))?'
        self._sample_pattern += r'(, *(\d+)-(\d+)-(\d+), *(\d+):(\d+):(\d+))?'        
        self._sample_regex = re.compile(self._sample_pattern)
        
        # Add parameter handlers to parameter dict.        
        self._add_param_dict(SBE37Parameter.OUTPUTSAL,
                             r'(do not )?output salinity with each sample',
                             lambda match : False if match.group(1) else True,
                             self._true_false_to_string)
        self._add_param_dict(SBE37Parameter.OUTPUTSV,
                             r'(do not )?output sound velocity with each sample',
                             lambda match : False if match.group(1) else True,
                             self._true_false_to_string)
        self._add_param_dict(SBE37Parameter.NAVG,
                             r'number of samples to average = (\d+)',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        self._add_param_dict(SBE37Parameter.SAMPLENUM,
                             r'samplenumber = (\d+), free = \d+',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        self._add_param_dict(SBE37Parameter.INTERVAL,
                             r'sample interval = (\d+) seconds',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        self._add_param_dict(SBE37Parameter.STORETIME,
                             r'(do not )?store time with each sample',
                             lambda match : False if match.group(1) else True,
                             self._true_false_to_string)
        self._add_param_dict(SBE37Parameter.TXREALTIME,
                             r'(do not )?transmit real-time data',
                             lambda match : False if match.group(1) else True,
                             self._true_false_to_string)
        self._add_param_dict(SBE37Parameter.SYNCMODE,
                             r'serial sync mode (enabled|disabled)',
                             lambda match : False if (match.group(1)=='disabled') else True,
                             self._true_false_to_string)
        self._add_param_dict(SBE37Parameter.SYNCWAIT,
                             r'wait time after serial sync sampling = (\d+) seconds',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        self._add_param_dict(SBE37Parameter.TCALDATE,
                             r'temperature: +((\d+)-([a-zA-Z]+)-(\d+))',
                             lambda match : self._string_to_date(match.group(1), '%d-%b-%y'),
                             self._date_to_string)
        self._add_param_dict(SBE37Parameter.TA0,
                             r' +TA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.TA1,
                             r' +TA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.TA2,
                             r' +TA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.TA3,
                             r' +TA3 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.CCALDATE,
                             r'conductivity: +((\d+)-([a-zA-Z]+)-(\d+))',
                             lambda match : self._string_to_date(match.group(1), '%d-%b-%y'),
                             self._date_to_string)
        self._add_param_dict(SBE37Parameter.CG,
                             r' +G = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.CH,
                             r' +H = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.CI,
                             r' +I = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.CJ,
                             r' +J = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.WBOTC,
                             r' +WBOTC = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.CTCOR,
                             r' +CTCOR = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.CPCOR,
                             r' +CPCOR = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PCALDATE,
                             r'pressure .+ ((\d+)-([a-zA-Z]+)-(\d+))',
                             lambda match : self._string_to_date(match.group(1), '%d-%b-%y'),
                             self._date_to_string)
        self._add_param_dict(SBE37Parameter.PA0,
                             r' +PA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PA1,
                             r' +PA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PA2,
                             r' +PA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PTCA0,
                             r' +PTCA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PTCA1,
                             r' +PTCA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PTCA2,
                             r' +PTCA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PTCB0,
                             r' +PTCSB0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PTCB1,
                             r' +PTCSB1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.PTCB2,
                             r' +PTCSB2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.POFFSET,
                             r' +POFFSET = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.RCALDATE,
                             r'rtc: +((\d+)-([a-zA-Z]+)-(\d+))',
                             lambda match : self._string_to_date(match.group(1), '%d-%b-%y'),
                             self._date_to_string)
        self._add_param_dict(SBE37Parameter.RTCA0,
                             r' +RTCA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.RTCA1,
                             r' +RTCA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._add_param_dict(SBE37Parameter.RTCA2,
                             r' +RTCA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)


    ########################################################################
    # Protocol connection interface.
    ########################################################################

    def initialize(self, timeout=10):
        """
        """
        
        # Construct state machine params and fire event.
        fsm_params = {'timeout':timeout}
        return self._fsm.on_event(SBE37Event.INITIALIZE, fsm_params)
    
    def configure(self, config, timeout=10):
        """
        """

        # Construct state machine params and fire event.
        fsm_params = {'config':config, 'timeout':timeout}
        return self._fsm.on_event(SBE37Event.CONFIGURE, fsm_params)
    
    def connect(self, timeout=10):
        """
        """

        # Construct state machine params and fire event.
        fsm_params = {'timeout':timeout}        
        return self._fsm.on_event(SBE37Event.CONNECT, fsm_params)
    
    def disconnect(self, timeout=10):
        """
        """

        # Construct state machine params and fire event.
        fsm_params = {'timeout':timeout}        
        return self._fsm.on_event(SBE37Event.DISCONNECT, fsm_params)
    
    def detach(self, timeout=10):
        """
        """

        # Construct state machine params and fire event.
        fsm_params = {'timeout':timeout}        
        return self._fsm.on_event(SBE37Event.DETACH, fsm_params)

    ########################################################################
    # Protocol command interface.
    ########################################################################

    def get(self, parameter, timeout=10):
        """
        """
        fsm_params = {'parameter':parameter, 'timeout':timeout}
        return self._fsm.on_event(SBE37Event.GET, fsm_params)
    
    def set(self, parameter, val, timeout=10):
        """
        """
        fsm_params = {'parameter':parameter, 'value':val, 'timeout':timeout}
        return self._fsm.on_event(SBE37Event.SET, fsm_params)

    def execute(self, command, timeout=10):
        """
        """
        fsm_params = {'command':command, 'timeout':timeout}
        return self._fsm.on_event(SBE37Event.EXECUTE, fsm_params)

    def execute_direct(self, bytes):
        """
        """
        fsm_params = {'bytes':bytes}
        return self._fsm.on_event(SBE37Event.EXECUTE, fsm_params)
    
    def update_params(self, timeout=10):
        """
        """
        fsm_params = {'timeout':timeout}
        return self._fsm.on_event(SBE37Event.UPDATE_PARAMS, fsm_params)
    
    ########################################################################
    # Protocol query interface.
    ########################################################################
    
    def get_status(self):
        """
        """
        pass
    
    def get_capabilities(self):
        """
        """
        pass

    def get_current_state(self):
        """
        """
        return self._fsm.get_current_state()

    ########################################################################
    # State handlers
    ########################################################################

    ########################################################################
    # SBE37State.UNCONFIGURED
    ########################################################################
    
    def _handler_unconfigured_enter(self, params):
        """
        """
        
        mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.UNCONFIGURED)
            
        # Initialize driver configuration.
        try:
            timeout = params['timeout']
            
        except (TypeError, KeyError):
            timeout = 10
        
        # Initialize throws no exceptions.
        InstrumentProtocol.initialize(self, timeout)
    
    def _handler_unconfigured_exit(self, params):
        """
        """
        pass

    def _handler_unconfigured_initialize(self, params):
        """
        """
        next_state = None
        result = InstErrorCode.OK
        
        # Reenter initialize.
        next_state = SBE37State.UNCONFIGURED

        return (next_state, result)

    def _handler_unconfigured_configure(self, params):
        """
        """
        # Attempt to configure driver, switch to disconnected if successful.
        try:
            config = params['config']
        
        except TypeError:
            # The params is not a dict. Fail and stay here.
            result = InstErrorCode.REQUIRED_PARAMETER
            next_state = None
        
        else:
            try:
                timeout = params['timeout']
            
            except KeyError:
                timeout = 10

            try:
                InstrumentProtocol.configure(self, config, timeout)
                
            except (TypeError, KeyError, InstrumentConnectionException):
                # Config is not a dict., e.g. None.
                # Config is missing required keys.
                # Config specifies invalid connection method.
                result = InstErrorCode.INVALID_PARAMETER
                next_state = None
                
            #except:
            #    # Unknown exception, do not proceed.
            #    result = InstErrorCode.UNKNOWN_ERROR
            #    next_state = None
                
            # Everything worked, set next state.
            else:
                next_state = SBE37State.DISCONNECTED
                result = InstErrorCode.OK
                
        return (next_state, result)
        
    ########################################################################
    # SBE37State.DISCONNECTED
    ########################################################################

    def _handler_disconnected_enter(self, params):
        """
        """
        mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.DISCONNECTED)

    def _handler_disconnected_exit(self, params):
        """
        """
        pass

    def _handler_disconnected_initialize(self, params):
        """
        """
        result = InstErrorCode.OK
        
        # Switch to unconfigured to initialize comms.
        next_state = SBE37State.UNCONFIGURED

        return (next_state, result)

    def _handler_disconnected_configure(self, params):
        """
        """

        # Attempt to configure driver, switch to disconnected if successful.
        try:
            config = params['config']
        
        except TypeError:
        # The params is not a dict. Fail and initialize comms.
            result = InstErrorCode.REQUIRED_PARAMETER
            next_state = SBE37State.UNCONFIGURED
            
        else:
            try:
                timeout = params['timeout']
            
            except KeyError:
                # Use a default timeout.
                timeout = 10

            try:
                InstrumentProtocol.configure(self, config, timeout)
                
            except (TypeError, KeyError, InstrumentConnectionException):
                # Config is not valid, initialize comms.
                # Config is not a dict., e.g. None.
                # Config is missing required keys.
                # Config specifies invalid connection method.
                result = InstErrorCode.INVALID_PARAMETER
                next_state = SBE37State.UNCONFIGURED

            #except:
            #    # Unknown exception, return to unconfigured.
            #    result = InstErrorCode.UNKNOWN_ERROR
            #    next_state = SBE37State.UNCONFIGURED
            
            else:
                # Conif successful, stay here.
                next_state = None
                result = InstErrorCode.OK
                

        return (next_state, result)

    def _handler_disconnected_connect(self, params):
        """
        @throw InstrumentTimeoutException on timeout
        """
        try:
            timeout = params['timeout']
            
        except (TypeError, KeyError):
            # Use a default timeout.
            timeout = 10
        
        try:
            InstrumentProtocol.connect(self, timeout)
            prompt = self._wakeup(timeout)
            if prompt == SBE37Prompt.COMMAND:
                next_state = SBE37State.COMMAND
    
            elif prompt == SBE37Prompt.AUTOSAMPLE:
                next_state = SBE37State.AUTOSAMPLE
        
        except InstrumentConnectionException:
            # Connection failed, fail and stay here.
            next_state = None
            result = InstErrorCode.DRIVER_CONNECT_FAILED
        
        except InstrumentTimeoutException:
            # Timeout connecting or waking device. Stay disconnected.
            InstrumentProtocol.disconnect(self)
            next_state = None
            result = InstErrorCode.DRIVER_CONNECT_FAILED
            
        #except:
        #    # Unknown exception, stay disconnected.
        #    next_state = None
        #    result = InstErrorCode.UNKNOWN_ERROR
            
        else:
            next_state = SBE37State.COMMAND
            result = InstErrorCode.OK

        return (next_state, result)

    ########################################################################
    # SBE37State.COMMAND
    ########################################################################

    def _handler_command_enter(self, params):
        """
        """
        mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.COMMAND)
        self._update_params()

    def _handler_command_exit(self, params):
        """
        """
        pass

    def _handler_command_disconnect(self, params):
        """
        """
        try:
            timeout = params['timeout']
            
        except (TypeError, KeyError):
            timeout = 10
        
        try:
            InstrumentProtocol.disconnect(self, timeout)
            next_state = SBE37State.DISCONNECTED

        except InstrumentConnectionException:
            # Disconnect failed. Fail and stay here.
            next_state = None
            result = InstErrorCode.DISCONNECT_FAILED
            
        #except:
        #    next_state = None
        #    result = InstErrorCode.UNKNOWN_ERROR
            
        else:
            next_state = SBE37State.DISCONNECTED
            result = InstErrorCode.OK

        return (next_state, result)

    def _handler_command_execute(self, params):
        """
        @throw InstrumentProtocolException on invalid command
        """
        
        try:
            command = params['command']
            cmd = command[0]
            
        except (TypeError, KeyError, IndexError):
            # Missing parameter, fail and stay here.
            next_state = None
            result = InstErrorCode.REQUIRED_PARAMETER
            
        else:
            try:
                timeout = params['timeout']
                
            except KeyError:
                timeout = 10

                
            if cmd == SBE37Command.ACQUIRE_SAMPLE:
                try:
                    result = self._do_cmd_resp('ts', timeout=timeout)
                    next_state = None
                
                except InstruemntTimeoutException:
                    next_state = None
                    result = InstErrorCode.TIMEOUT
                        
            elif cmd == SBE37Command.START_AUTO_SAMPLING:
                try:
                    self._do_cmd_no_resp('startnow', timeout=timeout)                
                    next_state = SBE37State.AUTOSAMPLE
                    result = InstErrorCode.OK
                
                except InstruemntTimeoutException:
                    next_state = None
                    result = InstErrorCode.TIMEOUT

            else:
                # Invalid command, fail and stay here.
                result = InstErrorCode.INVALID_COMMAND
                next_state = None
                
        return (next_state, result)

    def _handler_command_set(self, params):
        """
        """
        try:
            parameter = params['parameter']
            value = params['value']
            
        except (TypeError, KeyError):
            # Missing parameter, fail and stay here.
            next_state = None
            result = InstErrorCode.REQUIRED_PARAMETER
            
        else:
            try:
                timeout = params['timeout']
                
            except KeyError:
                timeout = 10

            try:
                result = self._do_cmd_resp('set', parameter, value, timeout=timeout)            
                next_state = None
                
            except InstrumentTimeoutException:
                next_state = None
                result = InstErrorCode.TIMEOUT
        
        return (next_state, result)

    def _handler_command_update_params(self, params):
        """
        """
        try:
            timeout = params['timeout']
            
        except (TypeError, KeyError):
            timeout = 10

        try:
            self._update_params(timeout)
            next_state = None
            result = InstErrorCode.OK
        
        except InstrumentTimeoutError:
            next_state = None
            result = InstErrorCode.TIMEOUT
        
        return (next_state, result)

    ########################################################################
    # SBE37State.AUTOSAMPLE
    ########################################################################

    def _handler_autosample_enter(self, params):
        """
        """
        mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.AUTOSAMPLE)
        
    def _handler_autosample_exit(self, params):
        """
        """
        pass

    def _handler_autosample_execute(self, params):
        """
        @throw InstrumentProtocolException on invalid command
        """
        try:
            command = params['command']
            cmd = command[0]
            
        except (TypeError, KeyError, IndexError):
            # Missing parameter, fail and stay here.
            next_state = None
            result = InstErrorCode.REQUIRED_PARAMETER
            
        else:
            try:
                timeout = params['timeout']
                
            except KeyError:
                timeout = 10
                
            if cmd == SBE37Command.STOP_AUTO_SAMPLING:
                try:
                    prompt = None
                    while prompt != SBE37Prompt.AUTOSAMPLE:
                        prompt = self._wakeup(timeout)
                    self._do_cmd_resp('stop')
                    prompt = None
                    while prompt != SBE37Prompt.COMMAND:
                        prompt = self._wakeup(timeout)
                    next_state = SBE37State.COMMAND
                    result = InstErrorCode.OK
                    
                except InstrumentTimeoutException:
                    next_state = None
                    result = InstErrorCode.TIMEOUT

            else:
                next_state = None
                result = InstErrorCode.INVALID_COMMAND

        return (next_state, result)

    ########################################################################
    # SBE37State.COMMAND and SBE37State.AUTOSAMPLE common handlers.
    ########################################################################

    def _handler_command_autosample_get(self, params):
        """
        """
        try:
            parameter = params['parameter']
            
        except (TypeError, KeyError):
            # Missing parameter, fail and stay here.
            next_state = None
            result = InstErrorCode.REQUIRED_PARAMETER
            
        else:
            try:
                timeout = params['timeout']
        
            except KeyError:
                timeout = 10

            try:
                result = self._get_param_dict(parameter)
                next_state = None

            except KeyError:
                result = InstErrorCode.INVALID_PARAMETER
                next_state = None
            
        return (next_state, result)

    ########################################################################
    # Private helpers
    ########################################################################
    
    def _got_data(self, data):
        """
        """
        CommandResponseInstrumentProtocol._got_data(self, data)
        
        # Only keep the latest characters in the prompt buffer.
        if len(self._promptbuf)>7:
            self._promptbuf = self._promptbuf[-7:]
            
        # If we are streaming, process the line buffer for samples.
        if self._fsm.get_current_state() == SBE37State.AUTOSAMPLE:
            self._process_streaming_data()
        
    def _process_streaming_data(self):
        """
        """
        if self.eoln in self._linebuf:
            lines = self._linebuf.split(self.eoln)
            self._linebuf = lines[-1]
            lines = lines[0:-1]
            mi_logger.debug('Streaming data received: %s',str(lines))
    
    def _send_wakeup(self):
        """
        """
        self._logger_client.send(SBE37_NEWLINE)

    def _update_params(self, timeout=10):
        """
        """
        self._do_cmd_resp('ds',timeout=timeout)
        self._do_cmd_resp('dc',timeout=timeout)

    def _extract_sample(self, sampledata):
        """
        """
        lines = sampledata.split(SBE37_NEWLINE)
        for line in lines:
            if self._sample_regex.match(line):
                pass
        
    def _build_simple_command(self, cmd):
        """
        """
        return cmd+SBE37_NEWLINE
    
    def _build_set_command(self, cmd, param, val):
        """
        """
        str_val = self._format_param_dict(param, val)
        set_cmd = '%s=%s' % (param, str_val)
        set_cmd = set_cmd + SBE37_NEWLINE
        return set_cmd

    def _parse_dsdc_response(self, response, prompt):
        """
        """
        mi_logger.debug('Got dcds response: %s', repr(response))
        for line in response.split(SBE37_NEWLINE):
            self._update_param_dict(line)
        
    def _parse_ts_response(self, response, prompt):
        """
        """
        mi_logger.debug('Got ts response: %s', repr(response))
        # Match sample for success.
        success = InstErrorCode.OK
        result = None
        
        return (success,response)

    def _parse_set_response(self, response, prompt):
        """
        """
        if prompt == SBE37Prompt.COMMAND:
            return InstErrorCode.OK
        else:
            return InstErrorCode.BAD_DRIVER_COMMAND

    ########################################################################
    # Static helpers to format set commands.
    ########################################################################

    @staticmethod
    def _true_false_to_string(v):
        """
        Write a boolean value to string formatted for sbe37 set operations.
        @param v a boolean value.
        @retval A yes/no string formatted for sbe37 set operations, or
            None if the input is not a valid bool.
        """
        
        if not isinstance(v,bool):
            return None
        if v:
            return 'y'
        else:
            return 'n'

    @staticmethod
    def _int_to_string(v):
        """
        Write an int value to string formatted for sbe37 set operations.
        @param v An int val.
        @retval an int string formatted for sbe37 set operations, or None if
            the input is not a valid int value.
        """
        
        if not isinstance(v,int):
            return None
        else:
            return '%i' % v

    @staticmethod
    def _float_to_string(v):
        """
        Write a float value to string formatted for sbe37 set operations.
        @param v A float val.
        @retval a float string formatted for sbe37 set operations, or None if
            the input is not a valid float value.
        """

        if not isinstance(v,float):
            return None
        else:
            return '%e' % v

    @staticmethod
    def _date_to_string(v):
        """
        Write a date tuple to string formatted for sbe37 set operations.
        @param v a date tuple: (day,month,year).
        @retval A date string formatted for sbe37 set operations,
            or None if the input is not a valid date tuple.
        """

        if not isinstance(v,(list,tuple)):
            return None
        
        if not len(v)==3:
            return None
        
        months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep',
                  'Oct','Nov','Dec']
        day = v[0]
        month = v[1]
        year = v[2]
        
        if len(str(year)) > 2:
            year = int(str(year)[-2:])
        
        if not isinstance(day,int) or day < 1 or day > 31:
            return None
        
        if not isinstance(month,int) or month < 1 or month > 12:
            return None

        if not isinstance(year,int) or year < 0 or year > 99:
            return None
        
        return '%02i-%s-%02i' % (day,months[month-1],year)

    @staticmethod
    def _string_to_date(datestr,fmt):
        """
        Extract a date tuple from an sbe37 date string.
        @param str a string containing date information in sbe37 format.
        @retval a date tuple, or None if the input string is not valid.
        """
        if not isinstance(datestr,str):
            return None
        try:
            date_time = time.strptime(datestr,fmt)
            date = (date_time[2],date_time[1],date_time[0])

        except ValueError:
            return None
                        
        return date

###############################################################################
# Seabird Electronics 37-SMP MicroCAT driver.
###############################################################################

class SBE37Driver(InstrumentDriver):
    """
    class docstring
    """
    def __init__(self, evt_callback):
        """
        method docstring
        """
        InstrumentDriver.__init__(self, evt_callback)
        
        # Build the protocol for CTD channel.
        protocol = SBE37Protocol(SBE37Prompt, SBE37_NEWLINE, evt_callback)
        self._channels = {SBE37Channel.CTD:protocol}
            
    ########################################################################
    # Channel connection interface.
    ########################################################################
    
    def initialize(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        (overall_success, result, valid_channels) = \
                self._check_channel_args(channels)

        for channel in valid_channels:
            init_result = self._channels[channel].initialize(timeout)
            result[channel] = init_result
            if InstErrorCode.is_error(init_result):
                overall_success = init_result
                
        return (overall_success, result)
    
    def configure(self, configs, timeout=10):
        """
        """        
        if configs == None or not isinstance(configs, dict):
            return (InstErrorCode.INVALID_PARAMETER, None)
        
        channels = configs.keys()
        (overall_success, result, valid_channels) = \
                self._check_channel_args(channels)

        for channel in valid_channels:
            config = configs[channel]
            config_result = self._channels[channel].configure(config, timeout)
            result[channel] = config_result
            if InstErrorCode.is_error(config_result):
                overall_success = config_result

        return (overall_success, result)
    
    def connect(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        (overall_success, result, valid_channels) = \
                self._check_channel_args(channels)

        for channel in valid_channels:
            connect_result = self._channels[channel].connect(timeout)
            result[channel] = connect_result
            if InstErrorCode.is_error(connect_result):
                overall_success = connect_result
                
        return (overall_success, result)
    
    def disconnect(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        (overall_success, result, valid_channels) = \
                self._check_channel_args(channels)

        for channel in valid_channels:
            disconnect_result = self._channels[channel].disconnect(timeout)
            result[channel] = disconnect_result
            if InstErrorCode.is_error(disconnect_result):
                overall_success = disconnect_result
                
        return (overall_success, result)
            
    def detach(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        (overall_success, result, valid_channels) = \
                self._check_channel_args(channels)

        for channel in valid_channels:
            detach_result = self._channels[channel].detach(timeout)
            result[channel] = detach_result
            if InstErrorCode.is_error(detach_result):
                overall_success = detach_result
                
        return (overall_success, result)

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, timeout=10):
        """
        """

        if params == None or not isinstance(params, (list, tuple)):
            return (InstErrorCode.INVALID_PARAMETER, None)
        
        (overall_success, result, params) = self._check_get_args(params)
        
        for (channel, parameter) in params:        
            success = InstErrorCode.OK
            val = self._channels[channel].get(parameter, timeout)
            if InstErrorCode.is_error(val):
                overall_success = InstErrorCode.GET_DEVICE_ERR
                mi_logger.debug('Error retrieving parameter %s', parameter)
            result[(channel, parameter)] = val
                
        # Return overall success and individual results.
        return (overall_success, result)
            
    def set(self, params, timeout=10):
        """
        """
        if params == None or not isinstance(params, dict):
            return (InstErrorCode.INVALID_PARAMETER, None)
        
        (overall_success, result, params) = self._check_set_args(params)
        
        updated_channels = []
        
        # Process each parameter-value pair.
        for (key, val) in params.iteritems():
            channel = key[0]
            parameter = key[1]
            set_result = self._channels[channel].set(parameter, val)
            if InstErrorCode.is_error(set_result):
                overall_success = InstErrorCode.SET_DEVICE_ERR
            elif channel not in updated_channels:
                updated_channels.append(channel)                
            result[key] = set_result
        
        for channel in updated_channels:
            self._channels[channel].update_params(timeout)
        
        # Additional checking can go here.
        
        # Return overall success and individual results.
        return (overall_success, result)

    def execute(self, channels=[SBE37Channel.CTD], command=[], timeout=10):
        """
        """
        (overall_success, result, valid_channels) = \
                self._check_channel_args(channels)

        if not isinstance(command, (list, tuple)) or len(command) == 0:
            overall_success == InstErrorCode.INVALID_PARAMETER
            
        else:
            for channel in valid_channels:
                cmd_result = \
                    self._channels[SBE37Channel.CTD].execute(command, timeout)                
                result[channel] = cmd_result
                if InstErrorCode.is_error(cmd_result):
                    overall_success = InstErrorCode.EXE_DEVICE_ERR

        return (overall_success, result)
        
    def execute_direct(self, channels=[SBE37Channel.CTD], bytes=''):
        """
        """
        pass
    
    ########################################################################
    # TBD.
    ########################################################################    
    
    def get_status(self, params, timeout=10):
        """
        """
        pass
    
    def get_capabilities(self, params, timeout=10):
        """
        """
        pass

    def get_channels(self):
        """
        """
        return SBE37Channels.list()
        
    def get_current_state(self, channels=[SBE37Channel.CTD]):
        """
        """
        (overall_success, result, valid_channels) = \
                self._check_channel_args(channels)

        for channel in valid_channels:
            state = self._channels[channel].get_current_state()
            result[channel] = state

        return (overall_success, result)

    ########################################################################
    # Private helpers.
    ########################################################################    

    @staticmethod
    def _check_channel_args(channels):
        """
        """
        overall_success = InstErrorCode.OK
        valid_channels = []
        result = {}
        
        if channels == None or not isinstance(channels, (list, tuple)):
            overall_success = InstErrorCode.REQUIRED_PARAMETER
            
        elif len(channels) == 0:
            overall_success = InstErrorCode.REQUIRED_PARAMETER
            
        else:
            clist = SBE37Channel.list()
            if SBE37Channel.ALL in clist:
                clist.remove(SBE37Channel.ALL)

            # Expand "ALL channel keys.
            if SBE37Channel.ALL in channels:
                channels += clist
                channels = [c for c in channels if c != SBE37Channel.ALL]

            # Make unique.
            channels = list(set(channels))

            # Separate valid and invalid channels.
            valid_channels = [c for c in channels if c in clist]
            invalid_channels = [c for c in channels if c not in clist]
            
            # Build result dict with invalid entries.
            for c in invalid_channels:
                result[c] = InstErrorCode.INVALID_CHANNEL
                overall_success = InstErrorCode.INVALID_CHANNEL
                                        
        return (overall_success, result, valid_channels)

    @staticmethod
    def _check_get_args(params):
        """
        """
        overall_success = InstErrorCode.OK
        valid_params = []
        result = {}
        
        if params == None or not isinstance(params, (list, tuple)):
            overall_success = InstErrorCode.REQUIRED_PARAMETER
            
        elif len(params) == 0:
            overall_success = InstErrorCode.REQUIRED_PARAMETER
            
        else:
            temp_list = []
            
            plist = SBE37Parameter.list()
            if SBE37Parameter.ALL in plist:
                plist.remove(SBE37Parameter.ALL)
            clist = SBE37Channel.list()
            if SBE37Channel.ALL in clist:
                clist.remove(SBE37Channel.ALL)

            # Expand and remove "ALL" channel specifiers.
            params += [(c, parameter) for (channel, parameter) in params
                if channel == SBE37Channel.ALL for c in clist]
            params = [(c, p) for (c, p) in params if c != SBE37Channel.ALL]

            # Expand and remove "ALL" parameter specifiers.
            params += [(channel, p) for (channel, parameter) in params
                if parameter == SBE37Parameter.ALL for p in plist]
            params = [(c, p) for (c, p) in params if p != SBE37Parameter.ALL]

            # Make list unique.
            params = list(set(params))
            
            # Separate valid and invalid params.
            invalid_params = [(c, p) for (c, p) in params if c in clist and p not in plist]
            invalid_channels = [(c, p) for (c, p) in params if c not in clist]
            valid_params = [(c, p) for (c, p) in params if c in clist and p in plist]

            # Build result
            for (c, p) in invalid_params:
                result[(c, p)] = InstErrorCode.INVALID_PARAMETER
                overall_success = InstErrorCode.GET_DEVICE_ERR
            for (c, p) in invalid_channels:
                result[(c, p)] = InstErrorCode.INVALID_CHANNEL
                overall_success = InstErrorCode.GET_DEVICE_ERR

        return (overall_success, result, valid_params)
   
    @staticmethod
    def _check_set_args(params):
        """
        """
        overall_success = InstErrorCode.OK
        valid_params = {}
        result = {}
        
        if params == None or not isinstance(params, dict):
            overall_success = InstErrorCode.REQUIRED_PARAMETER
            
        elif len(params) == 0:
            overall_success = InstErrorCode.REQUIRED_PARAMETER
            
        else:
            
            plist = SBE37Parameter.list()
            if SBE37Parameter.ALL in plist:
                plist.remove(SBE37Parameter.ALL)
            clist = SBE37Channel.list()
            if SBE37Channel.ALL in clist:
                clist.remove(SBE37Channel.ALL)

            # Expand and remove "ALL" channel specifiers.
            for (key, val) in params.iteritems():
                if key[0] == SBE37Channel.ALL:
                    for c in clist: params[(c, key[1])] = val
                    params.pop(key)

            # Remove invalid parameters.
            temp_params = params.copy()
            for (key, val) in temp_params.iteritems():
                if key[0] not in clist:
                    result[key] = InstErrorCode.INVALID_CHANNEL
                    overall_success = InstErrorCode.SET_DEVICE_ERR
                    params.pop(key)
                
                elif key[1] not in plist:
                    result[key] = InstErrorCode.INVALID_PARAMETER
                    overall_success = InstErrorCode.SET_DEVICE_ERR
                    params.pop(key)
                
        return (overall_success, result, params)

    ########################################################################
    # Misc and temp.
    ########################################################################

    def driver_echo(self, msg):
        """
        """
        echo = 'driver_echo: %s' % msg
        return echo

