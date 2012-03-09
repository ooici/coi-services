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
import datetime
import string

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
from ion.services.mi.exceptions import RequiredParameterException
from ion.services.mi.common import InstErrorCode
from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol import InstrumentProtocol
from ion.services.mi.instrument_protocol import CommandResponseInstrumentProtocol
from ion.services.mi.instrument_fsm_args import InstrumentFSM

#import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')


class SBE37State(BaseEnum):
    """
    """
    UNCONFIGURED = DriverState.UNCONFIGURED
    DISCONNECTED = DriverState.DISCONNECTED
    COMMAND = DriverState.COMMAND
    AUTOSAMPLE = DriverState.AUTOSAMPLE
    DIRECT = DriverState.DIRECT
    
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
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    TEST = DriverEvent.TEST
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    UPDATE_PARAMS = DriverEvent.UPDATE_PARAMS
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    START_DIRECT = DriverEvent.START_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT

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
        
        
PACKET_CONFIG = {
        'ctd_parsed' : ('prototype.sci_data.ctd_stream', 'ctd_stream_packet'),
        'ctd_raw' : None            
}

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
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.GET, self._handler_command_autosample_get)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.SET, self._handler_command_set)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.ACQUIRE_SAMPLE, self._handler_command_acquire_sample)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.START_AUTOSAMPLE, self._handler_command_start_autosample)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.TEST, self._handler_command_test)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.UPDATE_PARAMS, self._handler_command_update_params)
        self._fsm.add_handler(SBE37State.COMMAND, SBE37Event.START_DIRECT, self._handler_command_start_direct)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.ENTER, self._handler_autosample_enter)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.EXIT, self._handler_autosample_exit)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.STOP_AUTOSAMPLE, self._handler_autosample_stop_autosample)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.GET, self._handler_command_autosample_get)
        self._fsm.add_handler(SBE37State.DIRECT, SBE37Event.ENTER, self._handler_direct_enter)
        self._fsm.add_handler(SBE37State.DIRECT, SBE37Event.EXIT, self._handler_direct_exit)
        self._fsm.add_handler(SBE37State.DIRECT, SBE37Event.EXECUTE_DIRECT, self._handler_direct_execute_direct)
        self._fsm.add_handler(SBE37State.DIRECT, SBE37Event.STOP_DIRECT, self._handler_direct_stop_direct)

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

    def initialize(self, *args, **kwargs):
        """
        """
        
        # Construct state machine params and fire event.
        return self._fsm.on_event(SBE37Event.INITIALIZE, *args, **kwargs)
    
    def configure(self, *args, **kwargs):
        """
        """

        # Construct state machine params and fire event.
        return self._fsm.on_event(SBE37Event.CONFIGURE, *args, **kwargs)
    
    def connect(self, *args, **kwargs):
        """
        """

        # Construct state machine params and fire event.
        return self._fsm.on_event(SBE37Event.CONNECT, *args, **kwargs)
    
    def disconnect(self, *args, **kwargs):
        """
        """

        # Construct state machine params and fire event.
        return self._fsm.on_event(SBE37Event.DISCONNECT, *args, **kwargs)
    
    def detach(self, *args, **kwargs):
        """
        """

        # Construct state machine params and fire event.
        return self._fsm.on_event(SBE37Event.DETACH, *args, **kwargs)

    ########################################################################
    # Protocol command interface.
    ########################################################################

    def get(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(SBE37Event.GET, *args, **kwargs)
    
    def set(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(SBE37Event.SET, *args, **kwargs)

    def execute_direct(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(SBE37Event.EXECUTE, *args, **kwargs)
    
    def execute_direct_access(self, data):
        """
        """
        return self._fsm.on_event(SBE37Event.EXECUTE_DIRECT, data)
    
    def execute_start_direct_access(self):
        """
        """
        return self._fsm.on_event(SBE37Event.START_DIRECT)
    
    def execute_stop_direct_access(self):
        """
        """
        return self._fsm.on_event(SBE37Event.STOP_DIRECT)
    
    def execute_acquire_sample(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(SBE37Event.ACQUIRE_SAMPLE, *args, **kwargs)

    def execute_start_autosample(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(SBE37Event.START_AUTOSAMPLE, *args, **kwargs)

    def execute_stop_autosample(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(SBE37Event.STOP_AUTOSAMPLE, *args, **kwargs)

    def execute_test(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(SBE37Event.TEST, *args, **kwargs)

    def update_params(self, *args, **kwargs):
        """
        """
        return self._fsm.on_event(SBE37Event.UPDATE_PARAMS, *args, **kwargs)
            
    ########################################################################
    # Protocol query interface.
    ########################################################################
    
    
    def get_resource_commands(self):
        """
        """
        return [cmd for cmd in dir(self) if cmd.startswith('execute_')]    
    
    def get_resource_params(self):
        """
        """
        return self._get_param_dict_names()
        
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
    
    def _handler_unconfigured_enter(self, *args, **kwargs):
        """
        """
        
        mi_logger.info('channel %s entered state %s', SBE37Channel.CTD,
                           SBE37State.UNCONFIGURED)
        self._publish_state_change(SBE37State.UNCONFIGURED)
        
        # Initialize throws no exceptions.
        InstrumentProtocol.initialize(self, *args, **kwargs)
    
    def _handler_unconfigured_exit(self, *args, **kwargs):
        """
        """
        pass

    def _handler_unconfigured_initialize(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        # Reenter initialize.
        next_state = SBE37State.UNCONFIGURED

        return (next_state, result)

    def _handler_unconfigured_configure(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        try:
            InstrumentProtocol.configure(self, *args, **kwargs)
                
        except (TypeError, KeyError, InstrumentConnectionException, IndexError):
            result = InstErrorCode.INVALID_PARAMETER
            next_state = None
                
        # Everything worked, set next state.
        else:
            next_state = SBE37State.DISCONNECTED
                
        return (next_state, result)
        
    ########################################################################
    # SBE37State.DISCONNECTED
    ########################################################################

    def _handler_disconnected_enter(self, *args, **kwargs):
        """
        """
        mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.DISCONNECTED)
        self._publish_state_change(SBE37State.DISCONNECTED)

    def _handler_disconnected_exit(self, *args, **kwargs):
        """
        """
        pass

    def _handler_disconnected_initialize(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        # Switch to unconfigured to initialize comms.
        next_state = SBE37State.UNCONFIGURED

        return (next_state, result)

    def _handler_disconnected_configure(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        try:
            InstrumentProtocol.configure(self, *args, **kwargs)
                
        except (TypeError, KeyError, InstrumentConnectionException, IndexError):
            result = InstErrorCode.INVALID_PARAMETER
            next_state = SBE37State.UNCONFIGURED

        return (next_state, result)

    def _handler_disconnected_connect(self, *args, **kwargs):
        """
        @throw InstrumentTimeoutException on timeout
        """        
        next_state = None
        result = None

        try:
            InstrumentProtocol.connect(self, *args, **kwargs)
            timeout = kwargs.get('timeout', 10)
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
            InstrumentProtocol.disconnect(self, *args, **kwargs)
            next_state = None
            result = InstErrorCode.DRIVER_CONNECT_FAILED

        return (next_state, result)

    ########################################################################
    # SBE37State.COMMAND
    ########################################################################

    def _handler_command_enter(self, *args, **kwargs):
        """
        """
        mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.COMMAND)
        self._publish_state_change(SBE37State.COMMAND)
        self._update_params(*args, **kwargs)

    def _handler_command_exit(self, *args, **kwargs):
        """
        """
        pass

    def _handler_command_disconnect(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        try:
            InstrumentProtocol.disconnect(self, *args, **kwargs)
            next_state = SBE37State.DISCONNECTED

        except InstrumentConnectionException:
            # Disconnect failed. Fail and stay here.
            next_state = None
            result = InstErrorCode.DISCONNECT_FAILED
            
        else:
            next_state = SBE37State.DISCONNECTED
            result = InstErrorCode.OK

        return (next_state, result)

    def _handler_command_set(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        try:
            result = self._do_cmd_resp('set', *args, **kwargs)            
            next_state = None
            
        except InstrumentTimeoutException:
            next_state = None
            result = InstErrorCode.TIMEOUT
        
        except IndexError:
            next_state = None
            result = InstErrorCode.REQUIRED_PARAMETER
        
        return (next_state, result)

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        try:
            result = self._do_cmd_resp('ts', *args, **kwargs)
        
        except InstrumentTimeoutException:
            result = InstErrorCode.TIMEOUT

        return (next_state, result)

    def _handler_command_start_autosample(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        try:
            self._do_cmd_no_resp('startnow', *args, **kwargs)                
            next_state = SBE37State.AUTOSAMPLE
        
        except InstrumentTimeoutException:
            result = InstErrorCode.TIMEOUT

        return (next_state, result)

    def _handler_command_test(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        return (next_state, result)

    def _handler_command_update_params(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None

        try:
            self._update_params(*args, **kwargs)
        
        except InstrumentTimeoutError:
            result = InstErrorCode.TIMEOUT
        
        return (next_state, result)

    def _handler_command_start_direct(self):
        """
        """
        next_state = None
        result = None

        next_state = SBE37State.DIRECT
        
        return (next_state, result)

    ########################################################################
    # SBE37State.AUTOSAMPLE
    ########################################################################

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        """
        mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.AUTOSAMPLE)
        self._publish_state_change(SBE37State.AUTOSAMPLE)
        
    def _handler_autosample_exit(self,  *args, **kwargs):
        """
        """
        pass

    def _handler_autosample_stop_autosample(self, *args, **kwargs):
        """
        @throw InstrumentProtocolException on invalid command
        """
        next_state = None
        result = None

        try:
            prompt = None
            count = 0
            timeout = kwargs.get('timeout', 10)
            while prompt != SBE37Prompt.AUTOSAMPLE:
                prompt = self._wakeup(timeout)
                count += 1
                if count == 3:
                    break
            self._do_cmd_resp('stop', *args, **kwargs)
            prompt = None
            while prompt != SBE37Prompt.COMMAND:
                prompt = self._wakeup(timeout)
            next_state = SBE37State.COMMAND
            
        except InstrumentTimeoutException:
            result = InstErrorCode.TIMEOUT

        return (next_state, result)

    ########################################################################
    # SBE37State.COMMAND and SBE37State.AUTOSAMPLE common handlers.
    ########################################################################

    def _handler_command_autosample_get(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
        
        try:
            parameter = args[0]
            
        except IndexError:
            result = InstErrorCode.REQUIRED_PARAMETER
            
        else:
            try:
                result = self._get_param_dict(parameter)

            except KeyError:
                result = InstErrorCode.INVALID_PARAMETER
            
        return (next_state, result)

    ########################################################################
    # SBE37State.DIRECT
    ########################################################################

    def _handler_direct_enter(self, *args, **kwargs):
        """
        """
        mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.DIRECT)
        self._publish_state_change(SBE37State.DIRECT)
        # get prompt from instrument
        self._do_cmd_direct(SBE37_NEWLINE)                
            
    def _handler_direct_exit(self,  *args, **kwargs):
        """
        """
        pass

    def _handler_direct_execute_direct(self, data):
        """
        """
        next_state = None
        result = None

        try:
            self._do_cmd_direct(data)                
        
        except InstrumentTimeoutException:
            result = InstErrorCode.TIMEOUT

        return (next_state, result)

    def _handler_direct_stop_direct(self):
        """
        @throw InstrumentProtocolException on invalid command
        """
        next_state = None
        result = None

        next_state = SBE37State.COMMAND
            
        return (next_state, result)

    ########################################################################
    # Private helpers
    ########################################################################
    
    def _got_data(self, data):
        # callback from logger
        """
        """
        
        if self._fsm.get_current_state() == SBE37State.DIRECT:
            # direct access mode
            if len(data) > 0:
                mi_logger.debug("SBE37Protocol._got_data(): <" + data + ">") 
                # check for echoed commands from instrument (TODO: this should only be done for telnet?)
                if len(self._sent_cmds) > 0:
                    # there are sent commands that need to have there echoes filtered out
                    oldest_sent_cmd = self._sent_cmds[0]
                    if string.count(data, oldest_sent_cmd) > 0:
                        # found a command echo, so remove it from data and delete the command form list
                        data = string.replace(data, oldest_sent_cmd, "", 1) 
                        self._sent_cmds.pop(0)            
                if len(data) > 0 and self.send_event:
                    event = {'type':'direct_access',
                             'value':data
                    }
                    self.send_event(event)
                    # TODO: what about logging this as an event?
            return
        
        if len(data)>0:
            CommandResponseInstrumentProtocol._got_data(self, data)                        
                        
            if len(self._promptbuf)>7:
                self._promptbuf = self._promptbuf[-7:]
                
            # If we are streaming, process the line buffer for samples.
            if self._fsm.get_current_state() == SBE37State.AUTOSAMPLE:
                self._process_streaming_data()
        
    def _process_streaming_data(self):
        """
        """
        if self.eoln in self._linebuf:
            lines = self._linebuf.split(SBE37_NEWLINE)
            self._linebuf = lines[-1]
            for line in lines:
                sample = self._extract_sample(line, True)
    
    def _send_wakeup(self):
        """
        """
        self._logger_client.send(SBE37_NEWLINE)

    def _update_params(self, *args, **kwargs):
        """
        """
        
        timeout = kwargs.get('timeout', 10)
        old_config = self._get_config_param_dict()
        self._do_cmd_resp('ds',timeout=timeout)
        self._do_cmd_resp('dc',timeout=timeout)
        new_config = self._get_config_param_dict()            
        if new_config != old_config:
            if self.send_event:
                event = {
                    'type' : 'config_change',
                    'value' : new_config
                }
                self.send_event(event)
        mi_logger.info('done updating params')
        
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
        for line in response.split(SBE37_NEWLINE):
            self._update_param_dict(line)
        
    def _parse_ts_response(self, response, prompt):
        """
        """
        sample = None
        for line in response.split(SBE37_NEWLINE):
            sample = self._extract_sample(line, True)
            if sample: break
            
        return sample

    def _extract_sample(self, line, publish=True):
        """
        """
        sample = None
        match = self._sample_regex.match(line)
        if match:
            sample = {}
            sample['t'] = [float(match.group(1))]
            sample['c'] = [float(match.group(2))]
            sample['p'] = [float(match.group(3))]

            # Extract sound velocity and salinity if present.
            #if match.group(5) and match.group(7):
            #    sample['salinity'] = float(match.group(5))
            #    sample['sound_velocity'] = float(match.group(7))
            #elif match.group(5):
            #    if self._get_param_dict(SBE37Parameter.OUTPUTSAL):
            #        sample['salinity'] = float(match.group(5))
            #    elif self._get_param_dict(SBE37Parameter.OUTPUTSV):
            #        sample['sound_velocity'] = match.group(5)
        
            # Extract date and time if present.
            # sample_time = None
            #if  match.group(8):
            #    sample_time = time.strptime(match.group(8),', %d %b %Y, %H:%M:%S')
            #
            #elif match.group(15):
            #    sample_time = time.strptime(match.group(15),', %m-%d-%Y, %H:%M:%S')
            #
            #if sample_time:
            #    sample['time'] = \
            #        '%4i-%02i-%02iT:%02i:%02i:%02i' % sample_time[:6]

            # Add UTC time from driver in iso 8601 format.
            #sample['driver_time'] = datetime.datetime.utcnow().isoformat()

            # Driver timestamp.
            sample['time'] = [time.time()]


            if publish and self.send_event:
                event = {
                    'type':'sample',
                    'name':'ctd_parsed',
                    'value':sample
                }
                self.send_event(event)
                

        return sample            

    def _parse_set_response(self, response, prompt):
        """
        """
        if prompt == SBE37Prompt.COMMAND:
            return InstErrorCode.OK
        else:
            return InstErrorCode.BAD_DRIVER_COMMAND

    def _publish_state_change(self, state):
        """
        """
        if self.send_event:
            event = {
                'type': 'state_change',
                'value': state
            }
            self.send_event(event)


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
    
    def initialize(self, channels = [SBE37Channel.CTD], *args, **kwargs):
        """
        """
        try:        
            (result, valid_channels) = self._check_channel_args(channels)

            for channel in valid_channels:
                result[channel] = self._channels[channel].initialize(*args, **kwargs)
                    
        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
                
        return result
    
    def configure(self, configs, *args, **kwargs):
        """
        """        
        try:
            channels = configs.keys()
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                config = configs[channel]
                result[channel] = self._channels[channel].configure(config, *args, **kwargs)
                    
        except (RequiredParameterException, TypeError):
            result = InstErrorCode.REQUIRED_PARAMETER

        return result
    
    def connect(self, channels = [SBE37Channel.CTD], *args, **kwargs):
        """
        """        
        try:
            (result, valid_channels) = self._check_channel_args(channels)

            for channel in valid_channels:
                result[channel] = self._channels[channel].connect(*args, **kwargs)
                
        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
                
        return result
    
    def disconnect(self, channels = [SBE37Channel.CTD], *args, **kwargs):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)

            for channel in valid_channels:
                result[channel] = self._channels[channel].disconnect(*args, **kwargs)
                    
        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
                
        return result
            
    def detach(self, channels=[SBE37Channel.CTD], *args, **kwargs):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                result[channel] = self._channels[channel].detach(*args, **kwargs)
            
        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
                
        return result

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, *args, **kwargs):
        """
        """
        try:
            (result, params) = self._check_get_args(params)
            
            for (channel, parameter) in params:        
                success = InstErrorCode.OK
                result[(channel, parameter)] = self._channels[channel].get(parameter, *args, **kwargs)
                
        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
            
        # Return overall success and individual results.
        return result
            
    def set(self, params, *args, **kwargs):
        """
        """
        try:
            (result, params) = self._check_set_args(params)
            
            updated_channels = []
            
            # Process each parameter-value pair.
            for (key, val) in params.iteritems():
                channel = key[0]
                parameter = key[1]
                result[key] = self._channels[channel].set(parameter, val, *args, **kwargs)
                if channel not in updated_channels:
                    updated_channels.append(channel)                
                    
            for channel in updated_channels:
                self._channels[channel].update_params(*args, **kwargs)
        
        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
        # Additional checking can go here.
        
        # Return overall success and individual results.
        return result
        
    def start_direct_access(self, channels=[SBE37Channel.CTD]):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                result[channel] = self._channels[SBE37Channel.CTD].\
                    execute_start_direct_access()                

        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
    
        return result
    
    def stop_direct_access(self, channels=[SBE37Channel.CTD]):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                result[channel] = self._channels[SBE37Channel.CTD].\
                    execute_stop_direct_access()                

        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
    
        return result
    
    def execute_direct_access(self, data):
        """
        """
        result = self._channels[SBE37Channel.CTD].execute_direct_access(data)                
  
        return result
    
    def execute_direct(self, channels=[SBE37Channel.CTD], *args, **kwargs):
        """
        """
        pass
    
    def execute_acquire_sample(self, channels=[SBE37Channel.CTD], *args, **kwargs):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                result[channel] = self._channels[SBE37Channel.CTD].\
                    execute_acquire_sample(*args, **kwargs)                

        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
    
        return result
    
    def start_autosample(self, channels=[SBE37Channel.CTD], *args, **kwargs):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                result[channel] = self._channels[SBE37Channel.CTD].\
                    execute_start_autosample(*args, **kwargs)                

        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER

        return result

    def stop_autosample(self, channels=[SBE37Channel.CTD], *args, **kwargs):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                result[channel] = self._channels[SBE37Channel.CTD].\
                    execute_stop_autosample(*args, **kwargs)                

        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER

        return result

    def execute_test(self, channels=[SBE37Channel.CTD], *args, **kwargs):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)
    
            for channel in valid_channels:
                result[channel] = self._channels[SBE37Channel.CTD].\
                    execute_test(*args, **kwargs)                

        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER
    
        return result
    
    ########################################################################
    # TBD.
    ########################################################################    
        

    def get_resource_commands(self):
        """
        """
        result = []
        cmds = self._channels[SBE37Channel.CTD].get_resource_commands()
        if cmds:
            result = [(SBE37Channel.CTD, cmd) for cmd in cmds]
        return result
    
    def get_resource_params(self):
        """
        """
        result = []
        params = self._channels[SBE37Channel.CTD].get_resource_params()
        if params:
            result = [(SBE37Channel.CTD, param) for param in params]
        return result        

    def get_channels(self):
        """
        """
        return SBE37Channels.list()
    
    def get_active_channels(self):
        """
        """
        state = self.get_current_state()[SBE37Channel.CTD]
        if state in [SBE37State.COMMAND, SBE37State.AUTOSAMPLE]:
            result = [SBE37Channel.CTD]
        else:
            result = []
            
        return result
    
    def get_current_state(self, channels=[SBE37Channel.CTD]):
        """
        """
        try:
            (result, valid_channels) = self._check_channel_args(channels)

            for channel in valid_channels:
                result[channel] = self._channels[channel].get_current_state()
        
        except RequiredParameterException:
            result = InstErrorCode.REQUIRED_PARAMETER

        return result

    ########################################################################
    # Private helpers.
    ########################################################################    

    @staticmethod
    def _check_channel_args(channels):
        """
        """
        valid_channels = []
        result = {}
        
        if channels == None or not isinstance(channels, (list, tuple)):
            raise RequiredParameterException()
            
        elif len(channels) == 0:
            raise RequiredParameterException()
            
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
                                        
        return (result, valid_channels)

    @staticmethod
    def _check_get_args(params):
        """
        """
        valid_params = []
        result = {}
        
        if params == None or not isinstance(params, (list, tuple)):
            raise RequiredParameterException()
            
        elif len(params) == 0:
            raise RequiredParameterException()
            
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
            for (c, p) in invalid_channels:
                result[(c, p)] = InstErrorCode.INVALID_CHANNEL

        return (result, valid_params)
   
    @staticmethod
    def _check_set_args(params):
        """
        """
        valid_params = {}
        result = {}
        
        if params == None or not isinstance(params, dict):
            raise RequiredParameterException()
            
        elif len(params) == 0:
            raise RequiredParameterException()
            
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
                    params.pop(key)
                
                elif key[1] not in plist:
                    result[key] = InstErrorCode.INVALID_PARAMETER
                    params.pop(key)
                
        return (result, params)

    ########################################################################
    # Misc and temp.
    ########################################################################

    def driver_echo(self, msg):
        """
        """
        echo = 'driver_echo: %s' % msg
        return echo

