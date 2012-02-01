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

class SBE37Channel(BaseEnum):
    """
    """
    CTD = DriverChannel.CTD
    ALL = DriverChannel.ALL
    INSTRUMENT = DriverChannel.INSTRUMENT

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
    def __init__(self, prompts, newline):
        """
        """
        CommandResponseInstrumentProtocol.__init__(self, None, prompts, newline)
        
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
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.ENTER, self._handler_autosample_enter)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.EXIT, self._handler_autosample_exit)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.EXECUTE, self._handler_autosample_execute)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.GET, self._handler_command_autosample_get)

        # Start state machine.
        self._fsm.start(SBE37State.UNCONFIGURED)

        # Add build command handlers.
        self._add_build_handler('ds', self._build_simple_cmd)
        self._add_build_handler('dc', self._build_simple_cmd)
        self._add_build_handler('ts', self._build_simple_cmd)
        self._add_build_handler('startnow', self._build_simple_cmd)
        self._add_build_handler('stop', self._build_simple_cmd)
        self._add_build_handler('set', self._build_set_cmd)

        # Add parse response handlers.
        self._add_response_handler('ds', self._parse_dsdc_response)
        self._add_response_handler('dc', self._parse_dsdc_response)
        self._add_response_handler('ts', self._parse_ts_response)

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
        timeout = None
        if params:
            timeout = params.get('timeout', None)
        InstrumentProtocol.initialize(self, timeout)
    
    def _handler_unconfigured_exit(self, params):
        """
        """
        pass

    def _handler_unconfigured_initialize(self, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None
        
        next_state = SBE37State.UNCONFIGURED

        return (success, next_state, result)

    def _handler_unconfigured_configure(self, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        # Attempt to configure driver, switch to disconnected
        # if successful.
        config = None
        timeout = None
        if params:
            config = params.get('config', None)
            timeout = params.get('timeout', None)
        success = InstrumentProtocol.configure(self, config, timeout)
        if InstErrorCode.is_ok(success):
            next_state = SBE37State.DISCONNECTED

        return (success, next_state, result)
    
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
        success = InstErrorCode.OK
        next_state = None
        result = None

        next_state = SBE37State.UNCONFIGURED

        return (success, next_state, result)

    def _handler_disconnected_configure(self, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        # Attempt to configure driver, switch to disconnected
        # if successful.
        config = None
        timeout = None
        if params:
            config = params.get('config', None)
            timeout = params.get('timeout', None)            
        success = InstrumentProtocol.configure(self, config, timeout)
        if InstErrorCode.is_error(success):
            next_state = SBE37State.DISCONNECTED

        return (success, next_state, result)

    def _handler_disconnected_connect(self, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        timeout = None
        if params:
            timeout = params.get('timeout', None)
        success = InstrumentProtocol.connect(self, timeout)
        
        if InstErrorCode.is_ok(success):                
            prompt = self._wakeup()
            
            if prompt == SBE37Prompt.COMMAND:
                next_state = SBE37State.COMMAND

            elif prompt == SBE37Prompt.AUTOSAMPLE:
                next_state = SBE37State.AUTOSAMPLE

            elif promp == InstErrorCode.TIMEOUT:
                # Disconnect on timeout from prompt.
                # TBD.
                InstrumentProtocol.disconnect(self)
                next_state = SBE37State.DISCONNECTED

        return (success, next_state, result)

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
        success = InstErrorCode.OK
        next_state = None
        result = None

        timeout = None
        if params:
            timeout = params.get('timeout', None)
        InstrumentProtocol.disconnect(self, timeout)
        next_state = SBE37State.DISCONNECTED

        return (success, next_state, result)

    def _handler_command_execute(self, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None
        command = None
        cmd = None
        if params:
            command = params.get('command', None)
        if command:
            cmd = command[0]
        if cmd == SBE37Command.ACQUIRE_SAMPLE:
            self._acquire_sample()
            
        elif cmd == SBE37Command.START_AUTO_SAMPLING:
            self._do_cmd_no_resp('startnow')
            next_state = SBE37State.AUTOSAMPLE
            
        else:
            success = InstErrorCode.INVALID_COMMAND        

        return (success, next_state, result)

    def _handler_command_set(self, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        parameter = None
        if params:
            timeout = params.get('timeout', None)
            parameter = params.get('parameter', None)
            value = params.get('value', None)
            
        return (success, next_state, result)

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
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        command = None
        cmd = None
        if params:
            command = params.get('command', None)
        if command:
            cmd = command[0]
        if cmd == SBE37Command.STOP_AUTO_SAMPLING:
            prompt = None
            while prompt != SBE37Prompt.AUTOSAMPLE:
                prompt = self._wakeup()
            self._do_cmd_resp('stop')
            prompt = None
            while prompt != SBE37Prompt.COMMAND:
                prompt = self._wakeup()
                if prompt == SBE37Prompt.COMMAND: break 
            next_state = SBE37State.COMMAND
            
        else:
            success = InstErrorCode.INVALID_COMMAND        

        return (success, next_state, result)

    ########################################################################
    # SBE37State.COMMAND and SBE37State.AUTOSAMPLE common handlers.
    ########################################################################

    def _handler_command_autosample_get(self, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        parameter = None
        if params:
            timeout = params.get('timeout', None)
            parameter = params.get('parameter', None)

        if parameter:
            try:
                result = self._get_param_dict(parameter)

            except KeyError:
                success = InstErrorCode.INVALID_PARAMETER

        else:
            success = InstErrorCode.INVALID_PARAMETER
            
        return (success, next_state, result)

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
        if SBE37_NEWLINE in self._linebuf:
            lines = self._linebuf.split(SBE37_NEWLINE)
            self._linebuf = lines[-1]
            lines = lines[0:-1]
            mi_logger.debug('Streaming data received: %s',str(lines))
    
    def _send_wakeup(self):
        """
        """
        self._logger_client.send(SBE37_NEWLINE)
    
    def _update_params(self):
        """
        """
        # Send display commands and capture result.
        self._do_cmd_resp('ds')
        self._do_cmd_resp('dc')

    def _acquire_sample(self):
        """
        """
        # Send take sample command.
        self._do_cmd_resp('ts')

    def _build_simple_cmd(self, cmd):
        """
        """
        return cmd+SBE37_NEWLINE
    
    def _build_set_cmd(self, param, val):
        """
        """
        #return "%s=%s" % (param, self._parameters.format_set(val)) + self.eoln
        pass

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
    def __init__(self):
        """
        method docstring
        """
        InstrumentDriver.__init__(self)
        
        # Build the protocol for CTD channel.
        protocol = SBE37Protocol(SBE37Prompt, SBE37_NEWLINE)
        self._channels = {SBE37Channel.CTD:protocol}
            
    ########################################################################
    # Channel connection interface.
    ########################################################################
    
    def initialize(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """        
        # SBE37 exposes a single CTD channel.
        if len(channels) != 1 or channels[0] != SBE37Channel.CTD:
            return (InstErrorCode.INVALID_CHANNEL, None)
        
        return self._channels[SBE37Channel.CTD].initialize(timeout)
    
    def configure(self, configs, timeout=10):
        """
        """
        # SBE37 exposes a single CTD channel.
        config = configs.get(SBE37Channel.CTD, None)
        if not config:
            return (InstErrorCode.INVALID_CHANNEL, None)

        # Forward command to the CTD protocol.
        return self._channels[SBE37Channel.CTD].configure(config, timeout)
    
    def connect(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """  
        # SBE37 exposes a single CTD channel.
        if len(channels) != 1 or channels[0] != SBE37Channel.CTD:
            return (InstErrorCode.INVALID_CHANNEL, None)

        # Forward command to the CTD protocol.
        return self._channels[SBE37Channel.CTD].connect(timeout)
    
    def disconnect(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        # SBE37 exposes a single CTD channel.
        if len(channels) != 1 and channels[0] != SBE37Channel.CTD:
            return (InstErrorCode.INVALID_CHANNEL, None)
        
        # Forward command to the CTD protocol.
        return self._channels[SBE37Channel.CTD].disconnect(timeout)
            
    def detach(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        # SBE37 exposes a single CTD channel.
        if len(channels) != 1 and channels[0] != SBE37Channel.CTD:
            return (InstErrorCode.INVALID_CHANNEL, None)
        
        # Forward command to the CTD protocol.
        return self._channels[SBE37Channel.CTD].disconnect(timeout)

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, timeout=10):
        """
        """
        result = {}
        overall_success = InstErrorCode.OK

        for (channel, parameter) in params:        
            success = InstErrorCode.OK
            val = None
            
            # If channel invalid, error.
            if channel != SBE37Channel.CTD:
                overall_success = InstErrorCode.GET_DEVICE_ERR
                result[(channel, parameter)] = InstErrorCode.INVALID_CHANNEL                
                
            # Channel valid, all parameters.
            elif parameter == SBE37Parameter.ALL:
                for specific_parameter in SBE37Parameter.list():
                    if specific_parameter != SBE37Parameter.ALL:
                        (success, val) = self._channels[SBE37Channel.CTD]\
                                            .get(specific_parameter, timeout)
                        if InstErrorCode.is_error(success):
                            overall_success = InstErrorCode.GET_DEVICE_ERR
                            mi_logger.debug('Error retrieving parameter %s', specific_parameter)
                        result[(channel, specific_parameter)] = (success, val)

            # Channel valid, specific parameter.
            # Forward to protocol.
            else:
                (success, val) = self._channels[SBE37Channel.CTD].get(parameter,
                                                                      timeout)
                if InstErrorCode.is_error(success):
                    overall_success = InstErrorCode.GET_DEVICE_ERR
                result[(channel, parameter)] = (success, val)                
                
        # Return overall success and individual results.
        return (overall_success, result)
            
    def set(self, params, timeout=10):
        """
        """
        result = {}
        overall_success = InstErrorCode.OK
        
        # Process each parameter-value pair.
        for (key, val) in params:
            channel = key[0]
            parameter = key[1]
            set_result = None
            
            # If channel invalid, report error.
            if channel != SBE37Channel.CTD:
                set_result = InstErrorCode.INVALID_CHANNEL
                overall_success = InstErrorCode.SET_DEVICE_ERR

            # If channel valid, forward to protocol.
            else:
                set_result = self._channels[SBE37Channel.CTD].set(parameter,
                                                                  val)

            # Populate result.
            result[(channel, parameter)] = set_result

        # Return overall success and individual results.
        return (overall_success, result)

    def execute(self, channels=[SBE37Channel.CTD], command=[], timeout=10):
        """
        """
        # SBE37 exposes a single CTD channel.
        if len(channels) != 1 and channels[0] != SBE37Channel.CTD:
            return (InstErrorCode.INVALID_CHANNEL, None)

        # Check the command is not empty.
        if len(command) == 0:
            return (InstErrorCode.INVALID_COMMAND, None)
        
        # Forward command to the CTD protocol.
        return self._channels[SBE37Channel.CTD].execute(command, timeout)
        
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
        
    ########################################################################
    # Private helpers.
    ########################################################################    

    ########################################################################
    # Misc and temp.
    ########################################################################

    def test_driver_messaging(self):
        """
        """
        result = 'random float %f' % random.random()
        return result

            

