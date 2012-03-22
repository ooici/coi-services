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

from ion.services.mi.common import BaseEnum
from ion.services.mi.single_connection_instrument_driver import SingleConnectionInstrumentDriver
from ion.services.mi.instrument_protocol import CommandResponseInstrumentProtocol
from ion.services.mi.instrument_fsm import InstrumentFSM
from ion.services.mi.instrument_driver import DriverEvent
from ion.services.mi.instrument_driver import DriverAsyncEvent
from ion.services.mi.instrument_driver import DriverProtocolState
from ion.services.mi.instrument_driver import DriverParameter

#import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')

# This is the mi_merge branch.

class SBE37ProtocolState(BaseEnum):
    """
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    AUTOSAMPLE = DriverProtocolState.AUTOSAMPLE
    TEST = DriverProtocolState.TEST
    CALIBRATE = DriverProtocolState.CALIBRATE
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS
    
class SBE37ProtocolEvent(BaseEnum):
    """
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    ACQUIRE_SAMPLE = DriverEvent.ACQUIRE_SAMPLE
    START_AUTOSAMPLE = DriverEvent.START_AUTOSAMPLE
    STOP_AUTOSAMPLE = DriverEvent.STOP_AUTOSAMPLE
    TEST = DriverEvent.TEST
    CALIBRATE = DriverEvent.CALIBRATE
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT

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
    
# Device prompts.
class SBE37Prompt(BaseEnum):
    """
    SBE37 io prompts.
    """
    COMMAND = 'S>'
    BAD_COMMAND = '?cmd S>'
    AUTOSAMPLE = 'S>\r\n'

SBE37_NEWLINE = '\r\n'
                
PACKET_CONFIG = {
        'ctd_parsed' : ('prototype.sci_data.ctd_stream', 'ctd_stream_packet'),
        'ctd_raw' : None            
}


###############################################################################
# Seabird Electronics 37-SMP MicroCAT Driver.
###############################################################################

class SBE37Driver(SingleConnectionInstrumentDriver):
    """
    """
    def __init__(self, evt_callback):
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)


    def _build_protocol(self):
        """
        """
        self._protocol = SBE37Protocol(SBE37Prompt, SBE37_NEWLINE, self._driver_event)

###############################################################################
# Seabird Electronics 37-SMP MicroCAT protocol.
###############################################################################

class SBE37Protocol(CommandResponseInstrumentProtocol):
    """
    """
    def __init__(self, prompts, newline, driver_event):
        """
        """
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, driver_event)
        
        # Build protocol state machine.
        self._protocol_fsm = InstrumentFSM(SBE37ProtocolState, SBE37ProtocolEvent,
                            SBE37ProtocolEvent.ENTER, SBE37ProtocolEvent.EXIT)

        self._protocol_fsm.add_handler(SBE37ProtocolState.UNKNOWN, SBE37ProtocolEvent.ENTER, self._handler_unknown_enter)
        self._protocol_fsm.add_handler(SBE37ProtocolState.UNKNOWN, SBE37ProtocolEvent.EXIT, self._handler_unknown_exit)
        self._protocol_fsm.add_handler(SBE37ProtocolState.UNKNOWN, SBE37ProtocolEvent.DISCOVER, self._handler_unknown_discover)
        self._protocol_fsm.add_handler(SBE37ProtocolState.COMMAND, SBE37ProtocolEvent.ENTER, self._handler_command_enter)
        self._protocol_fsm.add_handler(SBE37ProtocolState.COMMAND, SBE37ProtocolEvent.EXIT, self._handler_command_exit)
        self._protocol_fsm.add_handler(SBE37ProtocolState.COMMAND, SBE37ProtocolEvent.EXIT, self._handler_command_acquire_sample)
        self._protocol_fsm.add_handler(SBE37ProtocolState.AUTOSAMPLE, SBE37ProtocolEvent.ENTER, self._handler_autosample_enter)
        self._protocol_fsm.add_handler(SBE37ProtocolState.AUTOSAMPLE, SBE37ProtocolEvent.EXIT, self._handler_autosample_exit)
        self._protocol_fsm.add_handler(SBE37ProtocolState.DIRECT_ACCESS, SBE37ProtocolEvent.ENTER, self._handler_direct_access_enter)
        self._protocol_fsm.add_handler(SBE37ProtocolState.DIRECT_ACCESS, SBE37ProtocolEvent.EXIT, self._handler_direct_access_exit)

        self._build_param_dict()

        self._add_build_handler('ds', self._build_simple_command)
        self._add_build_handler('dc', self._build_simple_command)
        self._add_build_handler('ts', self._build_simple_command)

        self._add_response_handler('ds', self._parse_dsdc_response)
        self._add_response_handler('dc', self._parse_dsdc_response)
        self._add_response_handler('ts', self._parse_ts_response)

        self._protocol_fsm.start(SBE37ProtocolState.UNKNOWN)

    ########################################################################
    # Unknown handlers.
    ########################################################################

    def _handler_unknown_enter(self, *args, **kwargs):
        """
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
    
    def _handler_unknown_exit(self, *args, **kwargs):
        """
        """
        pass

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
                
        
        timeout = kwargs.get('timeout', 10)
        prompt = self._wakeup(timeout)
        
        if prompt == SBE37Prompt.COMMAND:
            next_state = SBE37ProtocolState.COMMAND
        elif prompt == SBE37Prompt.AUTOSAMPLE:
            next_state = SBE37ProtocolState.AUTOSAMPLE
        else:
            # Error discovering state.
            pass
        mi_logger.info('NEXT STATE IS %s', next_state)        
        return (next_state, result)

    ########################################################################
    # Command handlers.
    ########################################################################

    def _handler_command_enter(self, *args, **kwargs):
        """
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
        self._update_params()
    
    def _handler_command_exit(self, *args, **kwargs):
        """
        """
        pass

    def _handler_command_acquire_sample(self, *args, **kwargs):
        """
        """
        next_state = None
        result = None
                
        self._do_cmd_resp('ts', *args, **kwargs)

        return (next_state, result)

    ########################################################################
    # Autosample handlers.
    ########################################################################

    def _handler_autosample_enter(self, *args, **kwargs):
        """
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
    
    def _handler_autosample_exit(self, *args, **kwargs):
        """
        """
        pass

    ########################################################################
    # Direct access handlers.
    ########################################################################

    def _handler_direct_access_enter(self, *args, **kwargs):
        """
        """
        self._driver_event(DriverAsyncEvent.STATE_CHANGE)
    
    def _handler_direct_access_exit(self, *args, **kwargs):
        """
        """
        pass

    ########################################################################
    # Private helpers.
    ########################################################################
        
    def _send_wakeup(self):
        """
        """
        self._connection.send(SBE37_NEWLINE)
        
    def _update_params(self):
        """
        """
        
        timeout = 10
        old_config = self._param_dict.get_config()
        self._do_cmd_resp('ds',timeout=timeout)
        self._do_cmd_resp('dc',timeout=timeout)
        new_config = self._param_dict.get_config()
        if new_config != old_config:
            self._driver_event(DriverAsyncEvent.CONFIG_CHANGE)
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
            self._param_dict.update(line)
        
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

            # Driver timestamp.
            sample['time'] = [time.time()]

            if self._driver_event:
                self._driver_event(DriverAsyncEvent.SAMPLE, sample)

        return sample            
        
    def _build_param_dict(self):
        """
        """
        # Add parameter handlers to parameter dict.        
        self._param_dict.add(SBE37Parameter.OUTPUTSAL,
                             r'(do not )?output salinity with each sample',
                             lambda match : False if match.group(1) else True,
                             self._true_false_to_string)
        self._param_dict.add(SBE37Parameter.OUTPUTSV,
                             r'(do not )?output sound velocity with each sample',
                             lambda match : False if match.group(1) else True,
                             self._true_false_to_string)
        self._param_dict.add(SBE37Parameter.NAVG,
                             r'number of samples to average = (\d+)',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        self._param_dict.add(SBE37Parameter.SAMPLENUM,
                             r'samplenumber = (\d+), free = \d+',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        self._param_dict.add(SBE37Parameter.INTERVAL,
                             r'sample interval = (\d+) seconds',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        self._param_dict.add(SBE37Parameter.STORETIME,
                             r'(do not )?store time with each sample',
                             lambda match : False if match.group(1) else True,
                             self._true_false_to_string)
        self._param_dict.add(SBE37Parameter.TXREALTIME,
                             r'(do not )?transmit real-time data',
                             lambda match : False if match.group(1) else True,
                             self._true_false_to_string)
        self._param_dict.add(SBE37Parameter.SYNCMODE,
                             r'serial sync mode (enabled|disabled)',
                             lambda match : False if (match.group(1)=='disabled') else True,
                             self._true_false_to_string)
        self._param_dict.add(SBE37Parameter.SYNCWAIT,
                             r'wait time after serial sync sampling = (\d+) seconds',
                             lambda match : int(match.group(1)),
                             self._int_to_string)
        self._param_dict.add(SBE37Parameter.TCALDATE,
                             r'temperature: +((\d+)-([a-zA-Z]+)-(\d+))',
                             lambda match : self._string_to_date(match.group(1), '%d-%b-%y'),
                             self._date_to_string)
        self._param_dict.add(SBE37Parameter.TA0,
                             r' +TA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.TA1,
                             r' +TA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.TA2,
                             r' +TA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.TA3,
                             r' +TA3 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.CCALDATE,
                             r'conductivity: +((\d+)-([a-zA-Z]+)-(\d+))',
                             lambda match : self._string_to_date(match.group(1), '%d-%b-%y'),
                             self._date_to_string)
        self._param_dict.add(SBE37Parameter.CG,
                             r' +G = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.CH,
                             r' +H = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.CI,
                             r' +I = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.CJ,
                             r' +J = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.WBOTC,
                             r' +WBOTC = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.CTCOR,
                             r' +CTCOR = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.CPCOR,
                             r' +CPCOR = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PCALDATE,
                             r'pressure .+ ((\d+)-([a-zA-Z]+)-(\d+))',
                             lambda match : self._string_to_date(match.group(1), '%d-%b-%y'),
                             self._date_to_string)
        self._param_dict.add(SBE37Parameter.PA0,
                             r' +PA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PA1,
                             r' +PA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PA2,
                             r' +PA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PTCA0,
                             r' +PTCA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PTCA1,
                             r' +PTCA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PTCA2,
                             r' +PTCA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PTCB0,
                             r' +PTCSB0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PTCB1,
                             r' +PTCSB1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.PTCB2,
                             r' +PTCSB2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.POFFSET,
                             r' +POFFSET = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.RCALDATE,
                             r'rtc: +((\d+)-([a-zA-Z]+)-(\d+))',
                             lambda match : self._string_to_date(match.group(1), '%d-%b-%y'),
                             self._date_to_string)
        self._param_dict.add(SBE37Parameter.RTCA0,
                             r' +RTCA0 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.RTCA1,
                             r' +RTCA1 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
        self._param_dict.add(SBE37Parameter.RTCA2,
                             r' +RTCA2 = (-?\d.\d\d\d\d\d\de[-+]\d\d)',
                             lambda match : float(match.group(1)),
                             self._float_to_string)
    

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

