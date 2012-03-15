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
from ion.services.mi.instrument_driver import DriverProtocolState
from ion.services.mi.instrument_driver import DriverParameter

#import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')


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

print 'HELLO HELLO HELLO'

###############################################################################
# Seabird Electronics 37-SMP MicroCAT Driver.
###############################################################################

class SBE37Driver(SingleConnectionInstrumentDriver):
    """
    """
    def __init__(self, evt_callback):
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)

        # Construct the protocol.    
        self._protocol = SBE37Protocol(SBE37Prompt, SBE37_NEWLINE, self.publish_event)

        mi_logger.info('NEW DRIVER CONSTRUCTED!!')

###############################################################################
# Seabird Electronics 37-SMP MicroCAT protocol.
###############################################################################

class SBE37Protocol(CommandResponseInstrumentProtocol):
    """
    """
    def __init__(self, prompts, newline, publsih_event):
        """
        """
        CommandResponseInstrumentProtocol.__init__(self, prompts, newline, publsih_event)
        
        # Build protocol state machine.
        self._protocol_fsm = InstrumentFSM(SBE37ProtocolState, SBE37ProtocolEvent,
                            SBE37ProtocolEvent.ENTER, SBE37ProtocolEvent.EXIT)

        self._build_param_dict()



    ########################################################################
    # Private helpers.
    ########################################################################
        
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

