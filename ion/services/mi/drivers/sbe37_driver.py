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

from ion.services.mi.instrument_driver import InstrumentDriver
from ion.services.mi.instrument_driver import DriverChannel
from ion.services.mi.instrument_driver import DriverCommand
from ion.services.mi.instrument_driver import DriverState
from ion.services.mi.instrument_driver import DriverEvent
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

class SBE37Channel(BaseEnum):
    """
    """
    INSTRUMENT = DriverChannel.INSTRUMENT
    ALL = DriverChannel.ALL
    CTD = DriverChannel.CTD

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
        
        self._fsm = InstrumentFSM(SBE37State, SBE37Event, SBE37Event.ENTER,
                            SBE37Event.EXIT, InstErrorCode.UNHANDLED_EVENT)
        
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
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.ENTER, self._handler_autosample_enter)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.EXIT, self._handler_autosample_exit)
        self._fsm.add_handler(SBE37State.AUTOSAMPLE, SBE37Event.EXECUTE, self._handler_autosample_execute)

        self._fsm.start(SBE37State.UNCONFIGURED)

        self._add_build_handler('ds', self._build_simple_cmd)
        self._add_build_handler('dc', self._build_simple_cmd)
        self._add_build_handler('ts', self._build_simple_cmd)
        self._add_build_handler('startnow', self._build_simple_cmd)
        self._add_build_handler('stop', self._build_simple_cmd)
        self._add_build_handler('set', self._build_set_cmd)

        self._add_response_handler('ds', self._parse_dsdc_response)
        self._add_response_handler('dc', self._parse_dsdc_response)
        self._add_response_handler('ts', self._parse_ts_response)

    ########################################################################
    # Protocol connection interface.
    ########################################################################

    def initialize(self, timeout=10):
        """
        """
        fsm_params = {'timeout':timeout}
        return self._fsm.on_event(SBE37Event.INITIALIZE, fsm_params)
    
    def configure(self, config, timeout=10):
        """
        """
        fsm_params = {'config':config, 'timeout':timeout}
        return self._fsm.on_event(SBE37Event.CONFIGURE, fsm_params)
    
    def connect(self, timeout=10):
        """
        """
        fsm_params = {'timeout':timeout}        
        return self._fsm.on_event(SBE37Event.CONNECT, fsm_params)
    
    def disconnect(self, timeout=10):
        """
        """
        fsm_params = {'timeout':timeout}        
        return self._fsm.on_event(SBE37Event.DISCONNECT, fsm_params)
    
    def detach(self, timeout=10):
        """
        """
        fsm_params = {'timeout':timeout}        
        return self._fsm.on_event(SBE37Event.DETACH, fsm_params)

    ########################################################################
    # Protocol command interface.
    ########################################################################

    def get(self, params, timeout=10):
        """
        """
        fsm_params = {'params':params, 'timeout':timeout}
        return self._fsm.on_event(SBE37Event.EXECUTE, fsm_params)
    
    def set(self, params, timeout=10):
        """
        """
        fsm_params = {'command':command, 'timeout':timeout}
        return self._fsm.on_event(SBE37Event.EXECUTE, fsm_params)

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
    # TBD.
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

    """
    def _do_cmd(self, cmd, timeout=10):
        
        
        result = None
        
        # Wakeup the device.
        prompt = self._wakeup(timeout)
        if prompt != InstErrorCode.TIMEOUT:
            # Clear line and prompt buffers for result.
            self._linebuf = ''
            self._promptbuf = ''

            # Send command.
            mi_logger.debug('_do_cmd: %s', cmd)
            self._logger_client.send(cmd+SBE37_NEWLINE)

            # Wait for the prompt, prepare result and return.
            prompt = self._get_prompt(timeout)
            if prompt != InstErrorCode.TIMEOUT:
                result = self._linebuf.replace(prompt,'')
        return (prompt, result)
    """
    
    """
    def _do_cmd_no_prompt(self, cmd):
        
        
        # Clear the line buffer.
        self._linebuf = ''
        
        # Send command and return.
        mi_logger.debug('_do_cmd_no_prompt: %s', cmd)
        self._logger_client.send(cmd+SBE37_NEWLINE)
    """
    
    def _send_wakeup(self):
        """
        """
        self._logger_client.send(SBE37_NEWLINE)

    """
    def _wakeup(self, timeout=10):
        
        
        # Clear the prompt buffer.
        self._promptbuf = ''
        
        # Grab time for timeout.
        starttime = time.time()

        while True:
            # Send a line return and wait a sec.
            mi_logger.debug('Sending wakeup.')
            self._logger_client.send(SBE37_NEWLINE)
            time.sleep(1)

            # If prompt buffer contains a prompt return or timeout.
            if self._promptbuf.endswith(SBE37Prompt.COMMAND):
                mi_logger.debug('Got prompt: %s', repr(SBE37Prompt.COMMAND))
                return SBE37Prompt.COMMAND
            elif self._promptbuf.endswith(SBE37Prompt.AUTOSAMPLE):
                mi_logger.debug('Got prompt: %s', repr(SBE37Prompt.AUTOSAMPLE))
                return SBE37Prompt.AUTOSAMPLE
            elif time.time() > starttime + timeout:
                mi_logger.info('_wakeup timed out.')                
                return InstErrorCode.TIMEOUT                
    """
    
    """
    def _get_prompt(self, timeout=10):
        
        
        
        # Grab time for timeout and wait for prompt.
        starttime = time.time()
        while True:
            
            # If prompt buffer contains prompt return or timeout.
            if self._promptbuf.endswith(SBE37Prompt.COMMAND):
                mi_logger.debug('Got prompt: %s', repr(SBE37Prompt.COMMAND))                
                return SBE37Prompt.COMMAND
            elif self._promptbuf.endswith(SBE37Prompt.AUTOSAMPLE):
                mi_logger.debug('Got prompt: %s', repr(SBE37Prompt.AUTOSAMPLE))
                return SBE37Prompt.AUTOSAMPLE
            elif self._promptbuf.endswith(SBE37Prompt.BAD_COMMAND):
                mi_logger.debug('Got prompt: %s', repr(SBE37Prompt.BAD_COMMAND))
                return SBE37Prompt.AUTOSAMPLE                
            elif time.time() > starttime + timeout:
                mi_logger.info('_get_prompt timed out.')
                return InstErrorCode.TIMEOUT                
    """
    
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

    def _parse_ts_response(self, response, prompt):
        """
        """
        mi_logger.debug('Got ts response: %s', repr(response))


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
        protocol = SBE37Protocol(SBE37Prompt, SBE37_NEWLINE)
        self._channels = {SBE37Channel.CTD:protocol}
            
    ########################################################################
    # Channel connection interface.
    ########################################################################
    
    def initialize(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        if len(channels) != 1 and channels[0] != SBE37Channel.CTD:
            pass
        
        return self._channels[SBE37Channel.CTD].initialize(timeout)
    
    def configure(self, configs, timeout=10):
        """
        """
        config = configs.get(SBE37Channel.CTD, None)
        if not config:
            pass

        return self._channels[SBE37Channel.CTD].configure(config, timeout)
    
    def connect(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """  
        if len(channels) != 1 and channels[0] != SBE37Channel.CTD:
            pass

        return self._channels[SBE37Channel.CTD].connect(timeout)
    
    def disconnect(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        if len(channels) != 1 and channels[0] != SBE37Channel.CTD:
            pass
        
        return self._channels[SBE37Channel.CTD].disconnect(timeout)
            
    def detach(self, channels=[SBE37Channel.CTD], timeout=10):
        """
        """
        if len(channels) != 1 and channels[0] != SBE37Channel.CTD:
            pass
        
        return self._channels[SBE37Channel.CTD].disconnect(timeout)

    ########################################################################
    # Channel command interface.
    ########################################################################

    def get(self, params, timeout=10):
        """
        """
        pass
    
    def set(self, params, timeout=10):
        """
        """
        pass

    def execute(self, channels=[SBE37Channel.CTD], command=[], timeout=10):
        """
        """
        if len(channels) != 1 and channels[0] != SBE37Channel.CTD:
            pass

        if len(command):
            pass
        
        return self._channels[DriverChannel.CTD].execute(command, timeout)
        
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

    ########################################################################
    # Misc and temp.
    ########################################################################

    def test_driver_messaging(self):
        """
        """
        result = 'random float %f' % random.random()
        return result

            

