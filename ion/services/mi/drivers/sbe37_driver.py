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

from ion.services.mi.instrument_driver_eh import InstrumentDriver
from ion.services.mi.instrument_driver_eh import DriverChannel
from ion.services.mi.instrument_driver_eh import DriverCommand
from ion.services.mi.instrument_driver_eh import DriverState
from ion.services.mi.instrument_driver_eh import DriverEvent
from ion.services.mi.common import InstErrorCode
from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol_eh \
                        import CommandResponseInstrumentProtocol
from ion.services.mi.instrument_fsm import InstrumentFSM
from ion.services.mi.fsm import FSM, ExceptionFSM
from ion.services.mi.logger_process import EthernetDeviceLogger, LoggerClient


#import ion.services.mi.mi_logger
mi_logger = logging.getLogger('mi_logger')

class SBE37State(BaseEnum):
    """
    """
    UNCONFIGURED = DriverState.UNCONFIGURED
    DISCONNECTED = DriverState.DISCONNECTED
    DETACHED = DriverState.DETACHED
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
    NEWLINE = '\r\n'
    BAD_COMMAND = '?cmd S>'
    AUTOSAMPLE = 'S>\r\n'

class SBE37Protocol(CommandResponseInstrumentProtocol):
    """
    """
    def __init__(self):
        """
        """
        CommandResponseInstrumentProtocol.__init__(self)
        
        self._state_handlers = {
            SBE37State.UNCONFIGURED : self._state_handler_unconfigured,
            SBE37State.DISCONNECTED : self._state_handler_disconnected,
            SBE37State.COMMAND : self._state_handler_command,
            SBE37State.AUTOSAMPLE : self._state_handler_autosample
        }
        
        self._fsm = InstrumentFSM(SBE37State, SBE37Event, self._state_handlers,
                                  SBE37Event.ENTER, SBE37Event.EXIT)                
        self._fsm.start(SBE37State.UNCONFIGURED)

        self._linebuf = ''
        self._datalines = []
        self._prompt_recvd = None

    ########################################################################
    # Protocol Interface Functions
    ########################################################################

    def initialize(self):
        """
        """
        return self._fsm.on_event(SBE37Event.INITIALIZE)
    
    def configure(self, config):
        """
        """
        return self._fsm.on_event(SBE37Event.CONFIGURE, config)
    
    def connect(self, timeout=10):
        """
        """
        return self._fsm.on_event(SBE37Event.CONNECT)
    
    def disconnect(self, timeout=10):
        """
        """
        return self._fsm.on_event(SBE37Event.DISCONNECT)
    
    def detach(self):
        """
        """
        return self._fsm.on_event(SBE37Event.DETACH)

    def execute(self, command, timeout=10):
        """
        """
        return self._fsm.on_event(SBE37Event.EXECUTE, command)

    ########################################################################
    # State handlers
    ########################################################################

    def _state_handler_unconfigured(self, event, params):
        """
        """        
        success = InstErrorCode.OK
        next_state = None
        result = None
        
        if event == SBE37Event.ENTER:
            
            mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.UNCONFIGURED)
            
            # Initialize driver configuration.
            self._initialize()

        elif event == SBE37Event.EXIT:
            pass
        
        elif event == SBE37Event.INITIALIZE:
            
            # Initialize driver configuration.
            self._initialize()

        elif event == SBE37Event.CONFIGURE:
            
            # Attempt to configure driver, switch to disconnected
            # if successful.
            success = self._configure(params)
            if InstErrorCode.is_ok(success):
                next_state = SBE37State.DISCONNECTED

        else:
            success = InstErrorCode.UNHANDLED_EVENT
            
        return (success, next_state, result)


    def _state_handler_disconnected(self, event, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        if event == SBE37Event.ENTER:
            mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.DISCONNECTED)

        elif event == SBE37Event.EXIT:
            pass
        
        elif event == SBE37Event.CONFIGURE:
            
            # Attempt to configure driver, switch to disconnected
            # if successful.
            success = self._configure(params)
            if InstErrorCode.is_error(success):
                next_state = SBE37State.UNCONFIGURED
        
        elif event == SBE37Event.CONNECT:
            success = self._connect()
            
            if InstErrorCode.is_ok(success):                
                prompt = self._wakeup()
                
                if prompt == SBE37Prompt.COMMAND:
                    next_state = SBE37State.COMMAND

                elif prompt == SBE37Prompt.AUTOSAMPLE:
                    next_state = SBE37State.AUTOSAMPLE

                else:
                    # A timeout can occur here. In this case disconnect.
                    self._disconnect()
                    next_state = SBE37State.DISCONNECTED
                
                
        elif event == SBE37Event.INITIALIZE:
            next_state = SBE37State.UNCONFIGURED
        
        else:
            success = InstErrorCode.UNHANDLED_EVENT

        return (success, next_state, result)


    def _state_handler_command(self, event, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        if event == SBE37Event.ENTER:
            mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.COMMAND)
            self._update_params()

        elif event == SBE37Event.EXIT:
            pass
        
        elif event == SBE37Event.DISCONNECT:
            self._disconnect()
            next_state = SBE37State.DISCONNECTED


        elif event == SBE37Event.EXECUTE:
            cmd = params[0]
            if cmd == SBE37Command.ACQUIRE_SAMPLE:
                self._acquire_sample()
                
            elif cmd == SBE37Command.START_AUTO_SAMPLING:
                pass
            
            else:
                success = InstErrorCode.INVALID_COMMAND        

        else:
            success = InstErrorCode.UNHANDLED_EVENT

        return (success, next_state, result)

    def _state_handler_autosample(self, event, params):
        """
        """
        success = InstErrorCode.OK
        next_state = None
        result = None

        if event == SBE37Event.ENTER:
            mi_logger.info('channel %s entered state %s',SBE37Channel.CTD,
                           SBE37State.AUTOSAMPLE)

        elif event == SBE37Event.EXIT:
            pass
        
        else:
            success = InstErrorCode.UNHANDLED_EVENT

        return (success, next_state, result)


    ########################################################################
    # Private helpers
    ########################################################################

    def _initialize(self):
        """
        """
        self._logger = None
        self._logger_client = None
        
    def _configure(self, config):
        """
        """
        success = InstErrorCode.OK
        
        try:
            method = config['method']
            
            if method == 'ethernet':
                device_addr = config['device_addr']
                device_port = config['device_port']
                server_addr = config['server_addr']
                server_port = config['server_port']
                self._logger = EthernetDeviceLogger(device_addr, device_port,
                                                    server_port)
                self._logger_client = LoggerClient(server_addr, server_port)

            elif method == 'serial':
                pass
            
            else:
                success = InstErrorCode.INVALID_PARAMETER

        except KeyError:
            success = InstErrorCode.INVALID_PARAMETER

        return success

    def _connect(self):
        """
        """
        logger_pid = self._logger.get_pid()
        mi_logger.info('Found logger pid: %s.', str(logger_pid))
        if not logger_pid:
            self._logger.launch_process()
        time.sleep(1)         
        self._attach()

        success = InstErrorCode.OK
        return success

    def _disconnect(self):
        """
        """
        self._detach()
        self._logger.stop()
        
    def _attach(self):
        """
        """
        self._logger_client.init_comms(self._got_data)
    
    def _detach(self):
        """
        """
        self._logger_client.stop_comms()

    def _got_data(self, data):
        """
        """
        self._linebuf += data        
        if self._linebuf.endswith(SBE37Prompt.COMMAND):
            self._prompt_recvd = SBE37Prompt.COMMAND
            
        elif self._linebuf.endswith(SBE37Prompt.BAD_COMMAND):
            self._prompt_recvd = SBE37Prompt.BAD_COMMAND
            
        elif self._linebuf.endswith(SBE37Prompt.AUTOSAMPLE):
            self._prompt_recvd = SBE37Prompt.AUTOSAMPLE

    def _do_cmd(self, cmd, timeout=10):
        """
        """
        self._prompt_recvd = None
        self._logger_client.send(cmd+SBE37Prompt.NEWLINE)
        prompt = self._get_prompt(timeout)
        result = self._linebuf.replace(prompt,'')
        return (prompt, result)

    def _wakeup(self, timeout=10):
        """
        """
        self._linebuf = ''
        self._prompt_recvd = None
        starttime = time.time()
        while not self._prompt_recvd:
            mi_logger.debug('Sending wakeup.')
            self._logger_client.send(SBE37Prompt.NEWLINE)
            time.sleep(1)
            if time.time() > starttime + timeout:
                return InstErrorCode.TIMEOUT                
        return self._prompt_recvd

    def _get_prompt(self, timeout):
        """
        """
        self._linebuf = ''
        starttime = time.time()
        while not self._prompt_recvd:
            time.sleep(1)
            if time.time() > starttime + timeout:
                return InstErrorCode.TIMEOUT                
        return self._prompt_recvd

    def _update_params(self):
        """
        """
        (ds_prompt, ds_result) = self._do_cmd('ds')
        (dc_prompt, dc_result) = self._do_cmd('dc')
        
        result = ds_result + dc_result
        mi_logger.debug('Got parameters %s', repr(result))

    def _acquire_sample(self):
        """
        """
        (prompt, result) = self._do_cmd('ts')
        mi_logger.debug('Got sample %s', result)


class SBE37Driver(InstrumentDriver):
    """
    class docstring
    """
    def __init__(self):
        """
        method docstring
        """
        InstrumentDriver.__init__(self)
        protocol = SBE37Protocol()
        self._channels = {DriverChannel.CTD:protocol}
        
    def initialize(self, channels):
        """
        """
        return self._channels[DriverChannel.CTD].initialize()
    
    def configure(self, configs):
        """
        """
        config = configs[DriverChannel.CTD]
        return self._channels[DriverChannel.CTD].configure(config)
    
    def connect(self, channels, timeout=10):
        """
        """
        return self._channels[DriverChannel.CTD].connect(timeout)
        #return InstErrorCode.OK
    
    def disconnect(self, channels, timeout=10):
        """
        """
        return self._channels[DriverChannel.CTD].disconnect(timeout)
    
    def test_driver_messaging(self):
        """
        """
        result = 'random float %f' % random.random()
        return result
        
    def execute(self, channels, command, timeout=10):
        """
        """
        return self._channels[DriverChannel.CTD].execute(command, timeout)


            

