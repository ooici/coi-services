#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uwash_bars U. Washington TRHPH BARS driver
module
@file ion/services/mi/drivers/uwash_bars.py
@author Carlos Rueda
@brief Instrument driver classes to support interaction with the U. Washington
 TRHPH BARS sensor .

@NOTE preliminary skeleton, largely based on Steve's satlantic_par.py
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol import ScriptInstrumentProtocol

from ion.services.mi.common import InstErrorCode
from ion.services.mi.instrument_driver_eh import DriverEvent

from ion.services.mi.instrument_fsm import InstrumentFSM
from ion.services.mi.logger_process import EthernetDeviceLogger, LoggerClient


import time
#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


class BarsState(BaseEnum):
    COLLECTING_DATA = 'COLLECTING_DATA'
    MAIN_MENU = 'MAIN_MENU'
    CHANGE_PARAMS_MENU = 'CHANGE_PARAMS_MENU'
    WAITING_FOR_SYSTEM_DIAGNOSTICS = 'WAITING_FOR_SYSTEM_DIAGNOSTICS'
    SETTING_SYSTEM_CLOCK = 'SETTING_SYSTEM_CLOCK'
    WAITING_FOR_SYSTEM_INFO = 'WAITING_FOR_SYSTEM_INFO'


class BarsEvent(BaseEnum):
    #TODO: How is EXIT supposed to be used? At this moment, this event
    # causes no changes in the state machine
    EXIT = DriverEvent.EXIT

    # I'm focusing on the interaction with the BARS menu
    ENTER_MAIN_MENU = 19  # Control-S
    RESTART_DATA_COLLECTION = '1'
    ENTER_CHANGE_PARAMS = '2'
    SHOW_SYSTEM_DIAGNOSTICS = '3'
    ENTER_SET_SYSTEM_CLOCK = '4'
    SHOW_SYSTEM_INFO = '5'

    EXIT_PROGRAM = '6'  # defined for completeness -- NOT TO BE USED.


class BarsPrompt(BaseEnum):
    """
    BARS io prompts.
    """
    NEWLINE = '\r\n'  # TODO newline sequence TBD
    ENTER_MENU_OPTION = 'Enter 0, 1, 2, 3, 4, 5 or 6 here  -->'


####################################################################
# Protocol
####################################################################
class BarsInstrumentProtocol(ScriptInstrumentProtocol):
    """The instrument protocol classes to deal with a TRHPH BARS sensor.

    """

    def __init__(self, connection, config):
        """
        Creates and configures an instance of the protocol.
        @param connection
        @param config
        """

        ScriptInstrumentProtocol.__init__(self, connection)

        self._linebuf = ''

        state_handlers = {
            BarsState.COLLECTING_DATA: self._state_handler_collecting_data,
            BarsState.MAIN_MENU: self._state_handler_main_menu,
            BarsState.CHANGE_PARAMS_MENU:
                self._state_handler_change_params_menu,
            BarsState.WAITING_FOR_SYSTEM_DIAGNOSTICS:
                self._state_handler_waiting_for_system_diagnostics,
            BarsState.SETTING_SYSTEM_CLOCK:
                self._state_handler_setting_system_clock,
            BarsState.WAITING_FOR_SYSTEM_INFO:
                self._state_handler_waiting_for_system_info,
        }

        #
        # NOTE assume the instrument is collecting data
        # TODO: what if the instrument is NOT currently collecting data?
        #
        self._fsm = InstrumentFSM(BarsState, BarsEvent,
                                  state_handlers,
                                  BarsEvent.RESTART_DATA_COLLECTION,
                                  BarsEvent.EXIT)

        self._fsm.start(BarsState.COLLECTING_DATA)

        self._configure(config)

    def _logEvent(self, event):
        log.info("_logEvent: curr_state=%s, event=%s" %
                 (self._fsm.get_current_state(), event))

    ########################################################################
    # State handlers
    ########################################################################

    def _state_handler_collecting_data(self, event, params):
        """
        Handler for BarsState.COLLECTING_DATA
        """

        self._logEvent(event)

        success = InstErrorCode.OK
        next_state = None
        result = None

        if event == BarsEvent.ENTER_MAIN_MENU:

            #TODO send 19 (Control-S) to instrument
            #...

            next_state = BarsState.MAIN_MENU

        elif event == BarsEvent.RESTART_DATA_COLLECTION:
            pass

        elif event == BarsEvent.EXIT:
            pass

        else:
            success = InstErrorCode.UNHANDLED_EVENT

        return (success, next_state, result)

    def _state_handler_main_menu(self, event, params):
        """
        Handler for BarsState.MAIN_MENU
        """

        self._logEvent(event)

        success = InstErrorCode.OK
        next_state = None
        result = None

        if event == BarsEvent.RESTART_DATA_COLLECTION:

            #TODO send '1' to instrument
            # ...

            next_state = BarsState.MAIN_MENU

        elif event == BarsEvent.ENTER_CHANGE_PARAMS:

            #TODO send '2' to instrument
            # ...

            next_state = BarsState.CHANGE_PARAMS_MENU

        elif event == BarsEvent.SHOW_SYSTEM_DIAGNOSTICS:

            #TODO send '3' to instrument
            # ...

            next_state = BarsState.WAITING_FOR_SYSTEM_DIAGNOSTICS

        elif event == BarsEvent.ENTER_SET_SYSTEM_CLOCK:

            #TODO send '4' to instrument
            # ...

            next_state = BarsState.SETTING_SYSTEM_CLOCK

        elif event == BarsEvent.ENTER_MAIN_MENU:
            pass

        elif event == BarsEvent.EXIT:
            pass

        else:
            success = InstErrorCode.UNHANDLED_EVENT

        return (success, next_state, result)

    def _state_handler_change_params_menu(self, event, params):
        """
        Handler for BarsState.CHANGE_PARAMS_MENU
        """

        self._logEvent(event)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #
        #TODO We don't yet know how this menu looks like or works
        #

        if False:
            # ...
            pass

        elif event == BarsEvent.EXIT:
            pass

        else:
            success = InstErrorCode.UNHANDLED_EVENT

        return (success, next_state, result)

    def _state_handler_waiting_for_system_diagnostics(self, event, params):
        """
        Handler for BarsState.WAITING_FOR_SYSTEM_DIAGNOSTICS
        """

        self._logEvent(event)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #
        #TODO wait for some time, then read input stream from instrument
        # until we are sure the complete message has been
        # generated and captured. Then, send the message to somebody.

#        while not message generated:
#            continue waiting while gathering any input from instrument
#
        # etc.

        # TODO: what if we don't get the expected response?
        # ...

        next_state = BarsState.MAIN_MENU

        return (success, next_state, result)

    def _state_handler_setting_system_clock(self, event, params):
        """
        Handler for BarsState.SETTING_SYSTEM_CLOCK
        """

        self._logEvent(event)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #
        #TODO We don't yet know how this dialog looks like or works
        #

        if False:
            # ...
            pass

        elif event == BarsEvent.EXIT:
            pass

        else:
            success = InstErrorCode.UNHANDLED_EVENT

        return (success, next_state, result)

    def _state_handler_waiting_for_system_info(self, event, params):
        """
        Handler for BarsState.WAITING_FOR_SYSTEM_INFO
        """

        self._logEvent(event)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #
        #TODO wait for some time, then read input stream from instrument
        # until we are sure the complete message has been
        # generated and captured. Then, send the message to somebody.

#        while not message generated:
#            continue waiting while gathering any input from instrument
#
        # etc.

        # TODO: what if we don't get the expected response?
        # ...

        next_state = BarsState.MAIN_MENU

        return (success, next_state, result)

    def connect(self):
        """
        """
        logger_pid = self._logger.get_pid()
        log.info('Found logger pid: %s.', str(logger_pid))
        if not logger_pid:
            self._logger.launch_process()
        time.sleep(1)
        self._attach()

        success = InstErrorCode.OK
        return success

    def disconnect(self):
        """
        """
        self._detach()
        self._logger.stop()

    ########################################################################
    # Private helpers
    ########################################################################

    def _configure(self, config):
        """
        """
        self._logger = None
        self._logger_client = None

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
                # TODO serial method
                pass

            else:
                success = InstErrorCode.INVALID_PARAMETER

        except KeyError:
            success = InstErrorCode.INVALID_PARAMETER

        return success

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
        log.debug("Got data %s" % str(data))
        self._linebuf += data

        # TODO sync with other parts of the code

    def _do_cmd(self, cmd, timeout=10):
        """
        """
        self._prompt_recvd = None
        self._logger_client.send(cmd + BarsPrompt.NEWLINE)
        prompt = self._get_prompt(timeout)
        result = self._linebuf.replace(prompt, '')
        return (prompt, result)

    def _wakeup(self, timeout=10):
        """
        """
        self._linebuf = ''
        self._prompt_recvd = None
        starttime = time.time()
        while not self._prompt_recvd:
            log.debug('Sending wakeup.')
            self._logger_client.send(BarsPrompt.NEWLINE)
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
        log.debug('Got parameters %s', repr(result))
