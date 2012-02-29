#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uwash_bars U. Washington TRHPH BARS driver
module
@file ion/services/mi/drivers/uwash_bars.py
@author Carlos Rueda
@brief Instrument driver classes to support interaction with the U. Washington
 TRHPH BARS sensor .
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.services.mi.common import BaseEnum
from ion.services.mi.instrument_protocol import \
    CommandResponseInstrumentProtocol
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter

import ion.services.mi.drivers.uw_bars.bars as bars

from ion.services.mi.common import InstErrorCode
from ion.services.mi.instrument_fsm_args import InstrumentFSM

from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentTimeoutException

import time
import sys
import os
import re


import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')

CONTROL_S = '\x13'
CONTROL_M = '\x0d'

NEWLINE = '\r\n'

GENERIC_PROMPT_PATTERN = re.compile(r'--> $')

DATA_LINE_PATTERN = re.compile(r'(\d+\.\d*\s*){12}.*')

CYCLE_TIME_PATTERN = re.compile(
        r'present value for the Cycle Time is\s+([^.]*)\.')




class BarsProtocolState(BaseEnum):
    PRE_INIT = "PRE_INIT"
    COLLECTING_DATA = 'COLLECTING_DATA'
    MAIN_MENU = 'MAIN_MENU'
    CHANGE_PARAMS_MENU = 'CHANGE_PARAMS_MENU'
    WAITING_FOR_SYSTEM_INFO = 'WAITING_FOR_SYSTEM_INFO'


class BarsProtocolEvent(BaseEnum):
    INITIALIZE = "INITIALIZE"
    ENTER_MAIN_MENU = 19  # Control-S
    RESTART_DATA_COLLECTION = '1'
    ENTER_CHANGE_PARAMS = '2'
    SHOW_SYSTEM_DIAGNOSTICS = '3'
    ENTER_SET_SYSTEM_CLOCK = '4'
    SHOW_SYSTEM_INFO = '5'

    EXIT_PROGRAM = '6'  # defined for completeness -- NOT TO BE USED.

    EXIT_CHANGE_PARAMS = '3'


class BarsPrompt(BaseEnum):
    """
    BARS io prompts.
    """
    MAIN_MENU = 'Enter 0, 1, 2, 3, 4, 5, 6 or 7 here  --> '
    PARAM_MENU = 'Enter 0, 1, 2, or 3 here  --> '


class BarsInstrumentProtocol(CommandResponseInstrumentProtocol):
    """The instrument protocol classes to deal with a TRHPH BARS sensor.

    """

    def __init__(self, callback=None):
        """
        Creates an instance of this protocol. This basically sets up the
        state machine, which is initialized in the INIT state.
        """

        CommandResponseInstrumentProtocol.__init__(self, callback, BarsPrompt,
                                                   NEWLINE)

        self._outfile = sys.stdout
        self._outfile = file("protoc_output.txt", "w")

        self._fsm = InstrumentFSM(BarsProtocolState, BarsProtocolEvent,
                                  None,
                                  None,
                                  InstErrorCode.UNHANDLED_EVENT)

        # PRE_INIT
        self._fsm.add_handler(BarsProtocolState.PRE_INIT,
                              BarsProtocolEvent.INITIALIZE,
                              self._handler_pre_init_initialize)

        # COLLECTING_DATA
        self._fsm.add_handler(BarsProtocolState.COLLECTING_DATA,
                              BarsProtocolEvent.ENTER_MAIN_MENU,
                              self._handler_collecting_data_enter_main_menu)

        # MAIN_MENU
        self._fsm.add_handler(BarsProtocolState.MAIN_MENU,
                              BarsProtocolEvent.RESTART_DATA_COLLECTION,
                              self._handler_main_menu_restart_data_collection)

        self._fsm.add_handler(BarsProtocolState.MAIN_MENU,
                              BarsProtocolEvent.ENTER_CHANGE_PARAMS,
                              self._handler_main_menu_enter_change_params)

        self._fsm.add_handler(BarsProtocolState.MAIN_MENU,
                              BarsProtocolEvent.SHOW_SYSTEM_DIAGNOSTICS,
                              self._handler_main_menu_show_system_diagnostics)

        self._fsm.add_handler(BarsProtocolState.MAIN_MENU,
                              BarsProtocolEvent.ENTER_SET_SYSTEM_CLOCK,
                              self._handler_main_menu_enter_set_system_clock)

        # CHANGE_PARAMS_MENU
        self._fsm.add_handler(BarsProtocolState.CHANGE_PARAMS_MENU,
                              BarsProtocolEvent.EXIT_CHANGE_PARAMS,
                              self._handler_change_params_menu_exit)

        # we start in the PRE_INIT state
        self._fsm.start(BarsProtocolState.PRE_INIT)

        # add build command handlers
        self._add_build_handler(CONTROL_S, self._build_simple_cmd)
        self._add_build_handler(CONTROL_M, self._build_simple_cmd)
        for c in range(8):
            char ='%d' % c
            self._add_build_handler(char, self._build_simple_cmd)

        # add response handlers
        self._add_response_handler(CONTROL_S, self._control_s_response_handler)

    def _assert_state(self, state):
        cs = self.get_current_state()
        res = cs == state
        if not res:
            raise AssertionError("current state=%s, expected=%s" % (cs, state))

    def _build_simple_cmd(self, cmd):
        """
        """
        return cmd

    def _control_s_response_handler(self, response, prompt):
        """
        """
        log.debug("response='%s'  prompt='%s'" % (str(response), str(prompt)))

        # TODO
        return response

    def _got_data(self, data):
        """
        """
        #super(BarsInstrumentProtocol, self)._got_data(data)

        if self._outfile:
            os.write(self._outfile.fileno(), data)
            self._outfile.flush()

        if re.match(DATA_LINE_PATTERN, data):
            # clear prompt buffer
            self._promptbuf = ''

            self._process_streaming_data(data)
        else:
            self._promptbuf += data
            #self._show_buffer('_promptbuf', data)

    def _show_buffer(self, title, buffer, prefix='\n\t| '):
        """
        """
        msg = prefix + buffer.replace('\n', prefix)
        print("%s:%s" % (title, msg))

    def _process_streaming_data(self, data):
        """
        """
        # TODO just printing data out for now
        self._show_buffer('_process_streaming_data', data, '\n\t! ')

    def get_current_state(self):
        """Gets the current state of the protocol."""
        return self._fsm.get_current_state()

    def get(self, params, *args, **kwargs):

        # TODO only handles (INSTRUMENT, TIME_BETWEEN_BURSTS) for the moment

        if log.isEnabledFor(logging.DEBUG):
            log.debug("params=%s args=%s kwargs=%s" %
                      (str(params), str(args), str(kwargs)))

        assert isinstance(params, list)
        assert len(params) == 1

        param = params[0]

        assert param == BarsParameter.TIME_BETWEEN_BURSTS

        #
        # enter main menu
        #
        self._fsm.on_event(BarsProtocolEvent.ENTER_MAIN_MENU)
        self._assert_state(BarsProtocolState.MAIN_MENU)

        #
        # enter change param menu
        #
        self._fsm.on_event(BarsProtocolEvent.ENTER_CHANGE_PARAMS, params)
        self._assert_state(BarsProtocolState.CHANGE_PARAMS_MENU)

        #
        # save the menu to retrieve info below
        #
        menu = self._promptbuf

        #
        # exit change param menu
        #
        self._fsm.on_event(BarsProtocolEvent.EXIT_CHANGE_PARAMS)
        self._assert_state(BarsProtocolState.MAIN_MENU)

        #
        # return to COLLECTING_DATA
        #
        self._fsm.on_event(BarsProtocolEvent.RESTART_DATA_COLLECTION)
        self._assert_state(BarsProtocolState.COLLECTING_DATA)

        #
        # scan menu for requested value
        #
        seconds = None
        mo = re.search(CYCLE_TIME_PATTERN, menu)
        if mo is not None:
            cycle_time_str = mo.group(1)
            log.debug("scanned cycle_time_str='%s'" % str(cycle_time_str))
            seconds = bars.get_cycle_time_seconds(cycle_time_str)

        if seconds is None:
            raise InstrumentProtocolException(
                    msg="Unexpected: string could not be matched: %s" % menu)

        result = {param: seconds}
        return result

    ########################################################################

    def connect(self, channels=None, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        channels = channels or [BarsChannel.INSTRUMENT]

        self._assert_state(BarsProtocolState.PRE_INIT)
        super(BarsInstrumentProtocol, self).connect(channels, *args, **kwargs)

        if log.isEnabledFor(logging.DEBUG):
            log.debug("connected.")

        self._fsm.on_event(BarsProtocolEvent.INITIALIZE)

    def disconnect(self, channels=None, *args, **kwargs):
        """
        """
        if log.isEnabledFor(logging.DEBUG):
            log.debug("channels=%s args=%s kwargs=%s" %
                      (str(channels), str(args), str(kwargs)))

        channels = channels or [BarsChannel.INSTRUMENT]

        #self._assert_state(BarsProtocolState.PRE_INIT)
        super(BarsInstrumentProtocol, self).disconnect(channels, *args,
                                                      **kwargs)

        if log.isEnabledFor(logging.DEBUG):
            log.debug("disconnected.")

        #self._fsm.on_event(BarsProtocolEvent.INITIALIZE)

    ########################################################################
    # State handlers
    ########################################################################

    def _handler_pre_init_initialize(self, *args, **kwargs):
        """
        Handler to transition from PRE_INIT to appropriate state in the
        actual instrument, typically COLLECTING_DATA.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        next_state = None
        result = None

        #TODO read from the instrument to determine current state
        #...

        # assume collecting data for the moment
        next_state = BarsProtocolState.COLLECTING_DATA

        return (next_state, result)

    def _handler_collecting_data_enter_main_menu(self, *args, **kwargs):
        """
        handler to enter main menu from collecting data
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        time_limit = time.time() + 60
        log.debug("### automatic ^S")
        got_prompt = False
        while not got_prompt and time.time() <= time_limit:
            log.debug("### sending ^S")
            result = None
            try:
                result = self._do_cmd_resp(CONTROL_S, timeout=2)
            except InstrumentTimeoutException:
                pass  # ignore

            # TODO remove the following except case when
            # InstrumentTimeoutException is used by the core class
            # consistently.
            except InstrumentProtocolException as e:
                if e.error_code != InstErrorCode.TIMEOUT:
                    raise

            time.sleep(1)
            log.debug("### ******** result = '%s'" % str(result))
            log.debug("### ******** linebuff = '%s'" % self._linebuf)
            log.debug("### ******** _promptbuf = '%s'" % self._promptbuf)
            string = self._promptbuf
            got_prompt = GENERIC_PROMPT_PATTERN.search(string) is not None

        if not got_prompt:
            raise InstrumentTimeoutException()

        log.debug("### got prompt. Sending one ^m to clean up any ^S leftover")
        result = self._do_cmd_resp(CONTROL_M, timeout=10)

        time.sleep(1)
        log.debug("### ******** result = '%s'" % str(result))
        log.debug("### ******** linebuff = '%s'" % repr(self._linebuf))
        log.debug("### ******** _promptbuf = '%s'" % repr(self._promptbuf))
        string = self._promptbuf
        got_prompt = GENERIC_PROMPT_PATTERN.search(string) is not None

        if not got_prompt:
            raise InstrumentProtocolException(
                    InstErrorCode.UNKNOWN_ERROR,
                    msg="Unexpected, should have gotten prompt after enter.")

        next_state = BarsProtocolState.MAIN_MENU

        return (next_state, result)

    def  _wakeup(self, timeout=10):
        """overwritten: no need to send anything"""
        return None

    def _handler_main_menu_restart_data_collection(self, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        next_state = None
        result = None

        # send '1' not expecting response:
        result = self._do_cmd_no_resp('1', timeout=60)

        print "restart_data_coll result='%s'" % str(result)

        # TODO confirm that data is streaming again?
        # ...

        next_state = BarsProtocolState.COLLECTING_DATA

        return (next_state, result)

    def _handler_main_menu_enter_change_params(self, *args, **kwargs):
        """
        handler to enter in the change params menu. This menu shows the
        current value of some parameters so it's used to scan for
        respective values if that's the case.
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        next_state = None
        result = None

        result = self._do_cmd_resp('2', timeout=10)

        print "XXX2 result='%s'" % str(result)

        next_state = BarsProtocolState.CHANGE_PARAMS_MENU

        return (next_state, result)

    def _handler_main_menu_show_system_diagnostics(self, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        next_state = None
        result = None

        #TODO send '3' to instrument
        # ...

        # TODO wait for "How Many Scans do you want? --> "
        # ...

        # TODO what number of scans? -> presumably included in the params
        # ...
        num_scans = 3

        # TODO send num_scans
        # ...

        # TODO read response until "Press Enter to return to Main Menu." and
        # notify somebody about it

        # then, back to main menu:
        next_state = BarsProtocolState.MAIN_MENU

        return (next_state, result)

    def _handler_main_menu_enter_set_system_clock(self, *args, **kwargs):
        """
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        next_state = None
        result = None


        # TODO validate params, it should contain a datatime spec
        # ...

        #TODO send '4' to instrument
        # ...

        # TODO wait "Do you want to Change the Current Time? (0 = No, 1 = Yes) --> "
        # TODO enter 1

        # TODO complete interaction based on the given datetime:
#    Enter the Month (1-12)     : 1
#    Enter the Day (1-31)       : 26
#    Enter the Year (Two Digits): 12
#    Enter the Hour (0-23)      : 10
#    Enter the Minute (0-59)    : 51
#    Enter the Second (0-59)    : 14

        # TODO wat "Do you want to Change the Current Time? (0 = No, 1 = Yes) --> "
        # TODO enter 0

        # then, back to main menu:
        next_state = BarsProtocolState.MAIN_MENU

        return (next_state, result)

    def _handler_change_params_menu_exit(self, *args, **kwargs):
        """
        handler to exit change params menu
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

        #
        # Send '3'
        #
        result = self._do_cmd_resp(BarsProtocolEvent.ENTER_CHANGE_PARAMS)

        next_state = BarsProtocolState.MAIN_MENU

        return (next_state, result)

    def _state_handler_waiting_for_system_info(self, *args, **kwargs):
        """
        Handler for BarsState.WAITING_FOR_SYSTEM_INFO
        """

        if log.isEnabledFor(logging.DEBUG):
            log.debug("args=%s kwargs=%s" % (str(args), str(kwargs)))

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

        next_state = BarsProtocolState.MAIN_MENU

        return (next_state, result)
