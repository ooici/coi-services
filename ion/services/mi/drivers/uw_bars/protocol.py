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
from ion.services.mi.instrument_protocol import \
    CommandResponseInstrumentProtocol
from ion.services.mi.drivers.uw_bars.common import BarsChannel
from ion.services.mi.drivers.uw_bars.common import BarsParameter


from ion.services.mi.common import InstErrorCode
from ion.services.mi.instrument_fsm import InstrumentFSM

import re


import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


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


# TODO synchronize with actual instrument and simulator
NEWLINE = '\n'

# some patters
DATA_LINE_PATTERN = re.compile(r'(\d+\.\d*\s+)+.*')
CYCLE_TIME_PATTERN = re.compile(
        r'present value for the Cycle Time is\s+([^.]*)\.')


class BarsPrompt(BaseEnum):
    """
    BARS io prompts.
    """
    MAIN_MENU = 'Enter 0, 1, 2, 3, 4, 5, 6 or 7 here  --> '
    PARAM_MENU = 'Enter 0, 1, 2, or 3 here  --> '


class BarsInstrumentProtocol(CommandResponseInstrumentProtocol):
    """The instrument protocol classes to deal with a TRHPH BARS sensor.

    """

    def __init__(self):
        """
        Creates an instance of this protocol. This basically sets up the
        state machine, which is initialized in the INIT state.
        """

#        InstrumentProtocol.__init__(self)
        callback, prompts, newline = None, BarsPrompt, NEWLINE
        CommandResponseInstrumentProtocol.__init__(self, callback, prompts,
                                                   newline)

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

        self._add_build_handler('^S', self._build_simple_cmd)
        for c in range(8):
            self._add_build_handler('%d' % c, self._build_simple_cmd)

    def _assert_state(self, state):
        cs = self.get_current_state()
        res = cs == state
        if not res:
            raise AssertionError("current state=%s, expected=%s" % (cs, state))

    def _build_simple_cmd(self, cmd):
        """
        """
        return cmd

    def _logEvent(self, params):
        #log.info
        print("_logEvent: curr_state=%s, params=%s" %
                 (self._fsm.get_current_state(), str(params)))

    def _got_data(self, data):
        """
        """
        #super(BarsInstrumentProtocol, self)._got_data(data)

        if re.match(DATA_LINE_PATTERN, data):
            # clear prompt buffer
            self._promptbuf = ''

            self._process_streaming_data(data)
        else:
            self._promptbuf += data
            self._show_buffer('_promptbuf', data)

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

    def connect(self, timeout=10):
        super(BarsInstrumentProtocol, self).connect(timeout)
        self._fsm.on_event(BarsProtocolEvent.INITIALIZE)

    def get(self, params=[(BarsChannel.INSTRUMENT,
                           BarsParameter.TIME_BETWEEN_BURSTS)],
            timeout=10):

        # TODO only handles (INSTRUMENT, TIME_BETWEEN_BURSTS) for the moment

        assert isinstance(params, list)
        assert len(params) == 1

        channel, param = cp = params[0]

        assert channel == BarsChannel.INSTRUMENT
        assert param == BarsParameter.TIME_BETWEEN_BURSTS

        #
        # enter main menu
        #
        success, result = self._fsm.on_event(BarsProtocolEvent.ENTER_MAIN_MENU)
        if InstErrorCode.is_error(success):
            return (success, result)

        self._assert_state(BarsProtocolState.MAIN_MENU)

        #
        # enter change param menu
        #
        success, result = self._fsm.on_event(
                BarsProtocolEvent.ENTER_CHANGE_PARAMS, params)
        if InstErrorCode.is_error(success):
            return (success, result)

        #
        # scan requested value
        #
        success = InstErrorCode.UNKNOWN_ERROR
        value = "??"
        mo = re.search(CYCLE_TIME_PATTERN, self._promptbuf)
        if mo is not None:
            cycle_time_str = mo.group(1)
            log.debug("scanned cycle_time_str='%s'" % str(cycle_time_str))
            value = cycle_time_str
            success = InstErrorCode.OK
        # we got out result
        result = {cp: (success, value)}

        #
        # exit change param menu
        #
        success, result = self._fsm.on_event(
                BarsProtocolEvent.EXIT_CHANGE_PARAMS)
        if InstErrorCode.is_error(success):
            return (success, result)

        self._assert_state(BarsProtocolState.MAIN_MENU)

        #
        # return to COLLECTING_DATA
        #
        success, result = self._fsm.on_event(
                BarsProtocolEvent.RESTART_DATA_COLLECTION)
        if InstErrorCode.is_error(success):
            return (success, result)

        self._assert_state(BarsProtocolState.COLLECTING_DATA)

        return (success, result)

    ########################################################################
    # State handlers
    ########################################################################

    def _handler_pre_init_initialize(self, params):
        """
        Handler to transition from PRE_INIT to appropriate state in the
        actual instrument, typically COLLECTING_DATA.
        """

        self._logEvent(params)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #TODO read from the instrument to determine current state
        #...

        # assume collecting data for the moment
        next_state = BarsProtocolState.COLLECTING_DATA

        return (success, next_state, result)

    def _handler_collecting_data_enter_main_menu(self, params):
        """
        handler to enter main menu from collecting data
        """

        self._logEvent(params)

        success = InstErrorCode.OK

        result = self._do_cmd_resp('^S')

        next_state = BarsProtocolState.MAIN_MENU

        return (success, next_state, result)

    def  _wakeup(self, timeout=10):
        """overwritten: no need to send anything"""
        return None

    def _handler_main_menu_restart_data_collection(self, params):
        """
        """

        self._logEvent(params)

        success = InstErrorCode.OK
        next_state = None
        result = None

        # send '1' not expecting response:
        result = self._do_cmd_no_resp('1')

        print "restart_data_coll result='%s'" % str(result)

        # TODO confirm that data is streaming again?
        # ...

        next_state = BarsProtocolState.COLLECTING_DATA

        return (success, next_state, result)

    def _handler_main_menu_enter_change_params(self, params):
        """
        handler to enter in the change params menu. This menu shows the
        current value of some parameters so it's used to scan for
        respective values if that's the case.
        """

        self._logEvent(params)

        success = InstErrorCode.OK
        next_state = None
        result = None

        result = self._do_cmd_resp('2')

        print "XXX2 result='%s'" % str(result)

        next_state = BarsProtocolState.CHANGE_PARAMS_MENU

        return (success, next_state, result)

    def _handler_main_menu_show_system_diagnostics(self, params):
        """
        """

        self._logEvent(params)

        success = InstErrorCode.OK
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

        return (success, next_state, result)

    def _handler_main_menu_enter_set_system_clock(self, params):
        """
        """

        self._logEvent(params)

        success = InstErrorCode.OK
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

        return (success, next_state, result)

    def _handler_change_params_menu_exit(self, params):
        """
        handler to exit change params menu
        """

        self._logEvent(params)

        success = InstErrorCode.OK

        #
        # Send '3'
        #
        result = self._do_cmd_resp(BarsProtocolEvent.ENTER_CHANGE_PARAMS)

        next_state = BarsProtocolState.MAIN_MENU

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

        next_state = BarsProtocolState.MAIN_MENU

        return (success, next_state, result)
