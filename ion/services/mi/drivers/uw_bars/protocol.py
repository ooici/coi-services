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
from ion.services.mi.instrument_protocol import InstrumentProtocol
from ion.services.mi.instrument_driver import DriverState

from ion.services.mi.common import InstErrorCode
from ion.services.mi.instrument_fsm import InstrumentFSM


#import ion.services.mi.mi_logger
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

    # TODO events associated to other menus
    TBD = "??"


class BarsPrompt(BaseEnum):
    """
    BARS io prompts.
    """
    NEWLINE = '\r\n'  # TODO newline sequence TBD
    ENTER_MENU_OPTION = 'Enter 0, 1, 2, 3, 4, 5 or 6 here  -->'


####################################################################
# Protocol
####################################################################
class BarsInstrumentProtocol(InstrumentProtocol):
    """The instrument protocol classes to deal with a TRHPH BARS sensor.

    """

    def __init__(self):
        """
        Creates an instance of this protocol. This basically sets up the
        state machine, which is initialized in the INIT state.
        """

        InstrumentProtocol.__init__(self)

        self._fsm = InstrumentFSM(BarsProtocolState, BarsProtocolEvent,
                                  BarsProtocolEvent.RESTART_DATA_COLLECTION,
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
                              self._handler_collecting_data_enter_main_menu)

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
                              BarsProtocolEvent.TBD,
                              self._handler_change_params_menu_TBD)

        # we start in the PRE_INIT state
        self._fsm.start(BarsProtocolState.PRE_INIT)

    def _logEvent(self, params):
        log.info("_logEvent: curr_state=%s, params=%s" %
                 (self._fsm.get_current_state(), str(params)))

    def _got_data(self, data):
        """
        NOTE: InstrumentProtocol.attach assumes the method _got_data but it's
        not defined there but in the CommandResponseInstrumentProtocol subclass
        """

        print("_got_data: '%s'" % data.replace('\n', '\\n'))

    def get_current_state(self):
        """Gets the current state of the protocol."""
        return self._fsm.get_current_state()

    def connect(self, timeout=10):
        super(BarsInstrumentProtocol, self).connect(timeout)
        self._fsm.on_event(BarsProtocolEvent.INITIALIZE)


    ########################################################################
    # State handlers
    ########################################################################

    def _handler_pre_init_initialize(self, params):
        """
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
        """

        self._logEvent(params)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #TODO send 19 (Control-S) to instrument
        #...

        next_state = BarsProtocolState.MAIN_MENU

        return (success, next_state, result)

    def _handler_main_menu_restart_data_collection(self, params):
        """
        """

        self._logEvent(params)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #TODO send '1' to instrument
        # ...

        next_state = BarsProtocolState.MAIN_MENU

        return (success, next_state, result)

    def _handler_main_menu_enter_change_params(self, params):
        """
        """

        self._logEvent(params)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #TODO send '2' to instrument
        # ...

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

    def _handler_change_params_menu_TBD(self, params):
        """
        """

        self._logEvent(params)

        success = InstErrorCode.OK
        next_state = None
        result = None

        #
        #TODO implement.  for now returning to MAIN_MENU
        #

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
