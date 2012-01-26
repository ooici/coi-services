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
from ion.services.mi.instrument_protocol_eh import InstrumentProtocol

from ion.services.mi.common import InstErrorCode
from ion.services.mi.instrument_driver_eh import DriverEvent

from ion.services.mi.instrument_fsm import InstrumentFSM


#import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


class BarsProtocolState(BaseEnum):
    COLLECTING_DATA = 'COLLECTING_DATA'
    MAIN_MENU = 'MAIN_MENU'
    CHANGE_PARAMS_MENU = 'CHANGE_PARAMS_MENU'
    WAITING_FOR_SYSTEM_DIAGNOSTICS = 'WAITING_FOR_SYSTEM_DIAGNOSTICS'
    SETTING_SYSTEM_CLOCK = 'SETTING_SYSTEM_CLOCK'
    WAITING_FOR_SYSTEM_INFO = 'WAITING_FOR_SYSTEM_INFO'


class BarsProtocolEvent(BaseEnum):
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
class BarsInstrumentProtocol(InstrumentProtocol):
    """The instrument protocol classes to deal with a TRHPH BARS sensor.

    """

    def __init__(self):
        """
        Creates an instance of this protocol. This basically sets up the
        state machine, which is initialized in the COLLECTING_DATA state.
        """

        InstrumentProtocol.__init__(self)

        self._linebuf = ''

        state_handlers = {
            BarsProtocolState.COLLECTING_DATA:
                self._state_handler_collecting_data,
            BarsProtocolState.MAIN_MENU: self._state_handler_main_menu,
            BarsProtocolState.CHANGE_PARAMS_MENU:
                self._state_handler_change_params_menu,
            BarsProtocolState.WAITING_FOR_SYSTEM_DIAGNOSTICS:
                self._state_handler_waiting_for_system_diagnostics,
            BarsProtocolState.SETTING_SYSTEM_CLOCK:
                self._state_handler_setting_system_clock,
            BarsProtocolState.WAITING_FOR_SYSTEM_INFO:
                self._state_handler_waiting_for_system_info,
        }

        #
        # NOTE assume the instrument is collecting data
        # TODO: what if the instrument is NOT currently collecting data?
        #
        self._fsm = InstrumentFSM(BarsProtocolState, BarsProtocolEvent,
                                  state_handlers,
                                  BarsProtocolEvent.RESTART_DATA_COLLECTION,
                                  BarsProtocolEvent.EXIT)

        self._fsm.start(BarsProtocolState.COLLECTING_DATA)

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

        if event == BarsProtocolEvent.ENTER_MAIN_MENU:

            #TODO send 19 (Control-S) to instrument
            #...

            next_state = BarsProtocolState.MAIN_MENU

        elif event == BarsProtocolEvent.RESTART_DATA_COLLECTION:
            pass

        elif event == BarsProtocolEvent.EXIT:
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

        if event == BarsProtocolEvent.RESTART_DATA_COLLECTION:

            #TODO send '1' to instrument
            # ...

            next_state = BarsProtocolState.MAIN_MENU

        elif event == BarsProtocolEvent.ENTER_CHANGE_PARAMS:

            #TODO send '2' to instrument
            # ...

            next_state = BarsProtocolState.CHANGE_PARAMS_MENU

        elif event == BarsProtocolEvent.SHOW_SYSTEM_DIAGNOSTICS:

            #TODO send '3' to instrument
            # ...

            next_state = BarsProtocolState.WAITING_FOR_SYSTEM_DIAGNOSTICS

        elif event == BarsProtocolEvent.ENTER_SET_SYSTEM_CLOCK:

            #TODO send '4' to instrument
            # ...

            next_state = BarsProtocolState.SETTING_SYSTEM_CLOCK

        elif event == BarsProtocolEvent.ENTER_MAIN_MENU:
            pass

        elif event == BarsProtocolEvent.EXIT:
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

        elif event == BarsProtocolEvent.EXIT:
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

        next_state = BarsProtocolState.MAIN_MENU

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

        elif event == BarsProtocolEvent.EXIT:
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

        next_state = BarsProtocolState.MAIN_MENU

        return (success, next_state, result)
