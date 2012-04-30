#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.trhph_client
@file ion/services/mi/drivers/uw_trhph/trhph_client.py
@author Carlos Rueda

@brief TRHPH instrument client module.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

"""
TrhphClient allows iteraction with the TRHPH instrument or simulator via a
socket.
A demo program can be run as follows:
    $ bin/python ion/services/mi/drivers/uw_trhph/trhph_client.py \
           --host 10.180.80.172 --port 2001 --outfile output.txt
"""

import sys
import socket
import os
import re
import time

import gevent
from gevent import Greenlet, sleep

import logging
from ion.services.mi.mi_logger import mi_logger
log = mi_logger

import ion.services.mi.drivers.uw_trhph.trhph as trhph


NEWLINE = trhph.NEWLINE
EOF = '\x04'
CONTROL_S = '\x13'

DATA_LINE_PATTERN = re.compile(r'.*((\d+\.\d+\s*){12})$')

MAIN_MENU_PATTERN = re.compile(
    r'.*Welcome to the Consortium for Ocean Leadership Program Main Menu.*')
MAIN_MENU_PROMPT_PATTERN = re.compile(
    r'.*Enter 0, 1, 2, 3, 4, 5, or 6 here  -->.*')

SYSTEM_PARAM_MENU_PATTERN = re.compile(r'.*System Parameter Menu.*')
SYSTEM_PARAM_MENU_PROMPT_PATTERN = re.compile(
    r'.*Enter 0, 1, 2, or 3 here  -->.*')

# System info: the following pattern was enough for the BARS instrument but
# not for the TRHPH.
# TODO TRHPH complete extraction of system info 
SYSTEM_INFO_PATTERN = re.compile(
r'.*(System Name|System Owner|Contact Info|System Serial #): (.*)$')

ENTER_NUM_SCANS_PATTERN = re.compile(r'.*How Many Scans do you want\? --> ')

DIAGNOSTICS_HEADER_PATTERN = re.compile('-Res/5-.*-VBatt-')

DIAGNOSTICS_DATA_PATTERN = re.compile(r'.*((\d+\.\d+\s*){10})$')

RETURN_MAIN_MENU_PATTERN = re.compile(r'.*Press Enter to return to Main Menu.')

POWER_STATUS_MENU_PATTERN = re.compile(r'.*Sensor Power Control Menu.*')

POWER_STATUS_ITEM_PATTERN = re.compile(r'\s*([^.]*)\.+\s*(On|Off)')

GENERIC_PROMPT_PATTERN = re.compile(r'.*--> ')

# keep this max number of received lines
MAX_NUM_LINES = 30

# default value for the generic timeout. By default, 30 secs
DEFAULT_GENERIC_TIMEOUT = 30


# map from power status key to the command to toggle it:
_toggle_power_cmd = {
    'Res Sensor Power': '1',
    'Instrumentation Amp Power': '2',
    'eH Isolation Amp Power': '3',
    'Hydrogen Power': '4',
    'Reference Temperature Power': '5'
}


class State(object):
    """Instrument states"""
    COLLECTING_DATA = "COLLECTING_DATA"
    MAIN_MENU = "MAIN_MENU"
    SYSTEM_PARAM_MENU = "SYSTEM_PARAM_MENU"
    SYSTEM_INFO = "SYSTEM_INFO"
    ENTER_NUM_SCANS = "ENTER_NUM_SCANS"
    DIAGNOSTICS_INFO = "DIAGNOSTICS_INFO"
    POWER_STATUS_MENU = "POWER_STATUS_MENU"
    SOME_MENU = "SOME_MENU"


class TrhphClientException(Exception):
    """Some exception occured in TrhphClient."""


class TimeoutException(TrhphClientException):
    """Timeout while waiting for some event or state in the instrument."""

    def __init__(self, timeout, msg=None,
                 expected_state=None, curr_state=None):

        self.timeout = timeout
        self.expected_state = expected_state
        self.curr_state = curr_state
        if msg:
            self.msg = msg
        elif expected_state:
            self.msg = "timeout while expecting state %s" % expected_state
            if curr_state:
                self.msg += " (curr_state=%s)" % curr_state
        else:
            self.msg = "timeout reached"

    def __str__(self):
        s = "TimeoutException(msg='%s'" % self.msg
        s += "; timeout=%s" % str(self.timeout)
        if self.expected_state:
            s += "; expected_state=%s" % self.expected_state
        if self.curr_state:
            s += "; curr_state=%s" % self.curr_state
        s += ")"
        return s


class _Recv(Greenlet):
    """
    Thread to receive and optionaly write the received data to a given file.
    It keeps internal buffers to support checking for expected contents,
    and also keeps a current state according to the received information.
    Each line in the generated file will optionally be prefixed with the
    current state to facilitate debugging.
    """

    def __init__(self, sock, data_listener, outfile=None, prefix_state=False):
        """
        @param sock The connection to read in characters from the instrument.
        @param data_listener data_listener(sample) is called whenever a
               new data line is received, where sample is a dict indexed by
               the names in trhph.CHANNEL_NAMES.
        @param outfile name of output file for all received data; None by
                default.
        @param prefix_state True to prefix each line in the outfile with the
                  current state; False by default.
        """
#        Thread.__init__(self, name="_Recv")
        Greenlet.__init__(self)
        self._sock = sock
        self._data_listener = data_listener
        self._last_line = ''
        self._new_line = ''
        self._lines = []
        self._active = True
        self._outfile = outfile
        self._prefix_state = prefix_state
        self._state = None
#        self.setDaemon(True)

        self._last_data_burst = None
        self._diagnostic_data = []
        self._system_info = {}
        self._power_statuses = {}

        log.debug("_Recv created.")

    def _update_lines(self, c):
        """
        Updates the internal buffers.
        @param c Character that has just been received
        """
        if c == '\n':
            self._last_line = self._new_line
            self._new_line = ''
            self._lines.append(self._last_line)
            if len(self._lines) > MAX_NUM_LINES:
                self._lines = self._lines[1 - MAX_NUM_LINES:]
        else:
            self._new_line += c

    def _update_state(self):
        """
        Updates the state according to the last received information.
        """
        # use the last non-empty line:
        line = self._last_line if self._new_line == '' else self._new_line
        if line is None:
            return

        prev_state = self._state

        if DATA_LINE_PATTERN.match(line):
            if self._new_line == '':
                # this condition is to make sure we have received a complete
                # line to proceed with the (potential) state transition. In
                # particular, _update_values will have a complete data line
                # for the update.
                self._state = State.COLLECTING_DATA

        elif MAIN_MENU_PATTERN.match(line) or\
             MAIN_MENU_PROMPT_PATTERN.match(line):
            self._state = State.MAIN_MENU

        elif SYSTEM_PARAM_MENU_PATTERN.match(line):
            self._state = State.SYSTEM_PARAM_MENU

        elif SYSTEM_INFO_PATTERN.match(line):
            self._state = State.SYSTEM_INFO

        elif ENTER_NUM_SCANS_PATTERN.match(line):
            self._state = State.ENTER_NUM_SCANS

        elif DIAGNOSTICS_HEADER_PATTERN.match(line):
            self._state = State.DIAGNOSTICS_INFO

        elif POWER_STATUS_MENU_PATTERN.match(line):
            self._state = State.POWER_STATUS_MENU

        elif GENERIC_PROMPT_PATTERN.match(line):
            #
            # update only if not already known; this is because we use a line
            # or two of a menu contents to determine a particular menu, but the
            # menu itself usually ends with the generic prompt.
            #
            if self._state is None:
                self._state = State.SOME_MENU

        if prev_state != self._state and log.isEnabledFor(logging.INFO):
            transition = "%s ==> %s" % (prev_state, self._state)
            log.info("%-40s |%s" % (transition, repr(line)))

    def _notify_data_sample(self, data_burst):
        """
        Prepares a data sample and notifies the listener.
        @param data_burst received data burst to create corresponding sample.
        """
        assert len(trhph.CHANNEL_NAMES) == len(data_burst)
        if self._data_listener:
            sample = {}
            index = 0
            for name in trhph.CHANNEL_NAMES:
                sample[name] = data_burst[index]
                index += 1
            self._data_listener(sample)

    def _update_values(self, c):
        """
        Updates internal values according to the current state and the last
        received information. It only tries any update if a line has just
        been completed.
        """
        if c != '\n':
            return

        # use the last non-empty line:
        line = self._last_line if self._new_line == '' else self._new_line
        if line is None:
            return

        if self._state == State.COLLECTING_DATA:
            mo = DATA_LINE_PATTERN.search(line)
            if mo:
                dataline = mo.group(1)
                self._last_data_burst = [float(v) for v in dataline.split()]
                log.debug("_last_data_burst = %s" % str(self._last_data_burst))
                self._notify_data_sample(self._last_data_burst)

        elif self._state == State.DIAGNOSTICS_INFO:
            mo = DIAGNOSTICS_DATA_PATTERN.search(line)
            if mo:
                dataline = mo.group(1)
                diag_data_row = [float(v) for v in dataline.split()]
                log.debug("diag_data_row = %s" % str(diag_data_row))
                self._diagnostic_data.append(diag_data_row)

        elif self._state == State.SYSTEM_INFO:
            mo = SYSTEM_INFO_PATTERN.search(line)
            if mo:
                key = mo.group(1)
                val = mo.group(2).strip()
                self._system_info[key] = val
                log.debug("_system_info: %s = %s" % (repr(key), repr(val)))

        elif self._state == State.POWER_STATUS_MENU:
            mo = POWER_STATUS_ITEM_PATTERN.search(line)
            if mo:
                key = mo.group(1).strip()
                # remove the trailing " is" if present:
                if key.endswith(" is"):
                    key = key[:-len(" is")]
                val = mo.group(2).strip()
                self._power_statuses[key] = "On" == val
                log.debug("POWER STATUS: %s = %s" % (repr(key), repr(val)))

    def end(self):
        self._active = False

    def _update_outfile(self, c):
        """
        Updates the outfile if any.
        @param c Character that has just been received
        """
        if self._outfile:
            os.write(self._outfile.fileno(), c)
            if c == '\n' and self._prefix_state:
                os.write(self._outfile.fileno(), "%20s| " % self._state)
            self._outfile.flush()

    def _end_outfile(self):
        """
        Writes a mark to the outfile to indicate that
        the receiving thread has ended.
        """
        if self._outfile:
            os.write(self._outfile.fileno(), '\n\n<_Recv ended.>\n\n')
            self._outfile.flush()

    def _run(self):
        """
        Runs the receiver while updating buffers and state as appropriate.
        """

        # set timeout to the socket, mainly intended to allow yielding control
        # to other greenlets:
        if self._sock.gettimeout() is None:
            self._sock.settimeout(0.5)

        log.debug("_Recv running.")
        while self._active:
            # Note that we read one character at a time.
            try:
                c = self._sock.recv(1)
            except socket.timeout, e:
                # ok, just reattempt reading
                continue
            self._update_lines(c)
            self._update_state()
            self._update_values(c)
            self._update_outfile(c)
        log.debug("_Recv.run done.")
        self._end_outfile()


class TrhphClient(object):
    """
    TCP based client for communication with the TRHPH instrument.
    """

    def __init__(self, host, port, outfile=None, prefix_state=False):
        """
        Creates a TrhphClient instance.
        @param host
        @param port
        @param outfile name of output file for all received data; None by
                default.
        @param prefix_state True to prefix each line in the outfile with the
                  current state; False by default.
        """
        self._host = host
        self._port = port
        self._data_listener = None
        self._sock = None
        self._outfile = outfile
        self._prefix_state = prefix_state
        self._bt = None

        """sleep time used just before sending data"""
        self.delay_before_send = 0.2

        """sleep time used just before a expect operation"""
        self.delay_before_expect = 0.5

        """generic timeout for various operations"""
        self._generic_timeout = DEFAULT_GENERIC_TIMEOUT

    def set_data_listener(self, data_listener):
        """
        Sets the data listener. Call this before connect()'ing to have effect.
        @param data_listener data_listener(sample) is called whenever a
               new data line is received, where sample is a dict indexed by
               the names in trhph.CHANNEL_NAMES.
        """
        self._data_listener = data_listener

    def set_generic_timeout(self, timeout):
        """Sets generic timeout for various operations.
           By default DEFAULT_GENERIC_TIMEOUT."""
        self._generic_timeout = timeout
        log.info("Generic timeout set to: %d" % self.generic_timeout)

    @property
    def generic_timeout(self):
        """Generic timeout for various operations.
           By default DEFAULT_GENERIC_TIMEOUT."""
        return self._generic_timeout

    def connect(self, max_attempts=4, time_between_attempts=10):
        """
        Establishes the connection and starts the receiving thread.
        The connection is attempted a number of times.
        @param max_attempts Maximum number of socket connection attempts
                            (4 by default).
        @param time_between_attempts Time in seconds between attempts
                            (5 seconds by default).
        @throws socket.error The socket.error that was raised during the
                         last attempt.
        """
        assert self._sock is None

        host, port = self._host, self._port
        last_error = None
        attempt = 0
        while self._sock is None and attempt < max_attempts:
            attempt += 1
            log.info("Trying to connect to %s:%s (attempt=%d)" %
                     (host, port, attempt))
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                self._sock = sock  # success.
            except socket.error, e:
                log.info("Socket error while trying to connect: %s" %
                          str(e))
                last_error = e
                if attempt < max_attempts:
                    log.info("Re-attempting in %s secs ..." %
                              str(time_between_attempts))
                    gevent.sleep(time_between_attempts)

        if self._sock:
            log.info("Connected to %s:%s" % (host, port))

            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1)

            self._bt = _Recv(self._sock, self._data_listener,
                             self._outfile, self._prefix_state)
            self._bt.start()
            log.info("_Recv thread started.")
        else:
            raise last_error

    def end(self):
        """
        Ends the client.
        """
        if self._sock is None:
            log.warn("end() called again")
            return

        log.info("closing connection")
        try:
            self._bt.end()
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
            log.info("socket shutdown and closed")
        finally:
            self._sock = None

    def get_current_state(self, timeout=None):
        """
        Gets the current state of the instrument according to the received
        information from it. If no information has yet been received or if the
        state needs to be refreshed, then this method runs some
        heuristics to determine the state.

        @param timeout Timeout for the check, self.generic_timeout by default.
        @throws TimeoutException
        """

        if self._bt._state:
            # already known: return it immediately
            return self._bt._state

        self._refresh_state()

        if self._bt._state:
            log.info("get_current_state: %s" % self._bt._state)
            return self._bt._state
        else:
            raise TimeoutException(timeout)

    def _refresh_state(self):
        """
        Syncs the state. The method tries first to determine whether the
        instrument is in some menu (taking on this at most 15-20 secs); if
        this fails to be confirmed, then the state is assumed to be
        COLLECTING_DATA.
        """

        log.info("refreshing state ...")

        # 1. without sending anything, check regularly for up to a few secs
        # in case some output is currently being generated
        time_limit = time.time() + 5
        while self._bt._state is None and time.time() <= time_limit:
            gevent.sleep(1)
        if self._bt._state:
            return  # we got the state

        # 2. send a few "backspaces" (^H) to delete any ongoing partial entry
        for i in range(5):
            self._send_control('h')
            gevent.sleep(0.5)

        # 3. send ^M in case the instrument is in some menu, and check
        self.send_enter()
        time_limit = time.time() + 10
        while self._bt._state is None and time.time() <= time_limit:
            gevent.sleep(2)
        if self._bt._state:
            return  # we got the state

        # 4. the instrument does not react to ^M in streaming mode,
        # so just assume it is collecting data:
        self._bt._state = State.COLLECTING_DATA
        log.info("assuming %s" % self._bt._state)

    def is_collecting_data(self, timeout=None):
        """returns State.COLLECTING_DATA == self.get_current_state(timeout)"""
        return State.COLLECTING_DATA == self.get_current_state(timeout)

    def _assert_state(self, state):
        assert self._bt._state == state, \
            "expected=%s; current=%s" % (state, self._bt._state)

    def _enter_main_menu(self, timeout=None):
        """
        Assumes State.COLLECTING_DATA.
        Sends ^S characters repeatily until getting a prompt.
        It does this every 5 seconds.

        @param timeout Timeout for the operation, self.generic_timeout by
                default.
        @throws TimeoutException
        """

        self._assert_state(State.COLLECTING_DATA)

        timeout = timeout or self.generic_timeout
        log.debug("_enter_main_menu ... (timeout=%s)" % timeout)

        time_limit = time.time() + timeout
        got_prompt = False
        while not got_prompt and time.time() <= time_limit:
            log.debug("sending ^S")
            self._send_control('s')
            gevent.sleep(2)
            string = self._bt._new_line
            got_prompt = GENERIC_PROMPT_PATTERN.match(string) is not None

        if got_prompt:
            log.debug("got prompt. Sending one ^M to clean up any ^S leftover")
            self._send_control('m')
        else:
            raise TimeoutException(timeout,
                                   msg="while trying to enter main menu")

    def go_to_main_menu(self, timeout=None):
        """
        Goes to the main menu from whatever the current state is.
        """
        # TODO handle from all possible submenus/screens.

        timeout = timeout or self.generic_timeout
        log.debug("go_to_main_menu ... (timeout=%s)" % timeout)

        state = self.get_current_state(timeout=timeout)

        if State.MAIN_MENU == state:
            log.debug("Already in %s" % state)
            return

        if self._bt._state == State.COLLECTING_DATA:
            self._enter_main_menu()
            return

        # sequence of commands to return to main menu
        cmds = []

        if State.SYSTEM_PARAM_MENU == state:
            cmds.append(('3', 'return to main menu'))
        elif State.DIAGNOSTICS_INFO == state:
            cmds.append(('\r', 'return to main menu'))
        elif State.SYSTEM_INFO == state:
            cmds.append(('\r', 'return to main menu'))
        elif State.POWER_STATUS_MENU == state:
            cmds.append(('6', 'return to main menu'))

        else:
            log.warn("NOT yet handled in the %s state" % str(state))
            return

        # issue commands to return to main menu:
        for cmd, msg in cmds:
            log.info("sending '%s' to %s" % (cmd, msg))
            if '\r' == cmd:
                self.send_enter()
            else:
                self.send(cmd)
            gevent.sleep(4)

        self.expect_generic_prompt(timeout=timeout)

        if State.MAIN_MENU == self._bt._state:
            log.info("Now in state %s" % self._bt._state)
        else:
            log.warn("Should have gotten to the %s state, but got %s"
                        % (State.MAIN_MENU, state))

    def go_to_system_param_menu(self, timeout=None):
        """
        Goes to the system param menu from whatever the current state is.
        """
        # TODO handle from all possible submenus/screens.

        timeout = timeout or self.generic_timeout
        log.debug("go_to_system_param_menu ... (timeout=%s)" % timeout)

        state = self.get_current_state(timeout=timeout)

        if State.SYSTEM_PARAM_MENU == state:
            log.debug("Already in %s" % state)
            return

        # for simplicity, go to main menu and then enter the system param menu

        log.info("go_to_system_param_menu: going to main menu first")
        self.go_to_main_menu(timeout)
        self._assert_state(State.MAIN_MENU)

        log.debug("select 2 to get system parameter menu")
        self.send('2')
        self.expect_generic_prompt(timeout=timeout)

    def resume_data_streaming(self, timeout=None):
        """
        Requests that streaming be resumed.
        """
        # TODO handle from all possible submenus/screens.

        timeout = timeout or self.generic_timeout
        log.debug("resume_data_streaming ... (timeout=%s)" % timeout)

        state = self.get_current_state(timeout=timeout)
        if State.COLLECTING_DATA == state:
            log.debug("Already in %s" % state)
            return

        if State.MAIN_MENU != state:
            self.go_to_main_menu(timeout=timeout)

        state = self.get_current_state(timeout=timeout)
        if State.MAIN_MENU == state:
            # expect prompt for menu is completely output
            self.expect_generic_prompt()
            log.info("sending '1' to resume data streaming")
            self.send('1')
            # and set state to None for a refresh in next operation
            self._bt._state = None
        else:
            log.warn("Should have gotten to the %s state, but got %s"
                        % (State.MAIN_MENU, state))

    def get_data_collection_params(self, timeout=None):
        """
        Gets the tuple (seconds, is_data_only), that is,
                    the cycle time (int) and the data only flag (bool).

        @param timeout Timeout for the wait on each instrument interaction,
                   self.generic_timeout by default.
        @retval (seconds, is_data_only), that is,
                    the cycle time (int) and the data only flag (bool).
        @throws TrhphClientException if received string could not be matched
        @throws TimeoutException
        """

        timeout = timeout or self.generic_timeout
        self.go_to_system_param_menu(timeout=timeout)
        self._assert_state(State.SYSTEM_PARAM_MENU)

        buffer = self.get_last_buffer()
        log.debug("BUFFER=[%s]" % str(buffer))

        # capture seconds
        seconds_string = trhph.get_cycle_time(buffer)
        log.debug("seconds_string='%s'" % seconds_string)
        if seconds_string is None:
            raise TrhphClientException("seconds_string could not be matched")
        seconds = trhph.get_cycle_time_seconds(seconds_string)
        if seconds is None:
            raise TrhphClientException("Seconds could not be matched")

        # capture verbose thing
        data_only_string = trhph.get_verbose_vs_data_only(buffer)
        log.info("data_only_string='%s'" % data_only_string)
        is_data_only = trhph.DATA_ONLY == data_only_string

        log.debug("send 9 to return to main menu")
        self.send('9')
        self.expect_generic_prompt(timeout=timeout)

        return (seconds, is_data_only)

    def set_cycle_time(self, seconds, timeout=None):
        """
        Sets the cycle time.

        @param seconds cycle time in seconds, which must be >= 15.
        @param timeout Timeout for the wait on each instrument interaction,
                self.generic_timeout by default.
        @throws TimeoutException
        """

        assert isinstance(seconds, int)
        assert seconds >= 15

        timeout = timeout or self.generic_timeout
        self.go_to_system_param_menu(timeout=timeout)
        self._assert_state(State.SYSTEM_PARAM_MENU)

        log.debug("send 1 to change cycle time")
        self.send('1')
        self.expect_generic_prompt(timeout=timeout)

        if seconds <= 59:
            log.debug("send 1 to change cycle time in seconds")
            self.send('1')
            self.expect_generic_prompt(timeout=timeout)
            log.debug("send seconds=%d" % seconds)
            self.send(str(seconds))
        else:
            log.debug("send 2 to change cycle time in minutes")
            self.send('2')
            self.expect_generic_prompt(timeout=timeout)
            minutes = seconds / 60
            log.debug("send minutes=%d" % minutes)
            self.send(str(minutes))

        self.expect_generic_prompt(timeout=timeout)

        log.debug("send 9 to return to main menu")
        self.send('9')
        self.expect_generic_prompt(timeout=timeout)

    def set_is_data_only(self, data_only, timeout=None):
        """
        Sets the data-only flag.

        @param data_only True to set data-only; False for verbose.
        @param timeout Timeout for the wait on each instrument interaction,
                self.generic_timeout by default.
        @throws TimeoutException
        """

        assert isinstance(data_only, bool)

        timeout = timeout or self.generic_timeout
        self.go_to_system_param_menu(timeout=timeout)
        self._assert_state(State.SYSTEM_PARAM_MENU)

        buffer = self.get_last_buffer()
        log.debug("BUFFER='%s'" % repr(buffer))
        string = trhph.get_cycle_time(buffer)
        log.debug("VALUE='%s'" % string)

        log.debug("send 2 to change verbose setting")
        self.send('2')
        self.expect_generic_prompt(timeout=timeout)

        if data_only:
            log.debug("send 1 to change verbose setting to just data")
            self.send('1')
        else:
            log.debug("send 2 to change verbose setting to verbose")
            self.send('2')

        self.expect_generic_prompt(timeout=timeout)

        log.debug("send 9 to return to main menu")
        self.send('9')
        self.expect_generic_prompt(timeout=timeout)

    def get_system_info(self, timeout=None):
        """
        Gets the system info.

        @param timeout Timeout for the wait, self.generic_timeout by default.
        @retval a dict with the system info elements
        @throws TimeoutException
        """

        timeout = timeout or self.generic_timeout
        self.go_to_main_menu(timeout=timeout)
        self._assert_state(State.MAIN_MENU)

        log.info("sending '5' to get system info")
        self.send('5')

        time_limit = time.time() + timeout
        while self._bt._state != State.SYSTEM_INFO and time.time() <= \
                                                       time_limit:
            gevent.sleep(1)

        if self._bt._state != State.SYSTEM_INFO:
            raise TimeoutException(
                    timeout, expected_state=State.COLLECTING_DATA,
                    curr_state=self._bt._state)

        self.expect_generic_prompt(timeout=timeout)

        # send enter to return to main menu
        log.info("send enter to return to main menu")
        self.send_enter()
        gevent.sleep(1)

        return self._bt._system_info

    def execute_diagnostics(self, num_scans, timeout=None):
        """
        Executes the diagnostics operation.

        @param num_scans the number of scans for the operation
        @param timeout Timeout for the wait, self.generic_timeout by default.
        @retval a list of rows, one with values per scan
        @throws TimeoutException
        """

        timeout = timeout or self.generic_timeout
        self.go_to_main_menu(timeout=timeout)
        self._assert_state(State.MAIN_MENU)

        # re-init diagnostic data:
        self._bt._diagnostic_data = []

        log.info("sending '3' to enter diagnostics menu")
        self.send('3')

        time_limit = time.time() + timeout
        while (self._bt._state != State.ENTER_NUM_SCANS) and\
              (time.time() <= time_limit):
            gevent.sleep(1)

        if self._bt._state != State.ENTER_NUM_SCANS:
            raise TimeoutException(
                    timeout, expected_state=State.COLLECTING_DATA,
                    curr_state=self._bt._state)

        # enter number of scans and wait for the return to menu message:
        self.send(str(num_scans))
        self.expect_line(RETURN_MAIN_MENU_PATTERN)

        # send enter to return to main menu
        log.info("send enter to return to main menu")
        self.send_enter()
        self.expect_generic_prompt(timeout=timeout)

        return self._bt._diagnostic_data

    def get_power_statuses(self, timeout=None):
        """
        Gets the power statuses.

        @param timeout Timeout for the wait, self.generic_timeout by default.
        @retval a dict of power statuses
        @throws TimeoutException
        """

        timeout = timeout or self.generic_timeout
        self.go_to_main_menu(timeout=timeout)
        self._assert_state(State.MAIN_MENU)

        # re-init _power_statuses:
        self._bt._power_statuses = {}

        log.info("sending '4' to get power statuses")
        self.send('4')

        time_limit = time.time() + timeout
        while (self._bt._state != State.POWER_STATUS_MENU) and\
              (time.time() <= time_limit):
            gevent.sleep(1)

        if self._bt._state != State.POWER_STATUS_MENU:
            raise TimeoutException(
                    timeout, expected_state=State.POWER_STATUS_MENU,
                    curr_state=self._bt._state)

        # expect generic prompt, at which point all the statuses would have
        # been captured (just use the same timeout again)
        self.expect_generic_prompt(timeout=timeout)

        # send '6' to return to main menu
        log.info("send '9' to return to main menu")
        self.send('9')
        self.expect_generic_prompt(timeout=timeout)

        return self._bt._power_statuses

    def set_power_statuses(self, new_power_statuses, timeout=None):
        """
        Sets the power statuses.

        @param new_power_statuses dict with (key:bool) items to set the new
                    statuses
        @param timeout Timeout for the wait, self.generic_timeout by default.
        @throws TimeoutException
        """

        timeout = timeout or self.generic_timeout
        self.go_to_main_menu(timeout=timeout)
        self._assert_state(State.MAIN_MENU)

        # first, we get the current statuses:
        # re-init _power_statuses:
        self._bt._power_statuses = {}

        log.info("sending '4' to get power statuses")
        self.send('4')

        time_limit = time.time() + timeout
        while (self._bt._state != State.POWER_STATUS_MENU) and\
              (time.time() <= time_limit):
            gevent.sleep(1)

        if self._bt._state != State.POWER_STATUS_MENU:
            raise TimeoutException(
                    timeout, expected_state=State.POWER_STATUS_MENU,
                    curr_state=self._bt._state)

        self.expect_generic_prompt(timeout=timeout)

        # Then, compare current values with the desired ones to toggle
        # as needed:

        for (key, new_val) in new_power_statuses.items():
            curr_val = self._bt._power_statuses.get(key, new_val)
            if curr_val != new_val:
                # issue command to toggle this status:
                cmd = _toggle_power_cmd.get(key, None)
                if cmd:
                    self.send(cmd)
                    self.expect_generic_prompt(timeout=timeout)
                else:
                    log.warn("Unexpected key for toggle command: %s" % key)

        # send '9' to return to main menu
        log.info("send '9' to return to main menu")
        self.send('9')
        self.expect_generic_prompt(timeout=timeout)

    def send_and_expect_state(self, string, state, timeout=None):
        """
        Sends a string and expects a given state.
        @param string the string to send
        @param state the expected state
        @param timeout Timeout for the wait, self.generic_timeout by default.
        @throws TimeoutException if cannot get to the expected state.
        """

        # TODO: mark the current position of the received messages as a way
        # to actually check for new information
        self.send(string)

        gevent.sleep(2)

        timeout = timeout or self._generic_timeout
        time_limit = time.time() + timeout
        while self._bt._state != state and time.time() <= time_limit:
            gevent.sleep(1)

        if self._bt._state != state:
            raise TimeoutException(
                    timeout, expected_state=state,
                    curr_state=self._bt._state)

    def get_last_buffer(self):
        return '\n'.join(self._bt._lines)

    def send_enter(self):
        """
        Sleeps for self.delay_before_send and calls self._send_control('m').
        """
        gevent.sleep(self.delay_before_send)
        log.debug("send_enter")
        self._send_control('m')

    def send(self, string):
        """
        Sleeps for self.delay_before_send and then sends string + '\r'
        """
        gevent.sleep(self.delay_before_send)
        s = string + '\r'
        self._send(s)

    def expect_line(self, pattern, pre_delay=None, timeout=None):
        """
        Sleeps for the given pre_delay seconds (self.delay_before_expect by
        default).
        Then waits until the given pattern matches the current contents of the
        last received line. It does the check every half second.

        @pattern The pattern (a string or the result of a re.compile)
        @param pre_delay For the initial sleep (self.delay_before_expect by
                default).
        @param timeout Timeout for the wait, self.generic_timeout by default.
        @throws TimeoutException
        """

        timeout = timeout or self.generic_timeout
        pre_delay = pre_delay or self.delay_before_expect
        gevent.sleep(pre_delay)

        patt_str = pattern if isinstance(pattern, str) else pattern.pattern
        log.debug("expecting '%s'" % patt_str)
        time_limit = time.time() + timeout
        got_it = False
        while not got_it and time.time() <= time_limit:
            gevent.sleep(0.5)
            string = self._bt._new_line
            got_it = re.match(pattern, string) is not None

        if not got_it:
            raise TimeoutException(
                    timeout, msg="Expecting pattern %s" % repr(pattern))

    def expect_generic_prompt(self, pre_delay=None, timeout=None):
        """
        returns self.expect_line(GENERIC_PROMPT_PATTERN, pre_delay, timeout)
        where pre_delay is self.delay_before_expect by default.

        Note that the pre_delay is intended to allow the instrument to send new
        info after a preceeding requested option so as to avoid a false
        positive prompt verification.

        @param pre_delay For the initial sleep (self.delay_before_expect by
                default).
        @param timeout Timeout for the wait, self.generic_timeout by default.
        """
        return self.expect_line(GENERIC_PROMPT_PATTERN, pre_delay, timeout)

    def _send(self, s):
        """
        Sends a string. Returns the number of bytes written.
        """
        c = os.write(self._sock.fileno(), s)
        log.info("OUTPUT=%s" % repr(s))
        return c

    def _send_control(self, char):
        """
        Sends a control character.
        @param char must satisfy 'a' <= char.lower() <= 'z'
        """
        char = char.lower()
        assert 'a' <= char <= 'z'
        a = ord(char)
        a = a - ord('a') + 1
        return self._send(chr(a))


def main(host, port, timeout, outfile=sys.stdout, prefix_state=False):
    """
    Demo program:
    - checks if the instrument is collecting data
    - if not, this function returns False
    - breaks data streaming to enter main menu
    - selects 6 to get system info
    - sends enter to return to main menu
    - resumes data streaming
    - sleeps a few seconds to let some data to come in

    @param host Host of the instrument
    @param port Port of the instrument
    @param timeout Generic timeout
    @param outfile File object used to echo all received data from the socket
                   (by default, sys.stdout).
    """
    client = TrhphClient(host, port, outfile, prefix_state)
    try:
        if timeout:
            client.set_generic_timeout(timeout)
        client.connect()

        print ":: getting instrument state"
        state = client.get_current_state()
        print ":: current instrument state: %s" % str(state)

        print ":: getting system info"
        system_info = client.get_system_info()
        print ":: system_info = %s" % str(system_info)

        print ":: resuming data streaming"
        client.resume_data_streaming()
    finally:
        print ":: bye"
        client.end()


if __name__ == '__main__':
    usage = """USAGE: trhph_client.py [options]
       --host address      # instrument address (localhost)
       --port port         # instrument port (required)
       --timeout secs      # generic timeout (%s secs)
       --outfile filename  # file to save all received data (stdout)
       --prefix_state      # prefix state to each line in outfile (false)
       --loglevel level    # used to eval mi_logger.setLevel(logging.%%s)
    """ % DEFAULT_GENERIC_TIMEOUT
    usage += """\nExample: trhph_client.py --host 10.180.80.172 --port 2001"""

    host = 'localhost'
    port = None
    timeout = None
    outfile = sys.stdout
    prefix_state = False

    arg = 1
    while arg < len(sys.argv):
        if sys.argv[arg] == "--host":
            arg += 1
            host = sys.argv[arg]
        elif sys.argv[arg] == "--port":
            arg += 1
            port = int(sys.argv[arg])
        elif sys.argv[arg] == "--timeout":
            arg += 1
            timeout = int(sys.argv[arg])
        elif sys.argv[arg] == "--outfile":
            arg += 1
            outfile = file(sys.argv[arg], 'w')
        elif sys.argv[arg] == "--prefix_state":
            prefix_state = True
        elif sys.argv[arg] == "--loglevel":
            arg += 1
            loglevel = sys.argv[arg].upper()
            mi_logger = logging.getLogger('mi_logger')
            eval("mi_logger.setLevel(logging.%s)" % loglevel)
        else:
            print "error: unrecognized option %s" % sys.argv[arg]
            port = None
            break
        arg += 1

    if port is None:
        print usage
    else:
        main(host, port, timeout, outfile, prefix_state)
