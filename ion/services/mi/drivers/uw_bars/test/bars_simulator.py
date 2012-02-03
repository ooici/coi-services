#!/usr/bin/env python

"""
@package ion.services.mi.drivers.test.bars_simulator
@file ion/services/mi/drivers/test/bars_simulator.py
@author Carlos Rueda

@brief A simple simulator for the BARS instrument intended to facilitate
testing. It accepts a TCP connection on a port and starts by sending out
bursts of (random) data every few seconds. It dispatches the commands using,
for the momennt, ad hoc strings (not necessarily the exact strings used by
the real instrument).
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

import socket
import random
import time

from threading import Thread


NEWLINE = '\n'

UNRECOGNIZED = '???'


ENTER_MAIN_MENU = '^S'

# main menu commands:
PRINT_DATE_TIME = '0'
RESTART_DATA_COLLECTION = '1'
ENTER_CHANGE_PARAMS = '2'
SHOW_SYSTEM_DIAGNOSTICS = '3'
ENTER_SET_SYSTEM_CLOCK = '4'
SHOW_SYSTEM_INFO = '5'
EXIT_PROGRAM = '6'

# TODO change menu commands:
# ...


time_between_bursts = 2


class _BurstThread(Thread):
    """Thread to generate data bursts"""

    def __init__(self, conn, log_prefix):
        Thread.__init__(self, name="_BurstThread")
        self._conn = conn
        self._running = True
        self._enabled = True
        self._log_prefix = log_prefix

    def _log(self, m):
        print "%s_BurstThread: %s" % (self._log_prefix, m)

    def run(self):
        self._log("burst thread running")
        time_next_burst = time.time()  # just now
        while self._running:
            if self._enabled:
                if time.time() >= time_next_burst:
                    values = self._generate_burst()
                    reply = "%s%s" % (" ".join(values), NEWLINE)
                    self._conn.sendall(reply)
                    time_next_burst = time.time() + time_between_bursts

            time.sleep(0.2)

        self._log("burst thread exiting")

    def set_enabled(self, enabled):
        self._enabled = enabled

    def end(self):
        self._enabled = False
        self._running = False

    def _generate_burst(self):
        values = []
        for i in range(12):
            r = random.random()
            values.append("%.3f" % r)
        return values


class BarsSimulator(object):

    def __init__(self, host='', port=0, accept_timeout=None,
                 log_prefix='\t\t\t\t| '):
        """
        @param host Socket is bound to given (host,port)
        @param port Socket is bound to given (host,port)
        @param accept_timeout Timeout for accepting a connection
        @param log_prefix a prefix for every log message
        """

        self._log_prefix = log_prefix
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._sock.listen(1)
        self._accept_timeout = accept_timeout
        self._log("bound to port %s" % self.get_port())

    def _log(self, m):
        print "%sBarsSimulator: %s" % (self._log_prefix, m)

    def get_port(self):
        return self._sock.getsockname()[1]

    def run(self):
        self._enabled = True
        if self._accept_timeout is not None:
            accept_time_limit = time.time() + self._accept_timeout
        self._sock.settimeout(0.5)
        self._log('waiting for connection')
        while True:
            try:
                self._conn, addr = self._sock.accept()
                break  # connected
            except socket.timeout:
                if self._accept_timeout is not None and \
                   time.time() > accept_time_limit:
                    self._enabled = False
                    self._log("accept timeout. Simulator stopping")
                    return

        self._log('Connected by %s' % str(addr))

        bt = _BurstThread(self._conn, self._log_prefix)
        bt.start()

        self._connected(bt)
        bt.end()

        self._conn.close()
        self._sock.close()

        self._log("bye.")

    def stop(self):
        """Requests that the simulator terminate"""
        self._enabled = False
        self._log("simulator requested to stop.")

    def _recv(self):
        """does the recv call with handling of timeout"""
        while self._enabled:
            try:
                input = self._conn.recv(1024)
                if input is not None:
                    input = input.strip()
                    self._log("recv: '%s'" % input)
                else:
                    self._log("recv: None")
                return input
            except socket.timeout:
                # ok, retry receiving
                continue

        return None

    def _connected(self, bt):
        # set an ad hoc timeout to regularly check whether termination has been
        # requested
        self._conn.settimeout(1.0)
        while self._enabled:
            input = self._recv()
            if not input or not self._enabled:
                break

            self._log("input received: '%s'" % input)
            if input == "q":
                break

            if input == "^S":
                bt.set_enabled(False)
                self._main_menu(bt)
            else:
                response = "invalid input: '%s'" % input
                self._log(response)
                self._conn.sendall(response + NEWLINE)

        self._log("exiting connected")

    def _main_menu(self, bt):

        def _get_input(received):
            """
            This takes care of translating the received data into one of the
            codes above.
            """
            if received is None:
                return None

            received = received.strip()
            if received == "0":
                return PRINT_DATE_TIME
            elif received == "1":
                return RESTART_DATA_COLLECTION
            elif received == "2":
                return ENTER_CHANGE_PARAMS
            elif received == "3":
                return SHOW_SYSTEM_DIAGNOSTICS
            elif received == "4":
                return ENTER_SET_SYSTEM_CLOCK
            elif received == "5":
                return SHOW_SYSTEM_INFO
            elif received == "6":
                return EXIT_PROGRAM
            else:
                return UNRECOGNIZED

        menu = """\
            Select one of the following functions:

                0). Reprint Time & this Menu.
                1). Restart Data Collection.
                2). Change Data Collection Parameteres.
                3). System Diagnostics.
                4). Set The System Clock.
                5). Provide Information on this System.
                6). Exit this Program.

            Select 0, 1, 2, 3, 4, 5 or 6 here  --> """

        while self._enabled:
            self._conn.sendall(menu)
            recv = self._recv()
            if not recv or not self._enabled:
                break

            input = _get_input(recv)
            if input == PRINT_DATE_TIME:
                self._print_date_time()

            elif input == RESTART_DATA_COLLECTION:
                self._restart_data_collection(bt)
                break  # and exit this menu

            elif input == ENTER_CHANGE_PARAMS:
                self._change_params_menu(bt)

            elif input == SHOW_SYSTEM_DIAGNOSTICS:
                self._diagnostics()

            elif input == ENTER_SET_SYSTEM_CLOCK:
                self._reset_clock()

            elif input == SHOW_SYSTEM_INFO:
                self._system_info()

            elif input == EXIT_PROGRAM:
                # exit program -- IGNORED
                pass

            else:
                # TODO unrecognized command handling
                self._conn.sendall("unrecognized command: '%s'%s" %
                                   (input, NEWLINE))

        self._log("exiting main menu")

    def _print_date_time(self):
        # TODO print date time
        self._conn.sendall("TODO date time" + NEWLINE)

    def _restart_data_collection(self, bt):
        """restarts data collection"""
        print "resuming data"
        bt.set_enabled(True)

    def _change_params_menu(self, bt):
        """to change parameters
        """
        #TODO not clear how this menu looks like or works

        def _get_input(received):
            """
            This takes care of translating the received data into one of the
            codes corresponding to this menu.
            """
            if received is None:
                return None

            received = received.strip()
            if received == "":
                return None
            # TODO ...
            else:
                return UNRECOGNIZED

        menu = """\
            TODO change_params_menu:

                0). ??
                1). ??

            Select 0, 1, ??? here  --> """

        while self._enabled:
            self._conn.sendall(menu)
            recv = self._recv()
            if not recv or not self._enabled:
                break

            input = _get_input(recv)
            if input == "Dummy":
                # TODO this is not an actual command
                pass

            else:
                # TODO unrecognized command handling
                self._conn.sendall("unrecognized command: '%s'%s" %
                                   (input, NEWLINE))

        print "exiting change params menu"

    def _diagnostics(self):
        # TODO diagnostics
        self._conn.sendall("TODO diagnostics" + NEWLINE)

    def _reset_clock(self):
        # TODO reset clock
        self._conn.sendall("TODO reset clock" + NEWLINE)

    def _system_info(self):
        # TODO system info
        self._conn.sendall("TODO system info" + NEWLINE)


if __name__ == '__main__':
    simulator = BarsSimulator()
    simulator.run()
