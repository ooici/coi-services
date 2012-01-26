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

# change menu commands:
# TODO Not info yet available about this menu
# ...


time_between_bursts = 2


class _BurstThread(Thread):
    """Thread to generate data bursts"""

    def __init__(self, conn):
        Thread.__init__(self, name="_BurstThread")
        self._conn = conn
        self._running = True
        self._enabled = True

    def run(self):
        time_next_burst = time.time()  # just now
        while self._running:
            if self._enabled:
                if time.time() >= time_next_burst:
                    values = self._generate_burst()
                    reply = "%s%s" % (" ".join(values), NEWLINE)
                    self._conn.sendall(reply)
                    time_next_burst = time.time() + time_between_bursts

            time.sleep(0.2)

        print "burst thread exiting"

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


def _print_date_time(conn):
    # TODO print date time
    conn.sendall("TODO date time" + NEWLINE)


def _restart_data_collection(bt):
    """restarts data collection"""
    print "resuming data"
    bt.set_enabled(True)


def _change_params_menu(bt, conn):
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

    while True:
        conn.sendall(menu)

        input = _get_input(conn.recv(1024))
        if not input:
            break

        if input == "Dummy":
            # TODO this is not an actual command 
            pass

        else:
            # TODO unrecognized command handling
            conn.sendall("unrecognized command: '%s'%s" % (input, NEWLINE))

    print "exiting change params menu"


def _diagnostics(conn):
    # TODO diagnostics
    conn.sendall("TODO diagnostics" + NEWLINE)


def _reset_clock(conn):
    # TODO reset clock
    conn.sendall("TODO reset clock" + NEWLINE)


def _system_info(conn):
    # TODO system info
    conn.sendall("TODO system info" + NEWLINE)


def _main_menu(bt, conn):

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

    while True:
        conn.sendall(menu)
        input = _get_input(conn.recv(1024))
        if not input:
            break

        if input == PRINT_DATE_TIME:
            _print_date_time(conn)

        elif input == RESTART_DATA_COLLECTION:
            _restart_data_collection(bt)
            break  # and exit this menu

        elif input == ENTER_CHANGE_PARAMS:
            _change_params_menu(bt, conn)

        elif input == SHOW_SYSTEM_DIAGNOSTICS:
            _diagnostics(conn)

        elif input == ENTER_SET_SYSTEM_CLOCK:
            _reset_clock(conn)

        elif input == SHOW_SYSTEM_INFO:
            _system_info(conn)

        elif input == EXIT_PROGRAM:
            # exit program -- IGNORED
            pass

        else:
            # TODO unrecognized command handling
            conn.sendall("unrecognized command: '%s'%s" % (input, NEWLINE))

    print "exiting main menu"


def _connected(bt, conn):
    while True:
        input = conn.recv(1024)
        if not input:
            break

        input = input.strip()
        if input == "q":
            break

        if input == "^S":
            bt.set_enabled(False)
            _main_menu(bt, conn)
        else:
            response = "invalid input: '%s'" % input
            print response
            conn.sendall(response + NEWLINE)

    print "exiting connected"


class BarsSimulator(object):

    def __init__(self, host='', port=0):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._sock.listen(1)

    def get_port(self):
        return self._sock.getsockname()[1]

    def run(self):
        print 'waiting for connection'
        conn, addr = self._sock.accept()
        print 'Connected by', addr

        bt = _BurstThread(conn)
        bt.start()

        _connected(bt, conn)
        bt.end()

        conn.close()
        self._sock.close()

        print "bye."


if __name__ == '__main__':
    simulator = BarsSimulator()
    print "bound to port %s" % simulator.get_port()
    simulator.run()
