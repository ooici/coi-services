#!/usr/bin/env python

"""
@package ion.services.mi.drivers.test.bars_simulator
@file ion/services/mi/drivers/test/bars_simulator.py
@author Carlos Rueda

@brief A partial simulator for the BARS instrument intended to facilitate
testing.
It accepts a TCP connection on a port and starts by sending out bursts of
random data every few seconds. By default it binds the service to an
automatically assigned port.
It accepts multiple clients but in sequential order.

"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

import socket
import random
import time

from threading import Thread


NEWLINE = '\r\n'
EOF = '\x04'

CONTROL_S = '\x13'

time_between_bursts = 2


def _escape(str):
    str = str.replace('\r', '\\r')
    str = str.replace('\n', '\\n')
    str = str.replace(EOF, '^D')
    s = ''
    for c in str:
        o = ord(c)
        if o < 32 or (127 <= o < 160):
            s += '\\%02d' % o
        else:
            s += c
    str = s
    return str


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
    """
    Dispatches multiple clients but sequentially. The special command
    'q' can be requested by a client to make the whole simulator terminate.
    """

    _next_simulator_no = 0

    def __init__(self, host='', port=0, accept_timeout=None,
                 log_prefix='\t\t\t\t|* '):
        """
        @param host Socket is bound to given (host,port)
        @param port Socket is bound to given (host,port)
        @param accept_timeout Timeout for accepting a connection
        @param log_prefix a prefix for every log message
        """

        BarsSimulator._next_simulator_no += 1
        self._simulator_no = BarsSimulator._next_simulator_no
        self._client_no = 0

        self._log_prefix = log_prefix
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._sock.listen(1)
        self._accept_timeout = accept_timeout
        self._port = self._sock.getsockname()[1]
        self._log("bound to port %s" % self._port)
        self._bt = None

        # helps to ignore \r coming right after normal commands, which would
        # have already been dispatched.
        self._pre_recv = ''

    def _log_client(self, m):
        print "%s[%d.%d] BarsSimulator: %s" % (self._log_prefix,
                                               self._simulator_no,
                                               self._client_no,
                                               m)

    def _log(self, m):
        print "%s[%d]BarsSimulator: %s" % (self._log_prefix,
                                           self._simulator_no,
                                           m)

    @property
    def port(self):
        return self._port

    def run(self):
        #
        # internal timeout for the accept. It allows to check regularly if
        # the simulator has been requested to terminate.
        #
        self._sock.settimeout(0.2)
        self._enabled = True
        explicit_quit = False
        while self._enabled and not explicit_quit:
            if self._accept_timeout is not None:
                accept_time_limit = time.time() + self._accept_timeout
            self._log('---waiting for connection---')
            while self._enabled:
                try:
                    self._conn, addr = self._sock.accept()
                    break  # connected
                except socket.timeout:
                    if self._accept_timeout is not None and \
                       time.time() > accept_time_limit:
                        # real timeout
                        self._log("accept timeout. Simulator stopping")
                        return

            if not self._enabled:
                break

            self._client_no += 1
            self._log_client('Connected by %s' % str(addr))

            self._bt = _BurstThread(self._conn, self._log_prefix)
            self._bt.start()

            explicit_quit = self._connected()
            self._bt.end()
            self._bt = None

            self._conn.close()

            self._log("bye.")
            time.sleep(1)

        self._sock.close()

    def stop(self):
        """Requests that the simulator terminate"""
        self._enabled = False
        self._log("simulator requested to stop.")

    def _clear_screen(self, info):
        clear_screen = NEWLINE * 20
        info = info.replace('\n', NEWLINE)
        self._conn.sendall(clear_screen + info)

    def _recv(self):
        """
        does the recv call with handling of timeout
        """

        while self._enabled:
            try:
                input = None

                recv = self._conn.recv(1)

                if recv is not None:
                    self._log_client("RECV: '%s'   _pre_recv = '%s'" % (
                        _escape(recv), _escape(self._pre_recv)))
                    if EOF == recv:
                        self._enabled = False
                        break
                    if '\r' == recv:
                        if self._pre_recv == '':
                            input = recv
                        else:
                            # ignore \r, self._pre_recv already notified
                            self._pre_recv = ''
                            self._log_client("\\r IGNORED.")
                            continue

                    elif len(recv.strip()) > 0:
                        input = recv.strip()
                        if input != CONTROL_S:
                            self._pre_recv += input
                            self._log_client("APPENDED _pre_recv = '%s'" % \
                                _escape(self._pre_recv))
                    else:
                        input = recv

                else:
                    self._log_client("RECV: None")
                    self._pre_recv = ''

                if input is not None:
                    self._log_client("input: '%s'" % _escape(input))
                    return input

            except socket.timeout:
                # ok, retry receiving
                continue
            except Exception as e:
                self._log_client("!!!!! %s " % str(e))
                break

        return None

    def _connected(self):
        """
        Dispatches the main initial state which is streaming data.

        @relval True if an explicit quit command ('q') was received;
        False otherwise.
        """

        #self._conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1)

        # set an ad hoc timeout to regularly check whether termination has been
        # requested
        self._conn.settimeout(1.0)
        while self._enabled:
            input = self._recv()
            if not self._enabled:
                break
            if not input:
                break

            if input == "q":
                self._log_client("exiting connected upon explicit quit request")
                return True  # explicit quit

            if input == CONTROL_S:
                self._bt.set_enabled(False)
                self._main_menu()
            else:
                response = "invalid input: '%s'" % _escape(input)
                self._log_client(response)

        self._log_client("exiting connected")
        return False

    def _main_menu(self):
        menu = """\
  ***************************************************************
  *                                                             *
  *            Welcome to the BARS Program Main Menu            *
  *              (Benthic And Resistivity Sensors)              *
  *                    (Serial Number 002)                      *
  *                                                             *
  ***************************************************************

             Version 1.7 - Last Revision: July 11, 2011

                           Written by:

                           Rex Johnson
                           Engineering Services
                           School of Oceanography
                           University of Washington
                           Seattle, WA 98195


              The System Clock has not been set.
                Use option 4 to Set the Clock.

              Select one of the following functions:

                  0).  Reprint Time & this Menu.
                  1).  Restart Data Collection.
                  2).  Change Data Collection Parameters.
                  3).  System Diagnostics.
                  4).  Set the System Clock.
                  5).  Control Power to Sensors.
                  6).  Provide Information on this System.
                  7).  Exit this Program.

                Enter 0, 1, 2, 3, 4, 5, 6 or 7 here  --> """

        while self._enabled:
            self._clear_screen(menu)

            input = self._recv()
            if not self._enabled:
                break
            if not input:
                break

            if input == "0":
                self._print_date_time()

            elif input == "1":
                self._restart_data_collection()
                break

            elif input == "2":
                self._change_params_menu()

            elif input == "3":
                self._diagnostics()

            elif input == "4":
                self._reset_clock()

            elif input == "5":
                self._sensor_power_menu()

            elif input == "6":
                self._system_info()

            elif input == "7":
                self._log_client("exit program -- IGNORED")
                pass

            else:
                # just continue
                continue

        self._log_client("exiting _main_menu so resuming data collection")

    def _print_date_time(self):
        # TODO print date time
        self._conn.sendall("TODO date time" + NEWLINE)

    def _restart_data_collection(self):
        """restarts data collection"""
        self._bt.set_enabled(True)

    def _change_params_menu(self):
        menu = """\
                               System Parameter Menu

*****************************************************************************

                       The present value for the Cycle Time is
                                 20 Seconds.

                  The present setting for Verbose versus Data only is
                                     Data Only.

*****************************************************************************


                Select one of the following functions:

                      0).  Reprint this Menu.
                      1).  Change the Cycle Time.
                      2).  Change the Verbose Setting.
                      3).  Return to the Main Menu.

                    Enter 0, 1, 2, or 3 here  --> """

        while self._enabled:
            self._clear_screen(menu)

            input = self._recv()
            if not self._enabled:
                break
            if not input:
                break

            if input == "0":
                pass

            elif input == "1":
                # TODO change cycle time
                break

            elif input == "2":
                # TODO change verbose setting
                break

            elif input == "3":
                break

            else:
                # just continue
                continue

        self._log_client("exiting _change_params_menu")

    def _diagnostics(self):
        # TODO diagnostics
        self._conn.sendall("TODO diagnostics" + NEWLINE)

    def _reset_clock(self):
        # TODO reset clock
        self._conn.sendall("TODO reset clock" + NEWLINE)

    def _system_info(self):
        info = """\
  System Name: BARS (Benthic And Resistivity Sensors)
  System Owner: Marv Lilley, University of Washington
  Owner Contact Phone #: 206-543-0859
  System Serial #: 002

  Press Enter to return to the Main Menu. --> """

        while self._enabled:
            self._clear_screen(info)

            input = self._recv()
            if not self._enabled:
                break
            if not input:
                break

            if input == "\r":
                break
            else:
                # just continue
                continue

        self._log_client("exiting _system_info")

    def _sensor_power_menu(self):
        menu = """\
                             Sensor Power Control Menu

*****************************************************************************

                 Here is the current status of power to each sensor

                      Res Sensor Power is ............. On
                      Instrumentation Amp Power is .... On
                      eH Isolation Amp Power is ....... On
                      Hydrogen Power .................. On
                      Reference Temperature Power ..... On

*****************************************************************************


            Select one of the following functions:

                  0).  Reprint this Menu.
                  1).  Toggle Power to Res Sensor.
                  2).  Toggle Power to the Instrumentation Amp.
                  3).  Toggle Power to the eH Isolation Amp.
                  4).  Toggle Power to the Hydrogen Sensor.
                  5).  Toggle Power to the Reference Temperature Sensor.
                  6).  Return to the Main Menu.

                Enter 0, 1, 2, 3, 4, 5, or 6 here  --> """

        while self._enabled:
            self._clear_screen(menu)

            input = self._recv()
            if not self._enabled:
                break
            if not input:
                break

            if input == "0":
                pass

            elif input == "1":
                # TODO Toggle Power to Res Sensor.
                break

            elif input == "2":
                # TODO Toggle Power to the Instrumentation Amp.
                break

            elif input == "3":
                # TODO Toggle Power to the eH Isolation Amp.
                break

            elif input == "4":
                # TODO Toggle Power to the Hydrogen Sensor.
                break

            elif input == "5":
                # TODO Toggle Power to the Reference Temperature Sensor.
                break

            elif input == "6":
                break

            else:
                # just continue
                continue

        self._log_client("exiting _sensor_power_menu")


if __name__ == '__main__':
    simulator = BarsSimulator()
    simulator.run()
