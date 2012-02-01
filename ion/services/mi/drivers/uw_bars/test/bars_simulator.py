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
        self._port = self._sock.getsockname()[1]
        self._log("bound to port %s" % self._port)

    def _log(self, m):
        print "%sBarsSimulator: %s" % (self._log_prefix, m)

    @property
    def port(self):
        return self._port

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

    def _clear_screen(self, info):
        clear_screen = NEWLINE * 50
        self._conn.sendall(clear_screen + info)

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
            if not self._enabled:
                break
            if not input:
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
                self._restart_data_collection(bt)
                break

            elif input == "2":
                self._change_params_menu(bt)

            elif input == "3":
                self._diagnostics()

            elif input == "4":
                self._reset_clock()

            elif input == "5":
                self._sensor_power_menu(bt)

            elif input == "6":
                self._system_info()

            elif input == "7":
                self._log("exit program -- IGNORED")
                pass

            else:
                # TODO unrecognized command handling
                self._conn.sendall("unrecognized command: '%s'%s" %
                                   (input, NEWLINE))

        self._log("exiting _main_menu")

    def _print_date_time(self):
        # TODO print date time
        self._conn.sendall("TODO date time" + NEWLINE)

    def _restart_data_collection(self, bt):
        """restarts data collection"""
        print "resuming data"
        bt.set_enabled(True)

    def _change_params_menu(self, bt):
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
                # TODO unrecognized command handling
                self._conn.sendall("unrecognized command: '%s'%s" %
                                   (input, NEWLINE))

        print "exiting _change_params_menu"

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
        info = info.replace('\n', NEWLINE)

        while self._enabled:
            self._clear_screen(info)

            input = self._recv()
            if not self._enabled:
                break
            if not input:
                break

            else:
                # TODO unrecognized command handling
                self._conn.sendall("unrecognized command: '%s'%s" %
                                   (input, NEWLINE))

        print "exiting _system_info"


    def _sensor_power_menu(self, bt):
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
                # TODO unrecognized command handling
                self._conn.sendall("unrecognized command: '%s'%s" %
                                   (input, NEWLINE))

        print "exiting _sensor_power_menu"


if __name__ == '__main__':
    simulator = BarsSimulator()
    simulator.run()
