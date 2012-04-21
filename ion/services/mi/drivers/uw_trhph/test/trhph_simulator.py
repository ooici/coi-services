#!/usr/bin/env python

"""
@package ion.services.mi.drivers.uw_trhph.test.trhph_simulator
@file ion/services/mi/drivers/uw_trhph/test/trhph_simulator.py
@author Carlos Rueda

@brief A TRHPH instrument simulator.
See https://confluence.oceanobservatories.org/display/CIDev/UW+TRHPH+instrument+simulator
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

import ion.services.mi.drivers.uw_trhph.trhph as trhph

import socket
import random
import time
import sys
import os
import signal

from threading import Thread, Lock
import logging

log = logging.getLogger('mi_logger')


NEWLINE = trhph.NEWLINE
EOF = '\x04'

CONTROL_S = '\x13'

INITIAL_POWER_STATUSES = [True, True, True, True, True]


class _Sender(object):
    """
    Allows synchronized sends to the client so there are no intermixed
    messages being replied.
    """
    def __init__(self, conn):
        self._conn = conn
        self._lock = Lock()

    def send(self, m):
        """Does an atomic send to the client."""
        self._lock.acquire()
        try:
            self._conn.sendall(m)
        finally:
            self._lock.release()


class _BurstThread(Thread):
    """Thread to generate data bursts"""

    # TODO handle verbose mode; currently the "data-only" mode is always
    # dispatched regardless of the value of self._data_only

    def __init__(self, sender, client_prefix, time_between_bursts,
                data_only):
        """
        Creates a new object.
        @param sender _Sender object to use for sending messages.
        @param client_prefix Prefix for logging.
        @param data_only True for "Data only"; False for "verbose"
               TODO NOTE this is currently effectively IGNORED.
        @param time_between_bursts time in seconds.
        """

        Thread.__init__(self, name="BurstThread")
        self._sender = sender
        self._client_prefix = client_prefix
        self._data_only = data_only
        self._time_between_bursts = time_between_bursts
        self._running = True
        self._enabled = True
        self._values = None  # updated by _generate_burst

    def set_time_between_bursts(self, secs):
        """
        @param secs time in seconds
        """
        self._time_between_bursts = secs
        log.info(self._client_prefix + "set time_between_bursts = %d secs" %
                  self._time_between_bursts)

    def set_data_only(self, data_only):
        """
        @param data_only True for "Data only"; False for "verbose"
        """
        self._data_only = data_only
        log.info(self._client_prefix + "set data_only = %s" % self._data_only)

    def run(self):
        info = "burst thread running"
        info += ", time_between_bursts = %d secs" % self._time_between_bursts
        info += ", data_only = %s" % self._data_only
        log.info(self._client_prefix + info)
        time_next_burst = time.time()  # just now
        while self._running:
            if self._enabled:
                if time.time() >= time_next_burst:
                    self._generate_burst()
                    self._output_burst()
                    time_next_burst = time.time() + self._time_between_bursts

            time.sleep(0.2)

        log.info(self._client_prefix + "burst thread exiting")

    def set_enabled(self, enabled):
        log.info(self._client_prefix + "set_enabled: %s" % str(enabled))
        self._enabled = enabled

    def end(self):
        log.info(self._client_prefix + "end")
        self._enabled = False
        self._running = False

    def _generate_burst(self):
        """sets _values with random values"""
        self._values = []
        for i in range(12):
            r = random.random()
            self._values.append("%.3f" % r)

    def _output_burst(self):
        """sends the current _values"""
        line = " ".join(self._values)
        try:
            self._sender.send(line + NEWLINE)
            log.info(self._client_prefix + "data burst sent: %s" % line)
        except socket.error, e:
                # some connection error with client. Just log the error
                log.info("A socket error was raised " +
                         "while trying to send a data burst: %s" % e)


class TrhphSimulator(Thread):
    """
    Can dispatch multiple clients, not concurrently but sequentially.
    The special command 'q' can be requested by a client to make the whole
    simulator terminate.
    """

    _next_simulator_no = 0

    def __init__(self, host='', port=0, accept_timeout=None):
        """
        @param host Socket is bound to given (host,port)
        @param port Socket is bound to given (host,port)
        @param accept_timeout Timeout for accepting a connection
        """
        Thread.__init__(self, name="TrhphSimulator")

        TrhphSimulator._next_simulator_no += 1
        self._simulator_no = TrhphSimulator._next_simulator_no
        self._client_no = 0

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._sock.listen(1)
        self._accept_timeout = accept_timeout
        self._port = self._sock.getsockname()[1]
        log.info(self._sim_prefix() + "bound to port %s (pid=%s)" % (
                self._port, os.getpid()))
        self._bt = None

        self._cycle_time = "15"
        self._cycle_time_units = "Seconds"

        self._data_only = True

        self._power_statuses = INITIAL_POWER_STATUSES

    def _client_prefix(self):
        """prefix for log messages associated to the simulator and client"""
        return "[%d.%d] " % (self._simulator_no, self._client_no)

    def _sim_prefix(self):
        """prefix for log messages associated to the simulator"""
        return "[%d] " % self._simulator_no

    @property
    def port(self):
        return self._port

    def run(self):
        try:
            self._run()
        finally:
            self._end_burst_thread()

    def _run(self):
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
            log.info(self._sim_prefix() + '==== waiting for connection ====')
            while self._enabled:
                try:
                    self._conn, addr = self._sock.accept()
                    break  # connected
                except socket.timeout:
                    if self._accept_timeout is not None and \
                       time.time() > accept_time_limit:
                        # real timeout
                        log.info(self._sim_prefix() +
                                 "accept timeout. Simulator stopping")
                        return

            if not self._enabled:
                break

            self._client_no += 1
            log.info(self._client_prefix() + 'Connected by %s' % str(addr))

            self._sender = _Sender(self._conn)
            self._bt = _BurstThread(self._sender, self._client_prefix(),
                                    self._get_time_between_bursts(),
                                    self._data_only)
            self._bt.start()

            try:
                explicit_quit = self._streaming()
                self._conn.close()
            except socket.error, e:
                # some connection error with this client. Just log the error
                # and continue with next client.
                log.info(self._client_prefix() + "A socket error was raised" +
                    " while interacting with client: %s" % e)
            finally:
                self._end_burst_thread()

            log.info(self._sim_prefix() + "bye.")
            time.sleep(1)

        self._sock.close()

    def _get_time_between_bursts(self):
        """Gets the time between bursts in seconds"""
        value = int(self._cycle_time)
        return value if self._cycle_time_units == "Seconds" else 60 * value

    def _end_burst_thread(self):
        log.info(self._client_prefix() + 'end burst thread %s' % str(self._bt))
        self._bt, bt = None, self._bt
        if bt is not None:
            bt.end()
            bt.join()

    def stop(self):
        """Requests that the simulator terminate"""
        self._enabled = False
        log.info(self._sim_prefix() + "simulator requested to stop.")

    def _clear_screen(self, info):
        clear_screen = NEWLINE * 20
        self._sender.send(clear_screen + info)

    def _recv(self):
        """
        does the recv call with handling of timeout
        """

        while self._enabled:
            try:
                input = None

                recv = self._conn.recv(4096)

                if recv is not None:
                    log.debug(self._client_prefix() + "RECV: '%s'" %
                                                     repr(recv))
                    if EOF == recv:
                        self._enabled = False
                        break

                    if '\r' == recv:
                        input = recv

                    elif len(recv.strip()) > 0:
                        input = recv.strip()
                    else:
                        input = recv

                else:
                    log.info(self._client_prefix() + "RECV: None")

                if input is not None:
                    # echo the input:
                    if input == '\r' or input == NEWLINE:
                        self._sender.send(NEWLINE)
                    else:
                        self._sender.send(input + NEWLINE)

                    return input

            except socket.timeout:
                # ok, retry receiving
                continue
            except Exception as e:
                log.warn(self._client_prefix() + "!!!!! %s " % str(e))
                break

        return None

    def _streaming(self):
        """
        Dispatches the main initial state which is streaming data.

        @retval True if an explicit quit command ('q') was received;
        False otherwise.
        """

        # set an ad hoc timeout to regularly check whether termination has been
        # requested
        self._conn.settimeout(1.0)
        while self._enabled:
            input = self._recv()
            log.info(self._client_prefix() + "INPUT=%s" % repr(input))
            if not self._enabled:
                break
            if not input:
                break

            if input == "q":
                log.info(self._client_prefix() +
                         "exiting _streaming, explicit quit request")
                return True  # explicit quit

            if input == CONTROL_S:
                self._bt.set_enabled(False)
                self._main_menu()
                if self._enabled:
                    self._restart_data_collection()

            else:
                # Everything else is just ignored. Just log a warning.
                log.warn(self._client_prefix() + "%s UNEXPECTED" % repr(input))

        log.info(self._client_prefix() + "exiting _streaming")
        return False

    def _main_menu(self):
        """
        Dispatches the main menu.
        """
        while self._enabled:
            menu = trhph.MAIN_MENU

            self._clear_screen(menu)

            input = self._recv()

            log.info(self._client_prefix() + "INPUT=%s" % repr(input))
            if not self._enabled:
                break
            if not input:
                break

            if input == "0":
                # reprint menu
                continue

            elif input == "1":
                # Restart Data Collection
                break

            elif input == "2":
                self._change_params_menu()

            elif input == "3":
                self._diagnostics()

            elif input == "4":
                self._sensor_power_menu()

            elif input == "5":
                self._system_info()

            elif input == "6":
                log.info(self._client_prefix() + "exit program -- IGNORED")
                continue

            elif input == '\r' or input == NEWLINE:
                # ok, but just continue
                continue
            else:
                # unexpected but continue
                log.warn(self._client_prefix() + "%s UNEXPECTED" % repr(input))
                continue

        log.info(self._client_prefix() + "exiting _main_menu")

    def _restart_data_collection(self):
        """restarts data collection"""
        log.info(self._client_prefix() + "resuming data collection")
        self._bt.set_enabled(True)

    def _change_params_menu(self):
        """
        Dispatches the System Parameter menu.
        """
        while self._enabled:
            #
            # TODO The print metadata flags were introduced in the TRHPH
            # program -- not yet handled, they are fixed for now.
            #
            menu = trhph.SYSTEM_PARAMETER_MENU_FORMAT % (
                self._cycle_time, self._cycle_time_units,
                trhph.DATA_ONLY if self._data_only else trhph.VERBOSE,
                trhph.PRINT_METADATA_POWER_UP_ENABLED,
                trhph.PRINT_METADATA_RESTART_ENABLED)

            self._clear_screen(menu)

            input = self._recv()

            log.info(self._client_prefix() + "INPUT=%s" % repr(input))
            if not self._enabled:
                break
            if not input:
                break

            if input == "0":
                # reprint menu
                continue

            elif input == "1":
                self._change_cycle_time()
                continue

            elif input == "2":
                self._change_data_only_setting()
                continue

            elif input == "9":
                # return to main menu
                break

            elif input == '\r' or input == NEWLINE:
                # ok, but just continue
                continue
            else:
                # unexpected but continue
                log.warn(self._client_prefix() + "%s UNEXPECTED" % repr(input))
                continue

        log.info(self._client_prefix() + "exiting _change_params_menu")

    def _change_cycle_time(self):
        """
        Allows to change the time between data bursts.
        """
        menu = trhph.CHANGE_CYCLE_TIME

        while self._enabled:
            self._clear_screen(menu)

            input = self._recv()

            log.info(self._client_prefix() + "INPUT=%s" % repr(input))
            if not self._enabled:
                break
            if not input:
                break

            if input in ["1", "2"]:
                value = None
                while value is None:
                    if input == "1":  # enter seconds
                        self._sender.send(trhph.CHANGE_CYCLE_TIME_IN_SECONDS)
                        units = "Seconds"
                    else:  # enter minutes
                        self._sender.send(trhph.CHANGE_CYCLE_TIME_IN_MINUTES)
                        units = "Minutes"

                    input_value = self._recv()

                    if input == '\r' or input == NEWLINE:
                        continue
                    try:
                        try_value = int(input_value)
                    except:
                        continue
                    if units == "Seconds" and 15 <= try_value <= 59:
                        value = try_value
                    elif units == "Minutes" and 1 <= try_value:
                        value = try_value

                self._cycle_time = value
                self._cycle_time_units = units
                log.info(self._client_prefix() + "cycle_time set to: %s %s" %
                    (self._cycle_time, self._cycle_time_units))

                self._bt.set_time_between_bursts(
                        self._get_time_between_bursts())
                break

            elif input == '\r' or input == NEWLINE:
                # ok, it's a "refresh" command; just continue
                continue
            else:
                # unexpected but continue
                log.warn(self._client_prefix() + "%s UNEXPECTED" % repr(input))
                continue

        log.info(self._client_prefix() + "exiting _change_cycle_time")

    def _change_data_only_setting(self):
        """
        Allows to change the verbose mode.
        """
        menu = trhph.CHANGE_VERBOSE_MODE

        while self._enabled:
            self._clear_screen(menu)

            input = self._recv()

            log.info(self._client_prefix() + "INPUT=%s" % repr(input))
            if not self._enabled:
                break
            if not input:
                break

            if input == "1":
                self._data_only = True
                break

            elif input == "2":  # verbose mode
                self._data_only = False
                break

            #
            # NOTE: In the following cases, the actual instrument prints
            # something like:
            #    The value you have entered is not within valid limits!
            #                     Please Try Again!
            #                Press ENTER to Continue.
            #
            # Here we just immediately continue (ie. reprint menu) which is
            # simpler and consistent with other prompts.
            # TODO Request UW developers to make this behavior simpler and
            # consistent with other similar prompts.
            #
            elif input == '\r' or input == NEWLINE:
                # A "refresh" so continue
                continue

            else:
                # just continue for any other input
                log.warn(self._client_prefix() + "%s UNEXPECTED" % repr(input))
                continue

        log.info(self._client_prefix() + "exiting _change_data_only_setting")

    def _diagnostics(self):
        """
        Allows to generate diagnostics.
        """
        menu = trhph.HOW_MANY_SCANS_FOR_SYSTEM_DIAGNOSTICS

        while self._enabled:
            self._clear_screen(menu)

            input = self._recv()

            log.info(self._client_prefix() + "INPUT=%s" % repr(input))
            if not self._enabled:
                break
            if not input:
                break

            if input == '\r' or input == NEWLINE:
                # Note: this is interpreted as zero in the TRHPH program.
                value = 0
            else:
                try:
                    value = int(input)
                except:
                    log.warn(self._client_prefix() +
                             "%s: not a number" % input)
                    continue

            # NOTE: no control on upper limit, which does probably exist in the
            # TRHPH program, but the current prompt does not tell whether such
            # limit exists.
            if 1 <= value:
                self._generate_diagnostics(value)
                break

            else:
                # unexpected but continue
                log.warn(self._client_prefix() + "%s UNEXPECTED" % repr(input))
                continue

        log.info(self._client_prefix() + "exiting _diagnostics")

    def _generate_diagnostics(self, num_scans):
        """
        Generates diagnostic data.
        """
        self._sender.send(trhph.SYSTEM_DIAGNOSTICS_HEADER + NEWLINE)
        for i in range(num_scans):
            values = []
            for j in range(10):
                r = random.random()
                values.append("%.3f" % r)
            line = "  ".join(values)
            self._sender.send(line + NEWLINE)
        self._sender.send(trhph.SYSTEM_DIAGNOSTICS_RETURN_PROMPT)

        # wait for any input to return
        input = self._recv()
        log.info(self._client_prefix() + "INPUT=%s" % repr(input))

    def _system_info(self):
        """
        Displays the system info.
        """
        info = trhph.SYSTEM_INFO

        while self._enabled:
            self._clear_screen(info)

            input = self._recv()

            log.info(self._client_prefix() + "INPUT=%s" % repr(input))
            if not self._enabled:
                break
            if not input:
                break

            if input == '\r' or input == NEWLINE:
                break

            else:
                # unexpected but continue
                log.warn(self._client_prefix() + "%s UNEXPECTED" % repr(input))
                continue

        log.info(self._client_prefix() + "exiting _system_info")

    def _sensor_power_menu(self):
        """
        Dispatches the Sensor Power Control menu.
        """
        while self._enabled:
            statuses = [('On' if v else 'Off') for v in self._power_statuses]
            menu = trhph.SENSOR_POWER_CONTROL_MENU_FORMAT % \
                tuple(statuses)

            self._clear_screen(menu)

            input = self._recv()

            log.info(self._client_prefix() + "INPUT=%s" % repr(input))
            if not self._enabled:
                break
            if not input:
                break

            if input == "0":
                # reprint menu
                continue

            elif input in ["1", "2", "3", "4", "5"]:
                # toggle corresponding entry
                index = int(input) - 1
                self._power_statuses[index] = not self._power_statuses[index]
                continue

            elif input == "9":
                # return to main menu
                break

            elif input == '\r' or input == NEWLINE:
                # ok, but just continue (reprint menu)
                continue
            else:
                # unexpected but continue
                log.warn(self._client_prefix() + "%s UNEXPECTED" % repr(input))
                continue

        log.info(self._client_prefix() + "exiting _sensor_power_menu")


def _set_logger(filename):
    """
    Sets the logger according to the given filename.
    @param filename Name of desired out log file, which will be always
           recreated. Can be None meaning that a logging.StreamHandler() is
           used instead of logging.FileHandler(filename).
    """
    log.setLevel(logging.INFO)
    if filename:
        if os.path.exists(filename):
            # recreate file, which is in particular important so our caller
            # (in case of external process) can scan our output more easily
            # for needed information, in particular the bound port.
            f = file(filename, 'w')
            f.close()
        handler = logging.FileHandler(filename=filename)
    else:
        handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(levelname)-6s %(threadName)-12s %(module)-20s %(funcName)-25s ' +
        '- %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)

if __name__ == '__main__':
    usage = """USAGE: trhph_simulator.py [options]
       --port port             # port to bind to (0 by default)
       --accept_timeout time   # accept timeout (no timeout by default)
       --logfile filename      # log file (stdout by default)
       --loglevel level        # used to eval mi_logger.setLevel(logging.%s)

    Output will show actual port used.
    """

    accept_timeout = None
    port = 0
    loglevel = None
    logfilename = None

    show_usage = False
    arg = 1
    while arg < len(sys.argv):
        if sys.argv[arg] == "--accept_timeout":
            arg += 1
            accept_timeout = int(sys.argv[arg])
        elif sys.argv[arg] == "--port":
            arg += 1
            port = int(sys.argv[arg])
        elif sys.argv[arg] == "--logfile":
            arg += 1
            logfilename = sys.argv[arg]
        elif sys.argv[arg] == "--loglevel":
            arg += 1
            loglevel = sys.argv[arg].upper()
        elif sys.argv[arg] == "--help":
            show_usage = True
            break
        else:
            sys.stderr.write("error: unrecognized option %s\n" % sys.argv[arg])
            sys.stderr.flush()
            show_usage = True
            break
        arg += 1

    if show_usage:
        print usage
    else:
        _set_logger(logfilename)
        if loglevel:
            eval("log.setLevel(logging.%s)" % loglevel)

        def handler(signum, frame):
            log.info("==== SIGTERM received, exiting normally ====\n\n")
            os._exit(0)
        signal.signal(signal.SIGTERM, handler)

        simulator = TrhphSimulator(port=port,
                                   accept_timeout=accept_timeout)
        simulator.run()
