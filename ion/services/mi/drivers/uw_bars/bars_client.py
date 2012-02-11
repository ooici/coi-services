#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

"""
BarsClient allows direct iteraction with the instrument via a socket.
A demo program can be run as follows:
    $ bin/python ion/services/mi/drivers/uw_bars/bars_client.py \
           --host 10.180.80.172 --port 2001 --outfile output.txt
"""

import sys
import socket
import os
import re
import time
from threading import Thread

import ion.services.mi.mi_logger
import logging
log = logging.getLogger('mi_logger')


EOF = '\x04'
CONTROL_S = '\x13'

DATA_LINE_PATTERN = re.compile(r'.*(\d+\.\d*\s*){12}.*')

GENERIC_PROMPT_PATTERN = re.compile(r'.*--> ')

MAX_NUM_LINES = 100


class _Recv(Thread):
    """
    Thread to receive and optionaly write the received data to a given file.
    It keeps internal buffers to support the check for expected contents.
    """

    def __init__(self, conn, outfile=None):
        Thread.__init__(self, name="_Recv")
        self._conn = conn
        self._last_line = ''
        self._new_line = ''
        self._lines = []
        self._active = True
        self._outfile = outfile
        self.setDaemon(True)
        log.debug("### _Recv created.")

    def _update_lines(self, recv):
        if recv == '\n':
            self._last_line = self._new_line
            self._new_line = ''
            self._lines.append(self._last_line)
            if len(self._lines) > MAX_NUM_LINES:
                self._lines = self._lines[MAX_NUM_LINES - 1:]
            return True
        else:
            self._new_line += recv
            return  False

    def end(self):
        self._active = False

    def run(self):
        log.debug("### _Recv running.")
        while self._active:
            recv = self._conn.recv(1)
            self._update_lines(recv)

            if self._outfile:
                os.write(self._outfile.fileno(), recv)
                self._outfile.flush()
        log.debug("### _Recv.run done.")


class BarsClient(object):
    """
    TCP based client for communication with the BARS instrument.
    """

    def __init__(self, host, port, outfile=None):
        """
        Establishes the connection and starts the receiving thread.
        """
        self._host = host
        self._port = port
        self._sock = None
        self._outfile = outfile
        self._bt = None

        """sleep time used just before sending data"""
        self.delay_before_send = 0.2

        """sleep time used just before a expect operation"""
        self.delay_before_expect = 2

    def connect(self):
        host, port = self._host, self._port
        log.debug("### connecting to %s:%s" % (host, port))
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        log.debug("### connected to %s:%s" % (host, port))
        self._bt = _Recv(self._sock, self._outfile)
        self._bt.start()
        log.debug("### _Recv started.")

    def is_collecting_data(self, timeout=30):
        """
        Determines whether the instrument is currently collecting data.
        @param timeout Timeout for the check.
        """
        log.debug("### is_collecting_data? ...")
        time_limit = time.time() + timeout
        got_data = False
        while not got_data and time.time() <= time_limit:
            time.sleep(1)
            string = self._bt._last_line
            got_data = DATA_LINE_PATTERN.match(string) is not None

        return got_data

    def enter_main_menu(self):
        """
        Sends ^S repeatily until getting a prompt.
        It does the check every 2 seconds.
        """
        log.debug("### automatic ^S")
        got_prompt = False
        while not got_prompt:
            log.debug("### sending ^S")
            self._send_control('s')
            time.sleep(2)
            string = self._bt._new_line
            got_prompt = GENERIC_PROMPT_PATTERN.match(string) is not None

        log.debug("### got prompt. Sending one ^m to clean up any ^S leftover")
        self._send_control('m')

    def get_last_buffer(self):
        return '\n'.join(self._bt._lines)

    def send_enter(self):
        """
        Sleeps for self.delay_before_send and calls self._send_control('m').
        """
        time.sleep(self.delay_before_send)
        log.debug("### send_enter")
        self._send_control('m')

    def send_option(self, char):
        """
        Sleeps for self.delay_before_send and then sends the given char
        followed by a ^m
        """
        time.sleep(self.delay_before_send)
        log.debug("### send_option: '%s'" % char)
        self._send(char)
        self._send_control('m')

    def expect_line(self, pattern, pre_delay=None, timeout=30):
        """
        Sleeps for the given pre_delay seconds (self.delay_before_expect by
        default).
        Then waits until the given pattern matches the current contents of the
        last received line. It does the check every second.

        @pattern The pattern (a string or the result of a re.compile)
        @param pre_delay For the initial sleep (self.delay_before_expect by
                default).
        @param timeout Timeout for the wait.
        """
        pre_delay = pre_delay if pre_delay else self.delay_before_expect
        time.sleep(pre_delay)

        patt_str = pattern if isinstance(pattern, str) else pattern.pattern
        log.debug("### expecting '%s'" % patt_str)
        time_limit = time.time() + timeout
        got_it = False
        while not got_it and time.time() <= time_limit:
            time.sleep(1)
            string = self._bt._new_line
            got_it = re.match(pattern, string) is not None

        return got_it

    def expect_generic_prompt(self, pre_delay=None, timeout=30):
        """
        returns self.expect_line(GENERIC_PROMPT_PATTERN, pre_delay, timeout)
        where pre_delay is self.delay_before_expect by default.

        Note that the pre_delay is intended to allow the instrument to send new
        info after a preceeding requested option so as to avoid a false
        positive prompt verification.

        @param pre_delay For the initial sleep (self.delay_before_expect by
                default).
        @param timeout Timeout for the wait.
        """
        pre_delay = pre_delay if pre_delay else self.delay_before_expect
        return self.expect_line(GENERIC_PROMPT_PATTERN, pre_delay, timeout)

    def end(self):
        """
        Ends the client.
        """
        log.debug("### ending")
        self._bt.end()
        self._sock.close()

    def _send(self, s):
        """
        Sends a string. Returns the number of bytes written.
        """
        c = os.write(self._sock.fileno(), s)
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


def main(host, port, outfile=sys.stdout):
    """
    Demo program:
    - checks if the instrument is collecting data
    - if not, this function returns False
    - breaks data streaming to enter main menu
    - selects 6 to get system info
    - sends enter to return to main menu
    - resumes data streaming
    - sleeps a few seconds to let some data to come in
    - returns True

    @param host Host of the instrument
    @param port Port of the instrument
    @param outfile File object used to echo all received data from the socket
                   (by default, sys.stdout).
    """
    bars_client = BarsClient(host, port, outfile)
    bars_client.connect()

    print ":: is instrument collecting data?"
    if bars_client.is_collecting_data():
        print ":: Instrument is collecting data."
    else:
        print ":: Instrument in not in collecting data mode. Exiting."
        return False

    print ":: break data streaming to enter main menu"
    bars_client.enter_main_menu()

    print ":: select 6 to get system info"
    bars_client.send_option('6')
    bars_client.expect_generic_prompt()

    print ":: send enter to return to main menu"
    bars_client.send_enter()
    bars_client.expect_generic_prompt()

    print ":: resume data streaming"
    bars_client.send_option('1')

    print ":: sleeping for 10 secs to receive some data"
    time.sleep(10)

    print ":: bye"
    bars_client.end()
    return True


if __name__ == '__main__':
    usage = """USAGE: bars_client.py [options]
       --host address      # instrument address (localhost by default)
       --port port         # instrument port (required)
       --outfile filename  # file to save all received data (stdout by default)
       --loglevel level    # used to eval mi_logger.setLevel(logging.%s)
    """
    usage += """\nExample: bars_client.py --host 10.180.80.172 --port 2001"""

    host = 'localhost'
    port = None
    outfile = sys.stdout

    arg = 1
    while arg < len(sys.argv):
        if sys.argv[arg] == "--host":
            arg += 1
            host = sys.argv[arg]
        elif sys.argv[arg] == "--port":
            arg += 1
            port = int(sys.argv[arg])
        elif sys.argv[arg] == "--outfile":
            arg += 1
            outfile = file(sys.argv[arg], 'w')
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
        main(host, port, outfile)
