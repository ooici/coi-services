#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

"""
BarsClient allows direct iteraction with the instrument via a socket.
See main() for demo program.
"""

import sys
import socket
import os
import re
import time

from threading import Thread


EOF = '\x04'
CONTROL_S = '\x13'

DATA_LINE_PATTERN = re.compile(r'.*(\d+\.\d*\s*){12}.*')

GENERIC_PROMPT_PATTERN = re.compile(r'.*--> ')


class _Recv(Thread):
    """
    Thread to receive and optionaly write the data to a given file.
    """

    def __init__(self, conn, outfile=None):
        Thread.__init__(self, name="_Recv")
        self._conn = conn
        self._last_line = ''
        self._new_line = ''
        self._active = True
        self._outfile = outfile
        self.setDaemon(True)

    def _update_lines(self, recv):
        if recv == '\n':
            self._last_line = self._new_line
            self._new_line = ''
            return True
        else:
            self._new_line += recv
            return  False

    def end(self):
        self._active = False

    def run(self):
        print "### _Recv running."
        while self._active:
            recv = self._conn.recv(1)
            self._update_lines(recv)

            if self._outfile:
                os.write(self._outfile.fileno(), recv)
                self._outfile.flush()


class BarsClient(object):
    """
    Provides direct commnunication with the BARS instrument.
    """

    def __init__(self, host, port, outfile=None):
        """
        Establishes the connection and starts the receiving thread.
        """
        print "### connecting to %s:%s" % (host, port)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._bt = _Recv(self._sock, outfile)
        self._bt.start()

        self._delaybeforesend = 3

    def is_collecting_data(self, timeout=30):
        """
        Determines whether the instrument is currently collecting data.
        @param timeout Timeout for the check.
        """
        print "### is_collecting_data..."
        time_limit = time.time() + timeout
        got_data = False
        while not got_data and time.time() <= time_limit:
            time.sleep(1)
            str = self._bt._last_line
            got_data = DATA_LINE_PATTERN.match(str) is not None

        return got_data

    def enter_main_menu(self):
        self._automatic_control_s()

    def send_enter(self):
        time.sleep(self._delaybeforesend)
        print "### send_enter"
        self._send_control('m')

    def send_option(self, char):
        time.sleep(self._delaybeforesend)
        print "### send_option: '%s'" % char
        self._send(char)
        self._send_control('m')

    def end(self):
        self._bt.end()
        self._sock.close()

    def _automatic_control_s(self):
        """
        Sends ^S repeatily until getting a prompt.
        """
        print "### automatic ^S"
        got_prompt = False
        while not got_prompt:
            print "### sending ^S"
            self._send_control('s')
            time.sleep(2)
            str = self._bt._new_line
            got_prompt = GENERIC_PROMPT_PATTERN.match(str) is not None

        print "### got prompt."
        print "### sending one ^m to clean up any ^S leftover"
        self._send_control('m')

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
    Demo program.
    """
    bars_client = BarsClient(host, port, outfile)
    if bars_client.is_collecting_data():
        print "Instrument in in collecting data mode."
    else:
        print "Instrument in not in collecting data mode. Exiting."
        exit()

    bars_client.enter_main_menu()
    bars_client.send_enter()
    bars_client.send_option('6')
    bars_client.send_enter()
    bars_client.send_option('1')

    time.sleep(10)
    bars_client.end()

if __name__ == '__main__':
    usage = """USAGE:
    bars_client.py address port # connect to instrument on address:port
    bars_client.py port         # connect to assume simulator on localhost:port
    """
    if len(sys.argv) <= 1:
        print usage
        exit()

    if len(sys.argv) == 2:
        host = 'localhost'
        port = int(sys.argv[1])
    else:
        host = sys.argv[1]
        port = int(sys.argv[2])

    main(host, port)
