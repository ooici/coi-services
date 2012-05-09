#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

"""
This program allows direct user iteraction with the instrument via a socket.
It serves as a basis to determine and trace the actual messages in both
directions to implement the driver and diagnose any potential problem at the
communication level.

Usage example:
 bin/python ion/services/mi/drivers/uw_trhph/test/direct.py 10.180.80.172 2001

It establishes a TCP connection with the provided service, starts a thread to
print all incoming data from the associated socket, and goes into a loop to
dispach commands from the user.

The commands are:
    - a string with the two literal characters ^S --> sends a ^s
    - The character '!' --> does the multiple ^S requests
    - an empty string --> sends a ^m
    - The letter 'q' --> quits the program
    - Any other non-empty string --> sends the string followed by a ^m

Note, at the beginning of the interaction (assuming the instrument is in data
streaming mode) a number of ^S commands must typically be attempted to get to
the main menu. This same behavior is also seen when using telnet (both
directly or via pexpect.spawn.interact). Then, interaction is straightforward,
that is, no need to re-attempt any of the commands except for the ^S case.
The special command '!' makes the program do the multiple ^S attempts.
"""

import sys
import socket
import os
import re
import time

from threading import Thread


EOF = '\x04'
CONTROL_S = '\x13'


class _Recv(Thread):
    """
    Thread to receive and print data. Each line is prefixed
    with something like '\n\t| '. For human readability, some interesting
    characters are printed as follows: \r, \n, ^S, ^D, while other special
    ones are printed with the format ('\\%03d' % o) where o is the ord
    of the received character.
    """

    def __init__(self, conn):
        Thread.__init__(self, name="_Recv")
        self._conn = conn
        self._last_line = ''
        self._new_line = ''
        self.setDaemon(True)

    def _update_lines(self, recv):
        if recv == '\n':
            self._last_line = self._new_line
            self._new_line = ''
            return True
        else:
            self._new_line += recv
            return  False

    PREFIX = "\n\t| "

    def _prefix(self, str):
        str = str.replace('\r', '\\r')
        str = str.replace('\x13', '^S')
        str = str.replace('\x04', '^D')
        s = ''
        for c in str:
            if c in ['\r', '\n', CONTROL_S, EOF]:
                # this is processed outside of this section
                s += c
            else:
                o = ord(c)
                if o < 32 or (127 <= o < 256):
                    s += '\\%03d' % o
                else:
                    s += c
        str = s
        return str.replace('\n', '\\n' + _Recv.PREFIX)

    def run(self):
        print "### _Recv running."
        os.write(sys.stdout.fileno(), _Recv.PREFIX)
        sys.stdout.flush()
        while True:
            recv = self._conn.recv(1)
            newline = self._update_lines(recv)
            prefixed = self._prefix(recv)
            os.write(sys.stdout.fileno(), prefixed)
            sys.stdout.flush()
#            if newline:
#                str = self._last_line
#                str = str.replace('\r', '\\r')
#                str = str.replace('\n', '\\n')
#                print "### LINE: '%s'" % str


class _Direct(object):
    """
    Main program.
    """

    def __init__(self, host, port):
        """
        Establishes the connection and starts the receiving thread.
        """
        print "### connecting to %s:%s" % (host, port)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._bt = _Recv(self._sock)
        self._bt.start()

    def run(self):
        """
        Dispaches user commands.
        """
        while True:
            cmd = sys.stdin.readline()

            cmd = cmd.strip()
            if cmd == "^S":
                print "### sending '%s'" % cmd
                self.send_control('s')
            elif cmd == "q":
                print "### breaking"
                break
            elif cmd == "":
                print "### sending '^m'"
                self.send_control('m')
            elif cmd == "!":
                self._automatic_control_s()
            else:
                print "### sending '%s' + ^m" % cmd
                self.send(cmd)
                self.send_control('m')

        self.stop()

    def _automatic_control_s(self):
        """
        Sends ^S repeatily until we get the main menu.
        """
        print "### automatic ^S"
        got_prompt = False
        while not got_prompt:
            print "### sending ^S"
            self.send_control('s')
            time.sleep(2)
            str = self._bt._new_line
            got_prompt = re.match('.*--> ', str) is not None

        print "### got prompt."
        print "### sending one ^m to try to clean up any ^S leftover"
        self.send_control('m')

    def stop(self):
        self._sock.close()

    def send(self, s):
        """
        Sends a string. Returns the number of bytes written.
        """
        c = os.write(self._sock.fileno(), s)
        return c

    def send_control(self, char):
        """
        Sends a control character.
        @param char must satisfy 'a' <= char.lower() <= 'z'
        """
        char = char.lower()
        assert 'a' <= char <= 'z'
        a = ord(char)
        a = a - ord('a') + 1
        return self.send(chr(a))

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print """USAGE:
        direct.py address port  # connect to instrument on address:port
        direct.py port          # connect to assume simulator on localhost:port
        """
        exit()

    if len(sys.argv) == 2:
        host = 'localhost'
        port = int(sys.argv[1])
    else:
        host = sys.argv[1]
        port = int(sys.argv[2])

    direct = _Direct(host, port)
    direct.run()
