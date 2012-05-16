#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

"""
"""

from ion.services.mi.drivers.vadcp.pd0 import PD0DataStructure

import sys
import socket
import os
import re
import time

from threading import Thread


EOF = '\x04'
CONTROL_S = '\x13'

DATA_FILENAME = 'ion/services/mi/drivers/vadcp/test/pd0_sample.bin'


class _Recv(Thread):
    """
    """

    def __init__(self, conn):
        Thread.__init__(self, name="_Recv")
        self._conn = conn
        self._data = None
        self._file = None
        self.setDaemon(True)

    def run(self):
        print "### _Recv running."
        while True:
            recv = self._conn.recv(1)

            if recv == '\x7F':

                if self._data is None:
                    self._data = ''
                    self._file = file(DATA_FILENAME, 'w')
                    print "\n** RECEIVING DATA **\n"

            if self._data is None:
#                os.write(sys.stdout.fileno(), "%r " % repr(recv))
                os.write(sys.stdout.fileno(), ".")
                sys.stdout.flush()
                continue

            self._data += recv

            os.write(self._file.fileno(), recv)
            self._file.flush()

            if len(self._data) >= 20000:
                pd0 = PD0DataStructure(self._data)
                print "\n\n<<<pd0 = %s>>>\n\n" % pd0
                self._data = None
                break  #   TODO   NOTE THIS !!!!!!!!!!!!!!


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
            if cmd == "bin":
                print "### sending PD0 and then CS"
                self.send("PD0\r")
                self.send("CS\r")
            elif cmd == "q":
                print "### breaking"
                break
            elif cmd == "":
                print "### sending '^m'"
                self.send_control('m')
            else:
                print "### sending '%s' + ^m" % cmd
                self.send(cmd)
                self.send_control('m')

        self.stop()

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
