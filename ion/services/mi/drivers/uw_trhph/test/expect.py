#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

"""
A simple program exercising the pexpect library to communicate with the TRHPH
instrument.

NOTE:
 - works fine with immediate pexpect.spawn.interact
 - but there're sync issues with pexpect in general

Try this program with the TRHPH simulator:
    First, run the simulator:
    $ bin/python ion/services/mi/drivers/uw_trhph/test/trhph_simulator.py
            |* TrhphSimulator: bound to port 63179
            |* TrhphSimulator: waiting for connection

    Then, run this program like so:
    $ bin/python ion/services/mi/drivers/uw_trhph/test/expect.py 63179

With the actual instrument:
$ bin/python ion/services/mi/drivers/uw_trhph/test/expect.py 10.180.80.172 2001
"""

try:
    import pexpect
    pexpect_ok = True
except:
    pexpect_ok = False

import sys
import time

logfile = None
#logfile = sys.stdout

NEWLINE = '\r\n'
DATA_LINE_PATTERN = r'(\d+\.\d*\s*){12}.*'


def escape(str):
    str = str.replace('\r', '\\r')
    str = str.replace('\n', '\\n')
    str = str.replace('\x13', '^S')
    str = str.replace('\x04', '^D')
    return str

PREFIX = "\n\t| "


def prefix(child, str):
    str = str.replace('\r\n', '\\r\\n\n')
    str = str.replace('\x13', '^S')
    str = str.replace('\x04', '^D')
    return PREFIX + str.replace('\n', PREFIX)


def print_match(child):
    print prefix(child, child.after)


def print_before(child):
    print prefix(child, child.before)


def expect_data(child, timeout=30):
    print "# expecting data (timeout=%d)" % timeout
    index = child.expect([DATA_LINE_PATTERN, pexpect.TIMEOUT], timeout=timeout)
    got_data = 0 == index
    if got_data:
        print_match(child)
    else:
        print "# didn't get data.  before is:"
        print_before(child)
    return got_data


def send_newline(child):
    time.sleep(1)
    print "# sending ^M"
    child.sendcontrol('m')


def send_char(child, c, msg):
    assert len(c) == 1
    time.sleep(1)

    print "# sending '%s' (%s)" % (escape(c), msg)
    child.send(c + '\r')
#    child.sendcontrol('m')


def terminate(simulator, child):
    if child.isalive():
        if simulator:
            print "# terminate: sending q"
            child.sendline('q')
        child.close()

    if child.isalive():
        print 'Child did not exit gracefully.'
    else:
        print 'Child exited gracefully.'

    exit()


def main():
    simulator = False
    if len(sys.argv) == 2:
        host = 'localhost'
        port = sys.argv[1]
        simulator = True
    else:
        host = sys.argv[1]
        port = sys.argv[2]

    print "# spawning telnet"
    child = pexpect.spawn('telnet', [host, port], logfile=logfile)

    ### Note: immediate interaction with real instrument works fine with
    ### pexpect, but not as well when doing step-by-step interaction.
    #child.interact()
    #terminate(simulator, child)

    if not expect_data(child):
        terminate(simulator, child)

    time.sleep(2)

    got_prompt = False
    limit_time = time.time() + 20  # the following for up to ~ 20+2 secs
    while not got_prompt and time.time() < limit_time:
        print "# sending ^S and waiting for prompt"
        child.sendcontrol('s')
        if simulator:
            child.sendcontrol('m')
        index = child.expect(['.*--> ', pexpect.TIMEOUT], timeout=2)
        got_prompt = 0 == index

    if got_prompt:
        print "# got prompt"
        print "# MATCH:"
        print_match(child)

        send_newline(child)

        time.sleep(3)

        send_char(child, '6', "system info")
        child.expect('.*--> ')
        print_match(child)

        send_newline(child)

        print "# expecting prompt (main menu)"
        child.expect('.*--> ')
        print_match(child)

        expect_data(child)

    #    print("# Entering interact mode. Escape character is '^]'.\n")
    #    sys.stdout.write(child.after)
    #    sys.stdout.flush()
    #    child.interact()
    #    print '# Left interactive mode.'

    terminate(simulator, child)


if __name__ == '__main__':
    if not pexpect_ok:
        print "This program requires pexpect."
        exit()

    if len(sys.argv) <= 1:
        print """USAGE:
        expect.py port          # connect to localhost:port
        expect.py address port  # connect to address:port
        """
        exit()

    main()
