#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

"""
A simple program exercising the pexpect library to communicate with the BARS
instrument.

You can test it with the BARS simulator:
    First, run the simulator:
    $ bin/python ion/services/mi/drivers/uw_bars/test/bars_simulator.py
            |* BarsSimulator: bound to port 63179
            |* BarsSimulator: waiting for connection

    Then, run this program like so:
    $ bin/python ion/services/mi/drivers/uw_bars/test/expect.py 63179

With the actual instrument:
    $ bin/python ion/services/mi/drivers/uw_bars/test/expect.py 10.180.80.172 2001
"""

try:
    import pexpect
except:
    print "This program requires pexpect."
    exit()

import sys
import time

if len(sys.argv) <= 1:
    print """USAGE:
    expect.py port          # assume connection to simulator on localhost:port
    expect.py address port  # connect to real instrument on address:port
    """
    exit()

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

prefix = "\n\t| "
def print_str(child, str):
    str = str.replace('\r\n', '\\r\\n\n')
    str = str.replace('\x13', '^S')
    str = str.replace('\x04', '^D')
    print prefix + str.replace('\n', prefix)

def print_match(child):
    print_str(child, child.after)

def print_before(child):
    print_str(child, child.before)

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
    print "# sending newline '%s'" % escape(NEWLINE)
#    child.send(NEWLINE)
    
    child.sendcontrol('m')
    child.sendcontrol('m')

def send_char(child, c, msg):
    assert len(c) == 1
    time.sleep(1)

    print "# sending '%s' (%s)" % (escape(c), msg)

    print "# first %s" % c
    child.send(c)
    child.sendcontrol('m')

    print "# second %s" % c
    child.send(c)
    child.sendcontrol('m')

#    c = c + NEWLINE
#    child.send(c)

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


#if not simulator:
#    child.interact()
#    terminate(simulator, child)

if not expect_data(child):
    terminate(simulator, child)

time.sleep(2)

got_prompt = False
limit_time = time.time() + 20  # the following for up to ~ 20+2 secs
while not got_prompt and time.time() < limit_time:
    print "# sending ^S and waiting for prompt"

    child.sendcontrol('s')
    if simulator:
        send_newline(child)

    index = child.expect(['.*--> ', pexpect.TIMEOUT], timeout=2)
    got_prompt = 0 == index

if got_prompt:
    print "# got prompt"
    print "# BEFORE:"
    print_before(child)
    print "# MATCH:"
    print_match(child)

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