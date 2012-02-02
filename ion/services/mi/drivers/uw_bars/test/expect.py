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

simulator = False
if len(sys.argv) == 2:
    host = 'localhost'
    port = sys.argv[1]
    simulator = True
else:
    host = sys.argv[1]
    port = sys.argv[2]

child = pexpect.spawn('telnet', [host, port])
child.expect('.*')

got_prompt = False
if simulator:
    child.sendline('^S')
    got_prompt = True
else:
    limit_time = time.time() + 10  # the following up to ~ 10+2 secs
    while not got_prompt and time.time() < limit_time:
        print "sending ^S and waiting for prompt"
        child.sendcontrol('s')
        index = child.expect(['.*--> ', pexpect.TIMEOUT], timeout=2)
        got_prompt = 0 == index

if got_prompt:
    child.expect('.*--> ')
    child.sendline('3')  # system diagnostics
    child.expect('.*--> ')
    child.sendline('6')  # system info

    print("Escape character is '^]'.\n")
    sys.stdout.write(child.after)
    sys.stdout.flush()
    child.interact()

    # At this point the script is running again.
    print 'Left interactve mode.'

# The rest is not strictly necessary. This just demonstrates a few functions.
# This makes sure the child is dead; although it would be killed when Python exits.
if child.isalive():
    if simulator:
        child.sendline('q')
    child.close()

if child.isalive():
    print 'Child did not exit gracefully.'
else:
    print 'Child exited gracefully.'
