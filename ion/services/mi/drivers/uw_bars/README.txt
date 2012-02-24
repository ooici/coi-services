Driver code for the UW TRHPH BARS instrument
============================================

The real instrument was made accessible on 10.180.80.172 port 2001.

Our code assumes the BARS Program Version 1.7 - Last Revision: July 11, 2011
(or one with similar interaction characteristics) is running on the other side
of the communication. The real_bars_interaction.txt file captures a typical
interaction (via telnet).


Some preliminary programs
-------------------------

The following programs do a direct TCP connection with the instrument and were
mainly intended to examine actual interactions and eventually serve as a basis
for the actual implementation

* test/direct.py
Simple program for direct user iteraction with the instrument:
    $ bin/python ion/services/mi/drivers/uw_bars/test/direct.py 10.180.80.172 2001
See source code for details.

* bars_client.py
Class BarsClient does a direct communication with the instrument while
allowing some high-level operations (eg., is_collecting_data, enter_main_menu,
expect_generic_prompt).
The demo program is a complete script (no user interaction needed) involving:
check for data collection, break to main menu, see system info, and resume
data collection:

    $ bin/python ion/services/mi/drivers/uw_bars/bars_client.py  \
         --host 10.180.80.172 --port 2001 --outfile output.txt --loglevel debug
    DEBUG      bars_client               5988   MainThread      - ### connecting to 10.180.80.172:2001
    DEBUG      bars_client               5988   _Recv           - ### _Recv running.
    :: is instrument collecting data?
    DEBUG      bars_client               5988   MainThread      - ### is_collecting_data? ...
    :: Instrument is collecting data.
    :: break data streaming to enter main menu
    DEBUG      bars_client               5988   MainThread      - ### automatic ^S
    DEBUG      bars_client               5988   MainThread      - ### sending ^S
    DEBUG      bars_client               5988   MainThread      - ### sending ^S
    DEBUG      bars_client               5988   MainThread      - ### sending ^S
    DEBUG      bars_client               5988   MainThread      - ### sending ^S
    DEBUG      bars_client               5988   MainThread      - ### sending ^S
    DEBUG      bars_client               5988   MainThread      - ### sending ^S
    DEBUG      bars_client               5988   MainThread      - ### sending ^S
    DEBUG      bars_client               5988   MainThread      - ### got prompt. Sending one ^m to clean up any ^S leftover
    :: select 6 to get system info
    DEBUG      bars_client               5988   MainThread      - ### send: '6'
    DEBUG      bars_client               5988   MainThread      - ### expecting '.*--> '
    :: send enter to return to main menu
    DEBUG      bars_client               5988   MainThread      - ### send_enter
    DEBUG      bars_client               5988   MainThread      - ### expecting '.*--> '
    :: resume data streaming
    DEBUG      bars_client               5988   MainThread      - ### send: '1'
    :: sleeping for 10 secs to receive some data
    :: bye


* test/expect.py
A simple program exercising the pexpect library to communicate with the BARS
instrument. It works fine with an immediate pexpect.spawn.interact call
(basically emulating a telnet session) but further investigation would be
needed for programmatic interaction in case we wanted a pexpect-based
implementation for the driver.


* test/bars_simulator.py
A partial simulator for the BARS instrument intended to facilitate testing.
It accepts a TCP connection on a port and starts by sending out bursts of
random data every few seconds. By default it binds the service to an
automatically assigned port.
It accepts multiple clients but in sequential order.
    $ bin/python ion/services/mi/drivers/uw_bars/test/bars_simulator.py
                    |* [1]BarsSimulator: bound to port 53922
                    |* [1]BarsSimulator: ---waiting for connection---



Using MI core classes
---------------------

