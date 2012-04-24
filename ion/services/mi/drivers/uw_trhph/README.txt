Driver code for the UW TRHPH instrument
=======================================

Main documentation page: https://confluence.oceanobservatories.org/display/CIDev/UW+TRHPH

Some development notes:

2012-04-21,24:
Adjustments to start aligning with Edward's refactoring via Steve's
sfoley/mmaster branch:
- DriverAnnouncement is gone so removed references to it
- DriverChannel is gone so removed references to it
- Others agreed with Steve.
- Committing to sfoley/master.
- All TRHPH code is at least not causing errors. As before, most tests are
  dependent on the UW_TRHPH environment variable, with test_trhph_client.py
  now passing OK, and the others being still skipped while the alignment
  is completed.

2012-04-05:
- Systematic BARS->TRHPH related renamings.
- Simulator updated.
- Documentation updates almost complete, seeUW+TRHPH confluence page
- All tests OK with both real instrument and simulator as before, see the
- This version tagged: "0.1.2"


-----------------------------------------------------------------------------
The rest of this document uses "BARS" as the main term for the instrument.

2012-04-05:
- Completed all changes to align the driver code with the new instrument
  program TRHPH Version 1.2 - Last Revision: Mar. 27, 2012.
  Only pending updates are on the simulator.

- As shown below, *all* tests are OK with the real instrument. At this point
  I'm committing the current state and tagging it with "0.1.1". Then, to avoid
  potential confusion with the BARS vs. TRHPH terminology, I'll be doing a
  systematic renaming from "bars" to "trhph" everywhere in the code, expect
  where specifically referring to the BARS instrument. That will be tagged
  "0.1.2".

- Removed all system clock related functionality:
    bars.py: all related definitions -> removed
    BarsClient.get_system_clock -> removed
    BarsClient.set_system_clock -> removed
    test_bars_client.test_40_get_system_clock -> removed
    test_bars_client.test_45_set_system_clock -> removed
    test_basic.py: all related tests -> removed

- Tests:
  NOTE: Now the "toggle" tests are NOT skipped any more for the real
  instrument: the toggle operations are applied twice so the corresponding
  settings get reverted to its original values before the test:

    $ export UW_BARS="10.180.80.172:2001"
    $ export run_it=

    # All the tests (~11 min):
    $ bin/nosetests ion/services/mi/drivers/uw_bars
    ...................................
    ----------------------------------------------------------------------
    Ran 35 tests in 670.024s

    OK

    # Main individual tests:

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_bars_client.py
    -- Connect, get current state, sleep self._timeout and disconnect ... ok
    -- Get system info ... ok
    -- Get data collection params ... ok
    -- Set cycle time ... ok
    -- Toggle data-only flag twice ... ok
    -- Execute instrument diagnostics ... ok
    -- Get sensor power statuses ... ok
    -- Toggle all sensor power statuses twice ... ok
    -- Go to main menu and resume streaming ... ok

    ----------------------------------------------------------------------
    Ran 9 tests in 206.055s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_protocol.py
    -- PROTOCOL CONNECT/DISCONNECT test ... ok
    -- PROTOCOL START AUTOSAMPLE test ... ok
    -- PROTOCOL STOP AUTOSAMPLE test ... ok
    -- PROTOCOL GET test ... ok
    -- PROTOCOL SET test ... ok
    -- PROTOCOL GET/SET test ... ok
    -- PROTOCOL EXECUTE GET METADATA test ... ok
    -- PROTOCOL EXECUTE DIAGNOSTICS test ... ok
    -- PROTOCOL EXECUTE GET POWER STATUSES test ... ok

    ----------------------------------------------------------------------
    Ran 9 tests in 148.520s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_driver.py
    -- DRIVER CONNECT/DISCONNECT test ... ok
    -- DRIVER START AUTOSAMPLE test ... ok
    -- DRIVER STOP AUTOSAMPLE test ... ok
    -- DRIVER GET test ... ok
    -- DRIVER SET test ... ok
    -- DRIVER EXECUTE GET METADATA test ... ok
    -- DRIVER EXECUTE DIAGNOSTICS test ... ok
    -- DRIVER EXECUTE GET POWER STATUSES test ... ok

    ----------------------------------------------------------------------
    Ran 8 tests in 102.963s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_driver_proc.py
    BARS connect/disconnect tests ... ok
    BARS get tests ... ok
    BARS get and set tests ... ok
    BARS set test ... ok

    ----------------------------------------------------------------------
    Ran 4 tests in 119.212s

    OK



2012-04-04:
- New program interface was made available online today (at same usual
  address 10.180.80.172:2001).
  Note that this program is now identified as "Temperature Resistivity
  Probe (TRHPH)" (formerly "Benthic and Resistivity Sensors (BARS)") and it
  brings several changes to the interface as recently agreed with UW.

  Via email we confirmed with UW that this new version corresponds to the
  instrument intended to be used by OOI-CI; the previous BARS instrument was
  made available "while the actual TRHPH was being constructed... The driver
  that is developed does not need to be back compatible with the BARS."

  I just created a tag "0.1.0.bars" on the Git repo to reflect the state of
  things right before proceeding with the changes for the new TRHPH program
  interface.

- real_trhph_interaction.txt captures a 'telnet 10.180.80.172 2001' session
  with the new instrument interface.

- Following the tests in test_bars_client.py, did adjustments so the
  following tests as passing OK:
    test_00_connect_disconnect
    test_00_resume_streaming
    test_10_get_system_info



2012-03-30:
- Commented out code contents in test/pyon_test.py, because PyonBarsTestCase is
  not used anywhere yet its inclusion by nosetest causes misbehaviours to other
  tests.
- Generated code coverage report as follows:
    $ run_it= UW_BARS="simulator" bin/nosetests --with-coverage --cover-erase \
      --cover-package ion.services.mi.drivers.uw_bars \
       ion/services/mi/drivers/uw_bars
    Name                                          Stmts   Miss  Cover   Missing
    ---------------------------------------------------------------------------
    ion.services.mi.drivers.uw_bars                   1      0   100%
    ion.services.mi.drivers.uw_bars.bars             69      2    97%   185, 188
    ion.services.mi.drivers.uw_bars.bars_client     659    140    79%   116-126, 129-136, 200, 241, 244-245, 273, 321-322, 333-334, 451-458, 468, 504, 544, 579, 602-631, 646-647, 674, 685, 711, 714, 754-759, 793-794, 827, 866, 906, 948, 966, 1004, 1012, 1015, 1053, 1094-1104, 1155, 1210-1228, 1232-1278
    ion.services.mi.drivers.uw_bars.common           24      0   100%
    ion.services.mi.drivers.uw_bars.driver           46      0   100%
    ion.services.mi.drivers.uw_bars.protocol        242     67    72%   74, 78, 86, 105, 142-148, 159, 189, 202, 234, 252, 290-293, 303, 322-327, 336, 339, 344-347, 350-359, 366, 379-384, 391, 404-409, 425, 448-453, 466, 480-485, 497, 509-514
    ---------------------------------------------------------------------------
    TOTAL                                          1041    209    80%
    ----------------------------------------------------------------------
    Ran 38 tests in 541.354s

    OK
  and updated confluence page.  

2012-03-21:
- Documentation update
- Code clean-up (removal of obsolete files and some renamings)
- BARS output file name unified to bars_output.txt across the various tests.
- test_driver_proc updated

- All tests successful:
    $ export run_it=

  - Against actual instrument (30 tests ok; 2 skipped):
    $ export UW_BARS="10.180.80.172:2001"

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_driver_proc.py
    BARS connect/disconnect tests ... ok
    BARS get tests ... ok
    BARS get and set tests ... ok
    BARS set test ... ok

    ----------------------------------------------------------------------
    Ran 4 tests in 118.108s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_driver.py
    -- DRIVER CONNECT/DISCONNECT test ... ok
    -- DRIVER START AUTOSAMPLE test ... ok
    -- DRIVER STOP AUTOSAMPLE test ... ok
    -- DRIVER GET test ... ok
    -- DRIVER SET test ... ok
    -- DRIVER EXECUTE GET METADATA test ... ok
    -- DRIVER EXECUTE DIAGNOSTICS test ... ok
    -- DRIVER EXECUTE GET POWER STATUSES test ... ok

    ----------------------------------------------------------------------
    Ran 8 tests in 99.460s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_protocol.py
    -- PROTOCOL CONNECT/DISCONNECT test ... ok
    -- PROTOCOL START AUTOSAMPLE test ... ok
    -- PROTOCOL STOP AUTOSAMPLE test ... ok
    -- PROTOCOL GET test ... ok
    -- PROTOCOL SET test ... ok
    -- PROTOCOL GET/SET test ... ok
    -- PROTOCOL EXECUTE GET METADATA test ... ok
    -- PROTOCOL EXECUTE DIAGNOSTICS test ... ok
    -- PROTOCOL EXECUTE GET POWER STATUSES test ... ok

    ----------------------------------------------------------------------
    Ran 9 tests in 185.132s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_bars_client.py
    -- Connect, get current state, sleep self._timeout and disconnect ... ok
    -- Go to main menu and resume streaming ... ok
    -- Get system info ... ok
    -- Get data collection params ... ok
    -- Set cycle time ... ok
    -- Toggle data-only flag ... SKIP: Skipped for real instrument
    -- Execute instrument diagnostics ... ok
    -- Get sensor power statuses ... ok
    -- Set sensor power statuses (toggles all) ... SKIP: Skipped for real instrument
    -- Get system clock ... ok
    -- Set system clock ... ok

    ----------------------------------------------------------------------
    Ran 11 tests in 199.573s

    OK (SKIP=2)


  - Against simulator (32 tests ok; 0 skipped):
    $ export UW_BARS="simulator"

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_driver_proc.py
    BARS get tests ... ok
    BARS get and set tests ... ok
    BARS set test ... ok

    ----------------------------------------------------------------------
    Ran 4 tests in 126.549s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_driver.py
    -- DRIVER CONNECT/DISCONNECT test ... ok
    -- DRIVER START AUTOSAMPLE test ... ok
    -- DRIVER STOP AUTOSAMPLE test ... ok
    -- DRIVER GET test ... ok
    -- DRIVER SET test ... ok
    -- DRIVER EXECUTE GET METADATA test ... ok
    -- DRIVER EXECUTE DIAGNOSTICS test ... ok
    -- DRIVER EXECUTE GET POWER STATUSES test ... ok

    ----------------------------------------------------------------------
    Ran 8 tests in 111.862s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_protocol.py
    -- PROTOCOL CONNECT/DISCONNECT test ... ok
    -- PROTOCOL START AUTOSAMPLE test ... ok
    -- PROTOCOL STOP AUTOSAMPLE test ... ok
    -- PROTOCOL GET test ... ok
    -- PROTOCOL SET test ... ok
    -- PROTOCOL GET/SET test ... ok
    -- PROTOCOL EXECUTE GET METADATA test ... ok
    -- PROTOCOL EXECUTE DIAGNOSTICS test ... ok
    -- PROTOCOL EXECUTE GET POWER STATUSES test ... ok

    ----------------------------------------------------------------------
    Ran 9 tests in 147.783s

    OK

    $ bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_bars_client.py
    -- Connect, get current state, sleep self._timeout and disconnect ... ok
    -- Go to main menu and resume streaming ... ok
    -- Get system info ... ok
    -- Get data collection params ... ok
    -- Set cycle time ... ok
    -- Toggle data-only flag ... ok
    -- Execute instrument diagnostics ... ok
    -- Get sensor power statuses ... ok
    -- Set sensor power statuses (toggles all) ... ok
    -- Get system clock ... ok
    -- Set system clock ... ok

    ----------------------------------------------------------------------
    Ran 11 tests in 155.856s

    OK


2012-03-20:
- adjust bars_simulator according to recent updates to BARS program.
- All tests against simulator passing in a simular way as with real instrument.

2012-03-19:
- Update BARS state machine diagram according to BARS program "Version 1.75 - Last Revision: March 15, 2012."
- refresh state: add step to send backspaces (^H) before ^M.
- Add VERBOSE_MODE to BarsParameter and handle retrieval via get operation in
  protocol.

- Several additions to protocol and driver to expose most of the BarsClient
functionality.
- Tests go as follows:
    1$ tail -f bars_output.txt
    2$ run_it= UW_BARS="10.180.80.172:2001"  bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_driver.py:DriverTest
    -- DRIVER CONNECT/DISCONNECT test ... ok
    -- DRIVER START AUTOSAMPLE test ... ok
    -- DRIVER STOP AUTOSAMPLE test ... ok
    -- DRIVER GET test ... ok
    -- DRIVER SET test ... ok
    -- DRIVER EXECUTE GET METADATA test ... ok
    -- DRIVER EXECUTE DIAGNOSTICS test ... ok
    -- DRIVER EXECUTE GET POWER STATUSES test ... ok

    ----------------------------------------------------------------------
    Ran 8 tests in 107.167s

    OK

    1$ tail -f bars_output.txt
    2$ run_it= UW_BARS="10.180.80.172:2001"  bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_protocol.py:ProtocolTest
    -- PROTOCOL CONNECT/DISCONNECT test ... ok
    -- PROTOCOL START AUTOSAMPLE test ... ok
    -- PROTOCOL STOP AUTOSAMPLE test ... ok
    -- PROTOCOL GET test ... ok
    -- PROTOCOL SET test ... ok
    -- PROTOCOL GET/SET test ... ok
    -- PROTOCOL EXECUTE GET METADATA test ... ok
    -- PROTOCOL EXECUTE DIAGNOSTICS test ... ok
    -- PROTOCOL EXECUTE GET POWER STATUSES test ... ok

    ----------------------------------------------------------------------
    Ran 9 tests in 188.820s

    OK


    1$ tail -f bars_output.txt
    2$ run_it= UW_BARS="10.180.80.172:2001"  bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_bars_client.py:BarsClientTest
    -- Connect, get current state, sleep self._timeout and disconnect ... ok
    -- Go to main menu and resume streaming ... ok
    -- Get system info ... ok
    -- Get data collection params ... ok
    -- Set cycle time ... ok
    -- Toggle data-only flag ... SKIP: Skipped for real instrument
    -- Execute instrument diagnostics ... ok
    -- Get sensor power statuses ... ok
    -- Set sensor power statuses (toggles all) ... SKIP: Skipped for real instrument
    -- Get system clock ... ok
    -- Set system clock ... ok

    ----------------------------------------------------------------------
    Ran 11 tests in 190.285s

    OK (SKIP=2)




2012-03-16:
- some adjustments to sync state; add documentation.
- Tests OK as before:
  run_it= UW_BARS="10.180.80.172:2001"  bin/nosetests -sv ion/services/mi/drivers/uw_bars/test/test_bars_client.py:BarsClientTest
  run_it= UW_BARS="10.180.80.172:2001"  bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_protocol.py:ProtocolTest


2012-03-14:
- BarsClient test case with actual instrument now successful:
    $ run_it= UW_BARS="10.180.80.172:2001"  bin/nosetests -sv ion/services/mi/drivers/uw_bars/test/test_bars_client.py:BarsClientTest
    -- Break streaming and resume streaming ... ok
    -- Connect, get current state, sleep self._timeout and disconnect ... ok
    -- Get system info ... ok
    -- Get cycle time ... ok
    -- Set cycle time ... ok
    -- Get data-only flag ... ok
    -- Toggle data-only flag ... SKIP: Skipped for real instrument
    -- Execute instrument diagnostics ... ok
    -- Get sensor power statuses ... ok
    -- Set sensor power statuses (toggles all) ... SKIP: Skipped for real instrument
    -- Get system clock ... ok
    -- Set system clock ... ok

    ----------------------------------------------------------------------
    Ran 12 tests in 175.643s

    OK (SKIP=2)

  Although I ran it successfully several times, more tuning is necessary to
  make the tool more robust.

- test_protocol also successful with the actual instrument:
    $ run_it= UW_BARS="10.180.80.172:2001"  bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_protocol.py:ProtocolTest
    -- PROTOCOL CONNECT/DISCONNECT test ... ok
    -- PROTOCOL EXECUTE GET METADATA test ... ok
    -- PROTOCOL GET test ... ok
    -- PROTOCOL GET/SET test ... ok
    -- PROTOCOL SET test ... ok

    ----------------------------------------------------------------------
    Ran 5 tests in 82.189s

    OK

  NOTE: the test cases (both test_bars_client and test_bars_protocol)
  are pretty stable, but become less robust when there is a concurrent client
  connection to the instrument. In concrete, when I also have a telnet session
  open, I note that some of the errors happen because the _Recv (in
  bars_client) does not always get a complete prompt thus making the
  corresponding expectation in the program fail with a timeout (btw, the
  telnet session does seem to always show the complete prompts).
  So, the conclusion for the moment is:
   **Note: interaction with real instrument is less reliable when
   there is concurrent access to the instrument by other communication
   clients.


2012-03-13:
- Actual instrument made available again today
- Adjustments in BarsClient during tests with actual instrument
- Tests with simulator continue to be successful
- Tests with actual instrument go as follows:
    $ run_it= UW_BARS="10.180.80.172:2001"  bin/nosetests -sv ion/services/mi/drivers/uw_bars/test/test_bars_client.py
    -- Break streaming and resume streaming ... ok
    -- Connect, get current state, sleep self._timeout and disconnect ... ok
    -- Get system info ... ok
    -- Get cycle time ... ok
    -- Set cycle time ... ERROR
    -- Get data-only flag ... ok
    -- Toggle data-only flag ... SKIP: Skipped for real instrument
    -- Execute instrument diagnostics ... ok
    -- Get sensor power statuses ... ok
    -- Set sensor power statuses (toggles all) ... SKIP: Skipped for real instrument
    -- Get system clock ... ok
    -- Set system clock ... ERROR
  where the errors seem related with not getting the expected response. For
  example, the last logged line by the _Recv thread was:
        "Do you want to Change the Current Time? (0 = No, 1 = Yes) --"
  while a parallel telnet session showed:
        "Do you want to Change the Current Time? (0 = No, 1 = Yes) --> "
  So, some socket setting to force the flush of all received data from
  the socket seems to be missing. TODO.

2012-03-09:
- BarsClient: notifying data sample reception
- BarsInstrumentProtocol: set data listener to announce_to_driver
(DriverAnnouncement.DATA_RECEIVED, data_sample=sample)
- Adjustments in InstrumentProtocol.announce_to_driver and new
  EventKey.DATA_SAMPLE to support the above.

2012-03-08:
- BarsClient completed including toggle data-only vs. verbose flag. Only
  pending is the actual handling of the data bursts in verbose mode for which
  need to wait until having access to the real instrument again.
  Also added cycle time-related tests to test_bars_client.

    $ run_it= UW_BARS="simulator" bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_bars_client.py
    Break streaming and resume streaming ... ok
    Get system info ... ok
    Get cycle time ... ok
    Set cycle time ... ok
    Get data-only flag ... ok
    Toggle data-only flag ... ok
    Execute instrument diagnostics ... ok
    Get sensor power statuses ... ok
    Set sensor power statuses (toggles all) ... ok
    Get system clock ... ok
    Set system clock ... ok

    ----------------------------------------------------------------------
    Ran 11 tests in 335.674s

    OK

- bars_simulator:
 - adjusted to support all these operations
 - some doc updates.

- "timeout" environment variable can now be indicated to specify the generic
  operation timeout. See bars_client.py. This will in particular facilitate
  the initial testing against the actual instrument once it is available again.

2012-03-07:
- BarsClient: completed implementation of ALL major operations (only pending
is the "verbose vs. data only" related operations), and tested against the
simulator:

    # run simulator in one terminal:
    $ bin/python ion/services/mi/drivers/uw_bars/test/bars_simulator.py
    INFO   MainThread   bars_simulator       __init__                  - [1] bound to port 53209
    ...

    # run the bars_client tests:
    $ run_it= UW_BARS="localhost:53209"  bin/nosetests -v ion/services/mi/drivers/uw_bars/test/test_bars_client.py
    Break streaming and resume streaming ... ok
    Get system info ... ok
    Execute instrument diagnostics ... ok
    Get sensor power statuses ... ok
    Set sensor power statuses (toggles all) ... ok
    Get system clock ... ok
    Set system clock ... ok

    ----------------------------------------------------------------------
    Ran 7 tests in 172.638s

    OK
