Driver code for the UW TRHPH instrument
=======================================

Main documentation page: https://confluence.oceanobservatories.org/display/CIDev/UW+TRHPH

Some development notes:

2012-05-09:
- Removed files protocol.py, protocol_fsm.py, and driver.py that, although not
  used (obsolete at the moment) happen to be causing issues with
  bin/generate_interfaces. Putting stuff like this in cellar would be convenient
  but Tom L. says "you should remove them."
  Last commit containing those files:
  https://github.com/sfoley/coi-services/tree/c3af12c50ab562e6210c676d5d88f92638acf316/ion/services/mi/drivers/uw_trhph

2012-05-07:
- Various adjustments to align again with sfoley's master branch (which is to
  be merged into the mainline hopefully soon).

- test_instrument_agent_with_trhph.py completed with basically the same set of
  tests as with the other test cases:
    $ UW_TRHPH="simulator" bin/nosetests -v ion/services/mi/drivers/uw_trhph/test/test_instrument_agent_with_trhph.py
    -- INSTR-AGENT/TRHPH: initialize ... ok
    -- INSTR-AGENT/TRHPH: state transitions ... ok
    -- INSTR-AGENT/TRHPH: get valid and invalid params ... ok
    -- INSTR-AGENT/TRHPH: set valid params ... ok
    -- INSTR-AGENT/TRHPH: set invalid params ... ok
    -- INSTR-AGENT/TRHPH: get and set params ... ok
    -- INSTR-AGENT/TRHPH: execute stop autosample ... ok
    -- INSTR-AGENT/TRHPH: execute get metadata ... ok
    -- INSTR-AGENT/TRHPH: execute diagnostics ... ok
    -- INSTR-AGENT/TRHPH: execute get power statuses ... ok
    -- INSTR-AGENT/TRHPH: execute start autosample ... ok

    ----------------------------------------------------------------------
    Ran 11 tests in 273.497s

    OK

- All the other tests go as follows at this point:

  DRIVER via driver_client:
    $ UW_TRHPH="simulator" bin/nosetests -sv ion/services/mi/drivers/uw_trhph/test/test_trhph_driver_proc.py
    -- TRHPH DRIVER: basic tests ... ok
    -- TRHPH DRIVER: get valid params ... ok
    -- TRHPH DRIVER: get invalid params ... ok
    -- TRHPH DRIVER: set valid params ... ok
    -- TRHPH DRIVER: set invalid params ... ok
    -- TRHPH DRIVER: get and set params ... ok
    -- TRHPH DRIVER: stop autosample ... ok
    -- TRHPH DRIVER: get metadata ... ok
    -- TRHPH DRIVER: diagnostics ... ok
    -- TRHPH DRIVER: get power statuses ... ok
    -- TRHPH DRIVER: start autosample ... ok

    ----------------------------------------------------------------------
    Ran 11 tests in 291.347s

    OK


  The following two test cases with these prior and temporary preparations as
  a workround for the outstanding issue involving mixed monkey-patching wrt
  threading performed by some Nose plugin (even though that no pyon package is
  imported in any way):
    - remove the "pyon" path from the sys.path setting in bin/nosetests
    - define environment variable run_it

  DRIVER directly:
    $ run_it= UW_TRHPH="simulator" bin/nosetests -sv ion/services/mi/drivers/uw_trhph/test/test_trhph_driver.py
    -- TRHPH DRIVER: basic tests ... ok
    -- TRHPH DRIVER: get valid params ... ok
    -- TRHPH DRIVER: get invalid params ... ok
    -- TRHPH DRIVER: set valid params ... ok
    -- TRHPH DRIVER: set invalid params ... ok
    -- TRHPH DRIVER: get and set params ... ok
    -- TRHPH DRIVER: stop autosample ... ok
    -- TRHPH DRIVER: get metadata ... ok
    -- TRHPH DRIVER: diagnostics ... ok
    -- TRHPH DRIVER: get power statuses ... ok
    -- TRHPH DRIVER: start autosample ... ok

    ----------------------------------------------------------------------
    Ran 11 tests in 94.056s

    OK


  TrhphClient directly:
    $ run_it= UW_TRHPH="simulator" bin/nosetests -sv ion/services/mi/drivers/uw_trhph/test/test_trhph_client.py
    -- TRHPH CLIENT: Connect, get current state, sleep, disconnect ... ok
    -- TRHPH CLIENT: Get system info ... ok
    -- TRHPH CLIENT: Get data collection params ... ok
    -- TRHPH CLIENT: Set cycle time ... ok
    -- TRHPH CLIENT: Toggle data-only flag twice ... ok
    -- TRHPH CLIENT: Execute instrument diagnostics ... ok
    -- TRHPH CLIENT: Get sensor power statuses ... ok
    -- TRHPH CLIENT: Toggle all sensor power statuses twice ... ok
    -- TRHPH CLIENT: Go to main menu and resume streaming ... ok

    ----------------------------------------------------------------------
    Ran 9 tests in 151.666s

    OK


2012-05-06:
- Merged into coi-service mainline directly, including preliminary version of
  test_instrument_agent_with_trhph.py for tests on the TRHPH driver via the
  instrument agent.
  Also removed the old BARS code in the mainline.


2012-05-02:
- Now using a DriverTestMixin to avoid duplication of code in the TRHPH
  driver tests.
    The only requirement for a subclass is to provide the concrete driver
    implementation object by assigning it to self.driver, and to provide
    a configuration object and assign it to self.comm_config, which is
    used for the self.driver.configure call.

    Actual driver implementations currently tested with this mixin are:

    1) As shown in test_trhph_driver_proc, a driver proxy that interacts with
    corresponding driver client (which itself interacts with the actual
    driver process running separately).

    2) As shown in test_trhph_driver, the driver object is directly an
    instance of TrhphInstrumentDriver.

- Driver and simulator updated according to changes in new TRHPH instrument
  interface released on 2012-04-30, identified as "Version 1.25 - Last
  Revision: April 14, 2012."

- ALL important tests passing, both with real instrument and the simulator:

    $ export UW_TRHPH="10.180.80.172:2001"


    DRIVER via driver_client:

    $ bin/nosetests -sv ion/services/mi/drivers/uw_trhph/test/test_trhph_driver_proc.py
    -- DRIVER BASIC TESTS ... ok
    -- DRIVER GET PARAMS TESTS ... ok
    -- DRIVER SET TESTS ... ok
    -- DRIVER GET/SET TESTS ... ok
    -- DRIVER STOP AUTOSAMPLE TEST ... ok
    -- DRIVER EXECUTE GET METADATA TEST ... ok
    -- DRIVER EXECUTE DIAGNOSTICS TEST ... ok
    -- DRIVER EXECUTE GET POWER STATUSES TEST ... ok
    -- DRIVER START AUTOSAMPLE TEST ... ok

    ----------------------------------------------------------------------
    Ran 9 tests in 239.762s

    OK


    DRIVER directly:  (see note below)

    $ run_it= bin/nosetests -sv ion/services/mi/drivers/uw_trhph/test/test_trhph_driver.py
    -- DRIVER BASIC TESTS ... ok
    -- DRIVER GET PARAMS TESTS ... ok
    -- DRIVER SET TESTS ... ok
    -- DRIVER GET/SET TESTS ... ok
    -- DRIVER STOP AUTOSAMPLE TEST ... ok
    -- DRIVER EXECUTE GET METADATA TEST ... ok
    -- DRIVER EXECUTE DIAGNOSTICS TEST ... ok
    -- DRIVER EXECUTE GET POWER STATUSES TEST ... ok
    -- DRIVER START AUTOSAMPLE TEST ... ok

    ----------------------------------------------------------------------
    Ran 9 tests in 171.095s

    OK

    TrhphClient directly:  (see note below)

    $ run_it= bin/nosetests -sv ion/services/mi/drivers/uw_trhph/test/test_trhph_client.py
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
    Ran 9 tests in 250.581s

    OK


  **note**
    I recently learned that driver code should not in general use Gevent (!),
    so my core supporting module, trhph_client, is again implemented using
    threading.Thread (and not Greenlet). One important consequence of this is
    that the tests involving the TrhphClient in the same execution
    environment (that is, not in an external process) are *not* running
    properly because of a mix of monkey-patching and non mokey-patching
    triggered by bin/nosetests (apparently there is a Nose plugin that
    somehow executes pyon/__init__.py, where some special monkey-patching
    takes place). Note that this happens even when absolutely no packages
    with prefix "pyon" are imported, either directly or via other imports.
    So, these test cases are skipped unless the environment variable "run_it"
    is defined (its value is ignored).


- Some clean-up: Removed test cases related with obsolete files (protocol.py,
  driver.py): test_protocol.py, test_driver.py, test_driver_proc.py


- The whole set of tests with only the UW_TRHPH environment variable defined
  (and with value "simulator" to launch the simulator as part of the test
  cases) goes as follows:

    $ UW_TRHPH="simulator" bin/nosetests -sv ion/services/mi/drivers/uw_trhph
    test_get_cycle_time (ion.services.mi.drivers.uw_trhph.test.test_basic.BasicTrhphTest) ... ok
    test_get_power_statuses (ion.services.mi.drivers.uw_trhph.test.test_basic.BasicTrhphTest) ... ok
    test_get_system_info_metadata (ion.services.mi.drivers.uw_trhph.test.test_basic.BasicTrhphTest) ... ok
    test_get_verbose_vs_data_only (ion.services.mi.drivers.uw_trhph.test.test_basic.BasicTrhphTest) ... ok
    test_simple (ion.services.mi.drivers.uw_trhph.test.test_greenlet.Test) ... SKIP: define run_it to run this.
    -- Connect, get current state, sleep self._timeout and disconnect ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- Get system info ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- Get data collection params ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- Set cycle time ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- Toggle data-only flag twice ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- Execute instrument diagnostics ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- Get sensor power statuses ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- Toggle all sensor power statuses twice ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- Go to main menu and resume streaming ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER BASIC TESTS ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER GET PARAMS TESTS ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER SET TESTS ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER GET/SET TESTS ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER STOP AUTOSAMPLE TEST ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER EXECUTE GET METADATA TEST ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER EXECUTE DIAGNOSTICS TEST ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER EXECUTE GET POWER STATUSES TEST ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER START AUTOSAMPLE TEST ... SKIP: Not run by default because of mixed monkey-patching issues. Define environment variable run_it to force execution.
    -- DRIVER BASIC TESTS ... ok
    -- DRIVER GET PARAMS TESTS ... ok
    -- DRIVER SET TESTS ... ok
    -- DRIVER GET/SET TESTS ... ok
    -- DRIVER STOP AUTOSAMPLE TEST ... ok
    -- DRIVER EXECUTE GET METADATA TEST ... ok
    -- DRIVER EXECUTE DIAGNOSTICS TEST ... ok
    -- DRIVER EXECUTE GET POWER STATUSES TEST ... ok
    -- DRIVER START AUTOSAMPLE TEST ... ok

    ----------------------------------------------------------------------
    Ran 32 tests in 240.888s

    OK (SKIP=19)

  Only test_trhph_driver_proc will be run if ``-a INT'' is added to the
  bin/nosetests command above.




2012-04-30:
- Added ion/services/mi/driver_int_test_support.py to factor out common
  supporting functionality for driver integration tests (based on initial
  code by Steve).
  Already using it in test_trhph_driver_proc.py

- NOTE: new instrument interface released today, TRHPH program Version 1.25 - Last Revision: April 14, 2012
  Updated diagram. TODO update driver code accordingly.

2012-04-29:
- trhph_driver.py aligned and all tests passing as before, both with simulator
  and real instrument (Version 1.2 - Last Revision: Mar. 27, 2012):

    $ UW_TRHPH="10.180.80.172:2001" bin/nosetests -sv ion/services/mi/drivers/uw_trhph/test/test_trhph_client.py
    Ran 9 tests in 242.147s

    $ UW_TRHPH="10.180.80.172:2001" bin/nosetests -sv ion/services/mi/drivers/uw_trhph/test/test_trhph_driver.py
    Ran 9 tests in 162.387s


2012-04-24:
- TRHPH simulator now implemented using greenlets to facilitate integration
  with the rest of the pyon ecosystem. This is particularly important when
  the simulator is launched as part of the test setup (in concrete when
  indicating the environment variable UW_TRHPH="embsimulator", eg:
  $ UW_TRHPH="embsimulator" bin/nosetests -v ion/services/mi/drivers/uw_trhph/test/test_trhph_client.py

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
- Documentation updates almost complete, see UW+TRHPH confluence page
- All tests OK with both real instrument and simulator as before
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
