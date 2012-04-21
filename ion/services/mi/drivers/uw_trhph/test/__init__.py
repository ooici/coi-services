#!/usr/bin/env python

"""
@package ion.services.mi.drivers.test
@file ion/services/mi/drivers/test/__init__.py
@author Carlos Rueda

@brief Supporting stuff mainly for integration tests, that is, tests that
depend on a TRHPH instrument or simulator.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

import os
import unittest
from ion.services.mi.mi_logger import mi_logger
log = mi_logger


@unittest.skipIf(os.getenv('UW_TRHPH') is None,
                 'UW_TRHPH environment variable undefined')
class TrhphTestCase(unittest.TestCase):
    """
    Base class for test cases dependent on a TRHPH instrument or simulator.
    This is based on the UW_TRHPH environment variable.
    This class provides some supporting functionality related with
    configuration and launch of the simulator.

    This base class does not reference any Pyon elements, but can be used as a
    mixin, see PyonTrhphTestCase.

    "UW_TRHPH" environment variable:

    If this variable is not defined, then the test case is skipped.

    Otherwise:
    If UW_TRHPH is the literal value "simulator", then a simulator is
    launched as a separate process upon the first call to self.setUp.
    Such a process will be terminated at exit of the python instance.

    If UW_TRHPH is the literal value "embsimulator", then a new "embedded"
    (ie. not as a separate process) simulator is launched in setUp and
    terminated in tearDown.

    Otherwise, UW_TRHPH is assumed to be in the format address:port and a
    connection to such service will be used.

    "timeout" environment variable:
    A self._timeout variable is defined so it can be used across the various
    instrument interaction operations. The default timeout is 30 secs
    but can also be initialized from the environment variable "timeout".
    It's up to the subclasses to actually use this variable.

    "REAL_TRHPH" environment variable:
    A self._is_real_instrument variable is defined as a convenience for
    individual tests that depend on whether a real TRHPH instrument or the TRHPH
    simulator is being used. self._is_real_instrument is set to True only if
    the environment variable REAL_TRHPH is not defined with value "n" and the
    UW_TRHPH is not defined with a value containing "simulator".
    """

    @classmethod
    def setUpClass(cls):
        """
        Sets up _trhph, _timeout, _is_real_instrument according to corresponding
        environment variables.
        """
        cls._trhph = os.getenv('UW_TRHPH')

        cls._timeout = 30
        timeout_str = os.getenv('timeout')
        if timeout_str:
            try:
                cls._timeout = int(timeout_str)
            except:
                log.warn("Malformed timeout environment variable value '%s'",
                         timeout_str)
        log.info("Generic timeout set to: %d" % cls._timeout)

        cls._is_real_instrument = "n" != os.getenv('REAL_TRHPH') and \
            cls._trhph is not None and not "simulator" in cls._trhph
        log.info("_is_real_instrument set to: %s" % cls._is_real_instrument)

    def setUp(self):
        """
        Sets up the test case, launching a simulator if so specified and
        preparing self.config. Also sets self._timeout as the value
        indicated by the environment variable "timeout" (if defined) or 30;
        and also self._is_real_instrument depending on the enviroment
        variable REAL_TRHPH.
        """

        if self._trhph is None:
            # should not happen, but anyway just skip here:
            self.skipTest("Environment variable UW_TRHPH undefined")

        self._sim_launcher = None

        if "simulator" == self._trhph or "embsimulator" == self._trhph:
            self._sim_launcher = _SimulatorLauncher()
            self.device_port = self._sim_launcher.port
            self.device_address = 'localhost'
        else:
            try:
                a, p = self._trhph.split(':')
                port = int(p)
            except:
                self.skipTest("Malformed UW_TRHPH value")

            log.info("==Assuming TRHPH is listening on %s:%s==" % (a, p))
            self.device_address = a
            self.device_port = port

        self.config = {
            'method': 'ethernet',
            'device_addr': self.device_address,
            'device_port': self.device_port,
            'server_addr': 'localhost',
            'server_port': 8888
        }

        if self._sim_launcher is not None:
            self._sim_launcher.launch()

    def tearDown(self):
        """
        Stops simulator if so specified and joins calling thread to that of the
        simulator.
        """
        if self._sim_launcher is not None:
            self._sim_launcher.stop()


class _SimulatorLauncher(object):
    """
    Helper for TrhphTestCase to run the simulator either in the same python
    instance (we call this "embedded simulator") or in a separate OS process.
    This is determined by the UW_TRHPH environment variable:
    if UW_TRHPH=="simulator" then a unique separate process is launched;
    otherwise, self.launch() always starts a new simulator (in the running
    python instance, not as a separate process).

    This helper was mainly created to deal with issues related with gevent
    monkey patching that sometimes interferes with some of the tests when
    threads are involved. In concrete, the "embedded simulator" style in
    combination with pyon initialization makes the test case hang. The
    separate process style is more immune to that issue.

    In the case of a separate process, such process is launched only once
    in the current python execution environment and terminated at exit of the
    python instance via atexit.register'ing a function that sends the SIGTERM
    signal to the external simulator process.
    """

    _use_separate_process = "simulator" == os.getenv('UW_TRHPH')
    _os_proc = None
    _port = None

    @classmethod
    def _launch_separate_process(cls):
        """
        Launches (if not already) the separate process for the simulator.
        Returns the TCP port where such simulator has been bound to.
        """
        if cls._os_proc is None:
            cls._do_launch_separate_process()
        return cls._port

    @classmethod
    def _do_launch_separate_process(cls):
        """
        Unconditionally launches a separate process for the simulator.
        Sets the _os_proc and _port class variables.
        """
        import subprocess
        import time
        import re
        import signal

        output_name = 'sim_output.txt'

        args = ['bin/python',
                'ion/services/mi/drivers/uw_trhph/test/trhph_simulator.py',
                '--logfile', output_name
        ]

        log.info("==STARTING SIMULATOR== %s" % str(args))

        # bufsize=1: line buffered. The goal is that we be able to scan the
        # few first lines of the subprocess output for the port.
        cls._os_proc = subprocess.Popen(args, bufsize=1)
        log.info("process launched, pid=%s" % cls._os_proc.pid)
        time.sleep(2)

        # now, capture the port used by the simulator:
        port = None
        fread = file(output_name, 'r')
        lineno, max_lines = 0, 10
        while port is None and lineno < max_lines:
            output = fread.readline()
            lineno += 1
            mo = re.search(r'bound to port (\d+)', output)
            if mo is not None:
                port = int(mo.group(1))
        fread.close()

        if port is None:
            log.error("could not scan port number from subprocess output!")
        else:
            log.info("simulator subprocess bound to port = %s" % str(port))

        cls._port = port

        def terminate_simulator():
            log.info("===ENDING SIMULATOR=== pid=%s" % cls._os_proc.pid)
            cls._os_proc.send_signal(signal.SIGTERM)
        import atexit
        atexit.register(terminate_simulator)

    def __init__(self):
        self._port = None
        self._simulator = None

        if _SimulatorLauncher._use_separate_process:
            self._port = _SimulatorLauncher._launch_separate_process()
        else:
            import ion.services.mi.drivers.uw_trhph.test.trhph_simulator as s
            self._simulator = s.TrhphSimulator(accept_timeout=10.0)
            self._port = self._simulator.port

    def launch(self):
        if _SimulatorLauncher._use_separate_process:
            pass  # already launched in __init__
        else:
            log.info("===STARTING SIMULATOR===")
            self._simulator.start()

    @property
    def port(self):
        return self._port

    def stop(self):
        if _SimulatorLauncher._use_separate_process:
            pass  # subprocess will be sent SIGTERM at exit.
        else:
            log.info("===STOPPING SIMULATOR===")
            self._simulator.stop()
            self._simulator.join()
