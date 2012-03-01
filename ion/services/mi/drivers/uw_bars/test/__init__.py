#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

import os
import unittest
from ion.services.mi.mi_logger import mi_logger
log = mi_logger


@unittest.skipIf(os.getenv('UW_BARS') is None,
                 'UW_BARS environment variable undefined')
class BarsTestCase(unittest.TestCase):
    """
    Base class for test cases dependent on the UW_BARS environment variable
    and providing some supporting functionality related with configuration
    and launch of simulator.

    This base class does not reference any Pyon elements, but can be used as a
    mixin, see PyonBarsTestCase.

    If the environment variable UW_BARS is not defined, then the test case is
    skipped.

    Otherwise:
        If UW_BARS is the literal value "simulator", then a simulator is
        launched as a separate process upon the first call to self.setUp.
        Such a process will be terminated at exit of the python instance.

        If UW_BARS is the literal value "embsimulator", then a new simulator
        is launched in setUp and terminated in tearDown.

        Otherwise, UW_BARS is assumed to be in the format address:port and a
        connection to such service will be used.

        Corresponding self.config object initialized accordingly.
    """

    def setUp(self):
        """
        Sets up the test case, launching a simulator if so specified and
        preparing self.config.
        """

        bars = os.getenv('UW_BARS')

        if bars is None:
            # should not happen, but anyway just skip here:
            self.skipTest("Environment variable UW_BARS undefined")

        self._sim_launcher = None

        if "simulator" == bars or "embsimulator" == bars:
            self._sim_launcher = _SimulatorLauncher()
            self.device_port = self._sim_launcher.port
            self.device_address = 'localhost'
        else:
            try:
                a, p = bars.split(':')
                port = int(p)
            except:
                self.skipTest("Malformed UW_BARS value")

            log.info("==Assuming BARS is listening on %s:%s==" % (a, p))
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
    Helper for BarsTestCase to run the simulator either in the same python
    instance (we call this "embedded simulator") or in a separate OS process.
    This is determined by the UW_BARS environment variable:
    if UW_BARS=="simulator" then a unique separate process is launched;
    otherwise, self.launch() always starts a new simulator (in the running
    python instance, not as a separate process)

    This helper was mainly created to deal with issues related with gevent
    monkey patching that sometimes interferes with some of the tests when
    threads are involved. In concrete, the "embedded simulator" style in
    combination with pyon initialization makes the test case hang. The
    separate process style is more immune to that issue.

    In the case of a separate process, such process is launched only once
    in the current python execution environment and terminated at exit of the
    python instance via atexit.register(cls._os_proc.kill).
    """

    _use_separate_process = "simulator" == os.getenv('UW_BARS')
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

        output_name = 'sim_output.txt'

        args = ['bin/python',
                'ion/services/mi/drivers/uw_bars/test/bars_simulator.py',
                '--outfile', output_name
        ]

        log.info("\n==STARTING SIMULATOR== %s" % str(args))

        # bufsize=1: line buffered. The goal is that we be able to scan the
        # few first lines of the subprocess output for the port.
        cls._os_proc = subprocess.Popen(args, bufsize=1)
        log.info("process launched, pid=%s" % cls._os_proc.pid)
        time.sleep(0.2)

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

        import atexit
        atexit.register(cls._os_proc.kill)

    def __init__(self):
        self._port = None
        self._simulator = None

        if _SimulatorLauncher._use_separate_process:
            self._port = _SimulatorLauncher._launch_separate_process()
        else:
            import ion.services.mi.drivers.uw_bars.test.bars_simulator as bs
            self._simulator = bs.BarsSimulator(accept_timeout=10.0)
            self._port = self._simulator.port

    def launch(self):
        if _SimulatorLauncher._use_separate_process:
            pass  # already launched in __init__
        else:
            log.info("\n==STARTING SIMULATOR==")
            self._simulator.start()

    @property
    def port(self):
        return self._port

    def stop(self):
        if _SimulatorLauncher._use_separate_process:
            pass  # subprocess will be killed at exit.
        else:
            log.info("==STOPPING SIMULATOR==")
            self._simulator.stop()
            self._simulator.join()
