#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.process_util
@file    ion/agents/platform/rsn/simulator/process_util.py
@author  Carlos Rueda
@brief   Utility to launch/shutdown the RSN OMS simulator as an external process.
         (elements adapted/simplified from driver_process.py)

simple test:
  bin/python ion/agents/platform/rsn/simulator/process_util.py
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from ion.agents.platform.rsn.oms_client_factory import CIOMSClientFactory

from pyon.util.log import log
import os
import subprocess
import signal
from gevent import sleep

_PYTHON_PATH = 'bin/python'
_PROGRAM     = "ion/agents/platform/rsn/simulator/oms_simulator_server.py"
_COMMAND    = [_PYTHON_PATH, _PROGRAM, "--port", "0"]
# note: "--port 0" to bind the simulator to a newly generated port.


class ProcessUtil(object):
    """
    Utility to launch/shutdown the RSN OMS simulator as an external process.
    """
    def __init__(self):
        self._process = None
        self._rsn_oms = None

    def launch(self):
        """
        Launches the simulator process as indicated by _COMMAND.

        @return (rsn_oms, uri) A pair with the CIOMSSimulator instance and the
                associated URI to establish connection with it.
        """
        log.debug("[OMSim] Launching: %s", _COMMAND)

        self._process = self._spawn(_COMMAND)

        if not self._process or not self.poll():
            msg = "[OMSim] Failed to launch simulator: %s" % _COMMAND
            log.error(msg)
            raise Exception(msg)

        log.debug("[OMSim] process started, pid: %s", self.getpid())

        # give it some time to start up
        sleep(5)

        # get URI:
        uri = None
        with open("logs/rsn_oms_simulator.yml", buffering=1) as f:
            # we expect one of the first few lines to be of the form:
            # rsn_oms_simulator_uri=xxxx
            # where xxxx is the uri -- see oms_simulator_server.
            while uri is None:
                line = f.readline()
                if line.index("rsn_oms_simulator_uri=") == 0:
                    uri = line[len("rsn_oms_simulator_uri="):].strip()

        self._rsn_oms = CIOMSClientFactory.create_instance(uri)
        return self._rsn_oms, uri

    def stop(self):
        """
        Stop the process.
        """
        if self._rsn_oms is not None:
            log.debug("[OMSim] x_exit_simulator -> %r", self._rsn_oms.x_exit_simulator())

        if self._process:
            try:
                log.debug("[OMSim] terminating process %s", self._process.pid)
                self._process.send_signal(signal.SIGINT)
                log.debug("[OMSim] waiting process %s", self._process.pid)
                self._process.wait()
                log.debug("[OMSim] process killed")

            except OSError:
                log.warn("[OMSim] Could not stop process, pid: %s" % self._process.pid)

            sleep(4)

        self._process = None
        self._rsn_oms = None

    def poll(self):
        """
        Check to see if the process is alive.
        @return true if process is running, false otherwise
        """

        # The Popen.poll() doesn't seem to be returning reliable results.  
        # Sending a signal 0 to the process might be more reliable.

        if not self._process:
            return False

        try:
            os.kill(self._process.pid, 0)
        except OSError:
            log.warn("[OMSim] Could not send a signal to the process, pid: %s" % self._process.pid)
            return False

        return True

    def getpid(self):
        """
        Get the pid of the current running process and ensure that it is running.
        @returns the pid of the driver process if it is running, otherwise None
        """
        if self._process:
            if self.poll():
                return self._process.pid
            else:
                log.warn("[OMSim] process found, but poll failed for pid %s",
                         self._process.pid)
        else:
            return None

    def _spawn(self, spawnargs):
        """
        Launch a process using popen
        @param spawnargs a list of arguments for the Popen command line.  
                         The first argument must be a path to a
                         program and arguments much be in additional list elements.
        @return subprocess.Popen object
        """
        return subprocess.Popen(spawnargs, env=os.environ, close_fds=True)


def _test():  # pragma: no cover
    sim_process = ProcessUtil()
    for _ in range(2):
        rsn_oms, uri = sim_process.launch()
        print("rsn_oms_simulator_uri = %s" % uri)
        print("ping -> %r" % rsn_oms.ping())
        sim_process.stop()

# test using nosetest:
#     bin/nosetests -s ion/agents/platform/rsn/simulator/process_util.py
# commented out; this was for preliminary testing.
#
# from pyon.util.int_test import IonIntegrationTestCase
# class BaseIntTestPlatform(IonIntegrationTestCase):  # pragma: no cover
#     def test(self):
#         _test()


# Main program
if __name__ == "__main__":  # pragma: no cover
    _test()
