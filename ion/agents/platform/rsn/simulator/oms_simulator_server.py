#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.oms_simulator_server
@file    ion/agents/platform/rsn/simulator/oms_simulator_server.py
@author  Carlos Rueda
@brief   OMS simulator XML/RPC server. Program intended to be run outside of
         pyon.

 USAGE:
    $ bin/python ion/agents/platform/rsn/simulator/oms_simulator_server.py
    ...
    2012-09-27 21:15:51,335 INFO     MainThread oms_simulator  :107 <module> Listening on localhost:7700
    2012-09-27 21:15:51,335 INFO     MainThread oms_simulator  :108 <module> Enter ^D to exit

"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.rsn.simulator.logger import Logger
log = Logger.get_logger()

import logging

from ion.agents.platform.rsn.simulator.oms_simulator import CIOMSSimulator
from ion.agents.platform.util.network_util import NetworkUtil
from SimpleXMLRPCServer import SimpleXMLRPCServer
from threading import Thread
import time
import sys


class CIOMSSimulatorWithExit(CIOMSSimulator):
    """
    Adds some special methods for coordination from integration tests:

    x_exit_simulator:  to exit the simulator process.

    x_exit_inactivity: to make the simulator shutdown itself after a period of
                       inactivity.
    """

    def __init__(self, oss):
        """
        @param oss   CIOMSSimulatorServer
        """
        CIOMSSimulator.__init__(self)
        self._oss = oss

        # for inactivity checking
        self._last_activity = time.time()
        self._inactivity_period = None

    def x_exit_simulator(self):
        log.info("x_exit_simulator called. event_generator=%s; %s listeners registered",
                 self._event_generator, len(self._reg_event_listeners))
        if self._event_generator:
            self._event_generator.stop()
            self._event_generator = None

        def call_exit():
            self._oss.shutdown_server()
            time.sleep(4)
            quit()

        Thread(target=call_exit).start()
        return "Will exit in a couple of secs"

    def x_exit_inactivity(self, inactivity_period):
        if self._inactivity_period is None:
            # first call.
            self._inactivity_period = inactivity_period
            check_idle = Thread(target=self._check_inactivity)
            check_idle.setDaemon(True)
            check_idle.start()
            log.info("started check_inactivity thread: inactivity_period=%.1f",
                     inactivity_period)
        else:
            self._inactivity_period = inactivity_period
            log.info("updated inactivity_period=%.1f", inactivity_period)

    def _enter(self):
        self._last_activity = time.time()
        super(CIOMSSimulatorWithExit, self)._enter()

    def _check_inactivity(self):
        ip = self._inactivity_period
        warn_per = 30 if ip >= 60 else 10 if ip >= 30 else 5
        while self._oss._running:
            inactive = time.time() - self._last_activity

            if inactive >= self._inactivity_period:
                log.warn("%.1f secs of inactivity. Exiting...", inactive)
                self._oss.shutdown_server()
                quit()

            elif inactive >= warn_per and int(inactive) % warn_per == 0:
                log.warn("%.1f secs of inactivity", inactive)

            time.sleep(0.5)


class CIOMSSimulatorServer(object):
    """
    Dispatches an CIOMSSimulator with a SimpleXMLRPCServer.
    Intended to be run outside of pyon.
    """

    def __init__(self, host, port, inactivity_period=None):
        """
        Creates a SimpleXMLRPCServer and starts a Thread where serve_forever
        is called on the server.

        @param host   Hostname for the service
        @param port   Port for the service
        """
        self._running = True
        self._sim = CIOMSSimulatorWithExit(self)

        if log.isEnabledFor(logging.DEBUG):
            ser = NetworkUtil.serialize_network_definition(self._sim._ndef)
            log.debug("network serialization:\n   %s" % ser.replace('\n', '\n   '))
            log.debug("network.get_map() = %s\n" % self._sim.config.get_platform_map())

        self._server = SimpleXMLRPCServer((host, port), allow_none=True)

        actual_port = self._server.socket.getsockname()[1]
        uri = "http://%s:%s/" % (host, actual_port)

        # write the URI to a file for launching process to see it:
        with open("logs/rsn_oms_simulator.yml", 'w') as f:
            f.write("rsn_oms_simulator_uri=%s\n" % uri)

        self._server.register_introspection_functions()
        self._server.register_instance(self._sim, allow_dotted_names=True)

        log.info("Methods:\n\t%s", "\n\t".join(self._server.system_listMethods()))

        self._check_pyon()

        runnable = Thread(target=self._server.serve_forever)
        runnable.setDaemon(True)
        runnable.start()

        log.info("OMS simulator xmlrpc server listening on %s" % uri)

        if inactivity_period:
            self._sim.x_exit_inactivity(inactivity_period)

    def shutdown_server(self):
        self._running = False
        if self._sim:
            log.info("RSN OMS simulator exiting...")
            self._sim = None
            self._server.shutdown()
            self._server = None

    def wait_until_shutdown(self):
        while self._running:
            time.sleep(1)

        self.shutdown_server()

    @staticmethod
    def _check_pyon():
        """
        Prints a warning message if pyon is detected.
        """
        if 'pyon' in sys.modules:
            m = "!! WARNING: pyon in sys.modules !!"
            s = "!" * len(m)
            sys.stderr.write("\n%s\n%s\n%s\n\n" % (s, m, s))


# Main program
if __name__ == "__main__":  # pragma: no cover

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 7700

    import argparse
    import signal

    parser = argparse.ArgumentParser(description="OMS Simulator server")
    parser.add_argument("-H", "--host",
                        help="host (default: %s)" % DEFAULT_HOST,
                        default=DEFAULT_HOST)
    parser.add_argument("-P", "--port",
                        help="port (default: %s)" % DEFAULT_PORT,
                        default=DEFAULT_PORT)
    parser.add_argument("-i", "--inactivity",
                        help="inactivity period check in secs (no check by default)")

    opts = parser.parse_args()

    host = opts.host
    port = int(opts.port)
    inactivity_period = int(opts.inactivity) if opts.inactivity else None

    oss = CIOMSSimulatorServer(host, port, inactivity_period)

    def handler(signum, frame):
        log.info("\n--SIGINT--")
        oss.shutdown_server()
        quit()

    signal.signal(signal.SIGINT, handler)

    oss.wait_until_shutdown()
