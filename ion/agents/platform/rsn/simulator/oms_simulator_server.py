#!/usr/bin/env python

"""
@package ion.agents.platform.rsn.simulator.oms_simulator_server
@file    ion/agents/platform/rsn/simulator/oms_simulator_server.py
@author  Carlos Rueda
@brief   OMS simulator XML/RPC server. Program intended to be run outside of pyon.

 USAGE:
    $ bin/python ion/agents/platform/rsn/simulator/oms_simulator_server.py
    oms_simulator: setting log level to: logging.WARN

 Run with --help for usage options.

 Define environment variable oms_simulator_loglevel to set logging level, for example:
    $ oms_simulator_loglevel=INFO bin/python ion/agents/platform/rsn/simulator/oms_simulator_server.py
    oms_simulator: setting log level to: logging.INFO
    2014-06-16 14:49:02,228 INFO     MainThread oms_simulator  :145 __init__ Definition file: ion/agents/platform/rsn/simulator/network.yml
    2014-06-16 14:49:02,228 INFO     MainThread oms_simulator  :146 __init__ Events file:     ion/agents/platform/rsn/simulator/events.yml
    2014-06-16 14:49:02,439 INFO     MainThread oms_simulator  :167 __init__ Methods: ['generate_test_event', 'get_platform_attribute_values', 'get_platform_metadata', 'get_platform_ports', 'get_registered_event_listeners', 'ping', 'register_event_listener', 'set_over_current', 'system.listMethods', 'system.methodHelp', 'system.methodSignature', 'turn_off_platform_port', 'turn_on_platform_port', 'unregister_event_listener', 'x_disable', 'x_enable', 'x_exit_inactivity', 'x_exit_simulator']
    2014-06-16 14:49:02,439 INFO     MainThread oms_simulator  :175 __init__ OMS simulator xmlrpc server listening on http://localhost:7700/
"""

__author__ = 'Carlos Rueda'


import logging
from threading import Thread
import time
import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer

from ion.agents.platform.rsn.simulator.logger import Logger
log = Logger.get_logger()

from ion.agents.platform.rsn.simulator.oms_simulator import CIOMSSimulator
from ion.agents.platform.util.network_util import NetworkUtil


class CIOMSSimulatorWithExit(CIOMSSimulator):
    """
    Adds some special methods for coordination from integration tests:

    x_exit_simulator:  to exit the simulator process.

    x_exit_inactivity: to make the simulator shutdown itself after a period of
                       inactivity.
    """

    def __init__(self, oss, def_file, events_file):
        """
        @param oss   CIOMSSimulatorServer
        @param def_file      Platform network definition file
        @param events_file   File with events to be published
        """
        CIOMSSimulator.__init__(self, def_file, events_file)
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

    def __init__(self, def_file, events_file, host, port, inactivity_period):
        """
        Creates a SimpleXMLRPCServer and starts a Thread where serve_forever
        is called on the server.

        @param def_file      Platform network definition file
        @param events_file   File with events to be published
        @param host   Hostname for the service
        @param port   Port for the service
        """
        log.info("Definition file: %s", def_file)
        log.info("Events file:     %s", events_file)

        self._running = True
        self._sim = CIOMSSimulatorWithExit(self, def_file, events_file)

        if log.isEnabledFor(logging.DEBUG):
            ser = NetworkUtil.serialize_network_definition(self._sim._ndef)
            log.debug("network serialization:\n   %s" % ser.replace('\n', '\n   '))

        self._server = SimpleXMLRPCServer((host, port), allow_none=True)

        actual_port = self._server.socket.getsockname()[1]
        uri = "http://%s:%s/" % (host, actual_port)

        # write the URI to a file for launching process to see it:
        with open("logs/rsn_oms_simulator.yml", 'w') as f:
            f.write("rsn_oms_simulator_uri=%s\n" % uri)

        self._server.register_introspection_functions()
        self._server.register_instance(self._sim, allow_dotted_names=True)

        log.info("Methods: %s", self._server.system_listMethods())

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
    DEFAULT_DEF_FILE = 'ion/agents/platform/rsn/simulator/network.yml'
    DEFAULT_EVENT_FILE = 'ion/agents/platform/rsn/simulator/events.yml'

    import argparse
    import signal

    parser = argparse.ArgumentParser(description="OMS simulator XML/RPC server")
    parser.add_argument("-d", "--definition",
                        help="platform network definition file (default: %s)" % DEFAULT_DEF_FILE,
                        default=DEFAULT_DEF_FILE)
    parser.add_argument("-e", "--events",
                        help="file with events to be published (default: %s)" % DEFAULT_EVENT_FILE,
                        default=DEFAULT_EVENT_FILE)
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
    def_file = opts.definition
    events_file = opts.events

    oss = CIOMSSimulatorServer(def_file, events_file, host, port, inactivity_period)

    def handler(signum, frame):
        log.info("\n--SIGINT--")
        oss.shutdown_server()
        quit()

    signal.signal(signal.SIGINT, handler)

    oss.wait_until_shutdown()
