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

from ion.agents.platform.rsn.simulator.oms_simulator import CIOMSSimulator
from ion.agents.platform.util.network_util import NetworkUtil
from SimpleXMLRPCServer import SimpleXMLRPCServer
from threading import Thread
import time
import sys


class CIOMSSimulatorWithExit(CIOMSSimulator):
    """
    Adds a special method only intended as an alternative mechanism to exit the
    simulator as opposed to just via a signal to the process.
    """

    def __init__(self, oss):
        CIOMSSimulator.__init__(self)
        self._oss = oss

    def exit_simulator(self):
        log.info("exit_simulator called. event_generator=%s; %s listeners registered",
                 self._event_generator, len(self._reg_event_listeners))
        if self._event_generator:
            self._event_generator.stop()
            self._event_generator = None

        def call_exit():
            time.sleep(4)
            self._oss.shutdown()
            quit()

        Thread(target=call_exit).start()
        return "Will exit in a couple of secs"


class CIOMSSimulatorServer(object):
    """
    Dispatches an CIOMSSimulator with a SimpleXMLRPCServer. Normally,
    this is intended to be run outside of pyon.
    """

    def __init__(self, host, port, thread=False):
        """
        @param host   Hostname for the service
        @param port   Port for the service
        @param thread If True, a thread is launched to call serve_forever on
                      the server. Do not use this within a pyon-dominated
                      environment because of gevent monkey patching
                      behavior that doesn't play well with threading.Thread
                      (you'll likely see the thread blocked). By default False.
        """
        self._sim = CIOMSSimulatorWithExit(self)
        self._server = SimpleXMLRPCServer((host, port), allow_none=True)
        self._server.register_introspection_functions()
        self._server.register_instance(self._sim, allow_dotted_names=True)
        log.info("OMS simulator xmlrpc server listening on %s:%s ..." % (host, port))
        if thread:
            self._check_pyon()
            runnable = Thread(target=self._server.serve_forever)
            runnable.setDaemon(True)
            runnable.start()
            log.info("started thread.")
            self._running = True
        else:
            self._server.serve_forever()

    @property
    def methods(self):
        return self._server.system_listMethods()

    @property
    def oms_simulator(self):
        return self._sim

    def shutdown(self):
        log.info("RNS OMS simulator exiting...")
        self._running = False
        if self._sim:
            self._sim = None
            self._server.shutdown()
            self._server = None

    def wait_until_shutdown(self):
        while self._running:
            time.sleep(1)

        oss.shutdown()

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

    opts = parser.parse_args()

    host = opts.host
    port = int(opts.port)

    oss = CIOMSSimulatorServer(host, port, thread=True)
    sim = oss.oms_simulator

    def handler(signum, frame):
        log.info("\n--SIGINT--")
        oss.shutdown()
        quit()

    signal.signal(signal.SIGINT, handler)

    ser = NetworkUtil.serialize_network_definition(sim._ndef)
    log.info("network serialization:\n   %s" % ser.replace('\n', '\n   '))
    log.info("network.get_map() = %s\n" % sim.config.get_platform_map())

    log.info("Methods:\n\t%s", "\n\t".join(oss.methods))

    log.info("Listening on %s:%s", host, port)

    oss.wait_until_shutdown()
