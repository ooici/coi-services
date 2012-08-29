#!/usr/bin/env python

"""
@package ion.agents.platform.oms.simulator.oms_simulator_server
@file    ion/agents/platform/oms/simulator/oms_simulator_server.py
@author  Carlos Rueda
@brief   OMS simulator XML/RPC server for testing purposes.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from ion.agents.platform.oms.simulator.oms_simulator import OmsSimulator
from SimpleXMLRPCServer import SimpleXMLRPCServer
from threading import Thread


import logging

log = logging.getLogger('oms_simulator')
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)-10s %(module)-25s %(funcName)s %(lineno)-4d %(process)-6d %(threadName)-15s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


class OmsSimulatorServer(object):
    """
    Dispatches an OmsSimulator with a SimpleXMLRPCServer.
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
        self._sim = OmsSimulator()
        self._server = SimpleXMLRPCServer((host, port), allow_none=True)
        self._server.register_introspection_functions()
        self._server.register_instance(self._sim, allow_dotted_names=True)
        log.info("OMS simulator xmlrpc server listening on %s:%s ..." % (host, port))
        if thread:
            self._check_pyon()
            runnable = Thread(target=self._server.serve_forever)
            runnable.start()
            log.info("started thread.")
        else:
            self._server.serve_forever()

    @property
    def oms_simulator(self):
        return self._sim

    def shutdown(self):
        log.info("_server.shutdown called.")
        if self._sim:
            self._sim = None
            self._server.shutdown()
            self._server = None

    @staticmethod
    def _check_pyon():
        """
        Prints a warning message if pyon is detected.
        """
        import sys
        if 'pyon' in sys.modules:
            m = "!! WARNING: pyon in sys.modules !!"
            s = "!" * len(m)
            sys.stderr.write("\n%s\n%s\n%s\n\n" % (s, m, s))


DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 7700

# Main program
if __name__ == "__main__":
    import argparse
    import sys

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

    oss = OmsSimulatorServer(host, port, thread=True)
    sim = oss.oms_simulator

    log.info("network.dump():\n   |%s" % sim.dump().replace('\n', '\n   |'))
    log.info("network.get_map() = %s\n" % sim.config.getPlatformMap())

    log.info("Enter ^D to exit")
    try:
        sys.stdin.read()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        # likely not associated with a terminal
        pass
    log.info("\nExiting")
    oss.shutdown()
