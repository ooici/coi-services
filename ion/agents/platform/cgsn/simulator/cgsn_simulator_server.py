#!/usr/bin/env python

"""
@package ion.agents.platform.cgsn.simulator.cgsn_simulator_server
@file    ion/agents/platform/cgsn/simulator/cgsn_simulator_server.py
@author  Carlos Rueda
@brief   CGSN simulator server. Program intended to be run outside of pyon.

 USAGE:
    $ bin/python ion/agents/platform/cgsn/simulator/cgsn_simulator_server.py
    CgsnSimulatorServer listening on localhost:2221 ...
    started thread.
    Listening on localhost:2221
    Enter ^D to exit
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

import sys
from ion.agents.platform.cgsn.simulator.cgsn_simulator import CgsnSimulator


class CgsnSimulatorServer(object):
    """
    CGSN simulator server
    """

    def __init__(self, address, thread=False):
        """
        @param address  (host,port) for the service
        @param thread If True, a thread is launched to call serve_forever on
                      the simulator. Do not use this within a pyon-dominated
                      environment because of gevent monkey patching
                      behavior that doesn't play well with threading.Thread
                      (you'll likely see the thread blocked). By default False.
        """
        self._sim = CgsnSimulator(address)
        print("CgsnSimulatorServer listening on %s:%s ..." % (host, port))
        if thread:
            self._check_pyon()
            from threading import Thread
            runnable = Thread(target=self._sim.serve_forever)
            runnable.start()
            print("started thread.")
        else:
            self._sim.serve_forever()

    def shutdown(self):
        print("_sim.shutdown called.")
        if self._sim:
            self._sim.shutdown()
            self._sim = None

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


if "__main__" == __name__:   # pragma: no cover
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 2221

    import argparse
    import sys

    parser = argparse.ArgumentParser(description="CGSN Simulator server")
    parser.add_argument("-H", "--host",
                        help="host (default: %s)" % DEFAULT_HOST,
                        default=DEFAULT_HOST)
    parser.add_argument("-P", "--port",
                        help="port (default: %s)" % DEFAULT_PORT,
                        default=DEFAULT_PORT)

    opts = parser.parse_args()

    host = opts.host
    port = int(opts.port)
    address = (host, port)

    css = CgsnSimulatorServer(address, thread=True)

    print("Listening on %s:%s" % address)
    print("Enter ^D to exit")
    try:
        sys.stdin.read()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        # likely not associated with a terminal
        pass
    print("\nExiting")
    css.shutdown()
