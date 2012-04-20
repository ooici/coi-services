#!/usr/bin/env python

__author__ = "Carlos Rueda"
__license__ = 'Apache 2.0'

"""
Update 2012-02-16: The behavior described below is not happening anymore; the
test is now running fine with 'import pyon.util.unit_test' and under the
various launch styles.

(the description of the original behavior follows)
If pyon.util.unit_test is imported, then the thread in this test is NOT
started when this file is run via bin/nosetests or unittest. But it does
run fine when launched (individually) as a regular program via bin/python.
Apparently the pyon.util.unit_test import triggers some internal preparations
in pyon that don't play well with threads (likely related with gevent
monkey-patching).

Just remove the pyon.util.unit_test import, and the test runs fine with any
of the launch methods.

The following two require a ^\ to kill them (the thread is not started).

$ bin/nosetests -sv ion/services/mi/drivers/uw_bars/test/test_thread.py
2012-02-10 13:39:53,238 DEBUG    pyon.core.governance.governance_controller GovernanceController.__init__()
2012-02-10 13:39:53,238 DEBUG    pyon.core.governance.governance_interceptor GovernanceInterceptor enabled: False
test_simple (ion.services.mi.drivers.uw_bars.test.test_thread.ThreadTest) ... :: _T created
^\

$ bin/python -m unittest ion.services.mi.drivers.uw_bars.test.test_thread
test_simple (ion.services.mi.drivers.uw_bars.test.test_bars_client.DriverTest) ...
DEBUG      bars_client               20742  MainThread      - ### connecting to 10.180.80.172:2001
DEBUG      bars_client               20742  MainThread      - ### connected to 10.180.80.172:2001
DEBUG      bars_client               20742  MainThread      - ### _Recv created.
^\


The following runs fine:
$ bin/python  ion/services/mi/drivers/uw_bars/test/test_thread.py
2012-02-10 13:42:32,860 DEBUG    pyon.core.governance.governance_controller GovernanceController.__init__()
2012-02-10 13:42:32,860 DEBUG    pyon.core.governance.governance_interceptor GovernanceInterceptor enabled: False
:: _T created
:: _T statting to run
:: _T running ...
:: thread started
:: sleeping
:: _T running ...
:: _T running ...
:: ending thread


"""

# uncomment to see the described behaviour
#import pyon.util.unit_test

import unittest
from threading import Thread
import time
from nose.plugins.attrib import attr
import os


#@unittest.skipIf(None == os.getenv('run_it'), 'define run_it to run this.')
@attr('UNIT', group='mi')
@unittest.skip('Need to align.')
class _T(Thread):
    def __init__(self):
        Thread.__init__(self, name="_T")
        self._active = True
        print ":: _T created"

    def end(self):
        self._active = False

    def run(self):
        print ":: _T statting to run"
        while self._active:
            print ":: _T running ..."
            time.sleep(0.5)


class ThreadTest(unittest.TestCase):
    def setUp(self):
        self.thread = _T()
        self.thread.start()
        print ":: thread started"

    def tearDown(self):
        print ":: ending thread"
        self.thread.end()
        self.thread.join()

    def test_simple(self):
        print ":: sleeping"
        time.sleep(2)

if __name__ == '__main__':
    t = ThreadTest('test_simple')
    t.run()
