#!/usr/bin/env python

"""
@package ion.agents.platform.util.test.test_util
@file    ion/agents/platform/util/test/test_util.py
@author  Carlos Rueda
@brief   Test cases for some utilities.
"""

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'


from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr

from pyon.util.containers import get_ion_ts

from ion.agents.platform.util import ion_ts_2_ntp, ntp_2_ion_ts


@attr('UNIT', group='sa')
class Test(IonUnitTestCase):

    def test_ion_ts_and_ntp_conversion(self):
        sys_time = get_ion_ts()
        ntp_time = ion_ts_2_ntp(sys_time)
        sys2_time = ntp_2_ion_ts(ntp_time)
        self.assertEquals(sys_time, sys2_time)
