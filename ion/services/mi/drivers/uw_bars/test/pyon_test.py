#!/usr/bin/env python

__author__ = 'Carlos Rueda'
__license__ = 'Apache 2.0'

from pyon.util.unit_test import PyonTestCase
from ion.services.mi.drivers.uw_bars.test import BarsTestCase


class PyonBarsTestCase(PyonTestCase, BarsTestCase):
    """
    PyonTestCase base class for BARS test cases.
    """
