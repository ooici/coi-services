#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/utility/test/test_coverage_craft.py
@date Thu Jul 19 16:44:05 EDT 2012
@description Utilities for crafting granules into a coverage
'''

from pyon.util.unit_test import PyonTestCase
from ion.services.dm.utility.granule_utils import CoverageCraft, RecordDictionaryTool, build_granule, TaxyTool 
from nose.plugins.attrib import attr

import numpy as np

@attr('UNIT')
class CoverageCraftUnitTest(PyonTestCase):

    def sample_granule(self):
        rdt = RecordDictionaryTool(CoverageCraft.tx)
        rdt['time'] = np.arange(20)
        rdt['temp'] = np.array([5] * 20)
        rdt['conductivity'] = np.array([10] * 20)
        rdt['lat'] = np.array([0] * 20)
        rdt['lon'] = np.array([0] * 20)
        rdt['depth'] = np.array([0] * 20)
        rdt['data'] = np.array([0x01] * 20)

        return build_granule('sample', CoverageCraft.tx, rdt)

    def test_to_coverage(self):
        granule = self.sample_granule()
        crafter = CoverageCraft(granule=granule)
        crafter.add_granule()

        coverage = crafter.coverage
        time_vals = coverage.get_time_values()
        comp = time_vals == np.arange(20)
        self.assertTrue(comp.all())

