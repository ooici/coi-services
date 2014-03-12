#!/usr/bin/env python

__author__ = 'Michael Meisinger'

import datetime
from mock import Mock
from nose.plugins.attrib import attr

from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from pyon.public import RT, PRED, OT, log
from ion.processes.bootstrap.ion_loader import OOI_MAPPING_DOC
from ion.processes.bootstrap.ooi_loader import OOILoader


class TestOOILoader(IonUnitTestCase):

    @attr('UNIT', group='loader')
    def test_ooi_loader(self):
        self.ooi_loader = OOILoader(None,
                                    asset_path='res/preload/r2_ioc/ooi_assets',
                                    mapping_path=OOI_MAPPING_DOC)

        self.ooi_loader.extract_ooi_assets()
        ooiuntil = datetime.datetime.strptime("12/31/2013", "%m/%d/%Y")
        self.ooi_loader.analyze_ooi_assets(ooiuntil)

        # Test report
        self.ooi_loader.report_ooi_assets()
