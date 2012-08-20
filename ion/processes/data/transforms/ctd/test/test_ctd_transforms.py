#!/usr/bin/env python

'''
@brief Test to check CTD
@author Michael Meisinger
'''

from mock import Mock, sentinel, patch, mocksignature
from collections import defaultdict

from pyon.public import log
from pyon.util.containers import DotDict
from pyon.util.file_sys import FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr

from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.processes.data.transforms.ctd.ctd_L0_all import ctd_L0_all
from ion.processes.data.transforms.ctd.ctd_L1_conductivity import CTDL1ConductivityTransform
from ion.processes.data.transforms.ctd.ctd_L1_pressure import CTDL1PressureTransform
from ion.processes.data.transforms.ctd.ctd_L1_temperature import CTDL1TemperatureTransform
from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform
from ion.processes.data.transforms.ctd.ctd_L2_density import DensityTransform
import unittest, os

@attr('UNIT', group='ctd')
@unittest.skip('Not working')
class TestScienceObjectCodec(IonUnitTestCase):
    pass

    def setUp(self):
        # This test does not start a container so we have to hack creating a FileSystem singleton instance
#        FileSystem(DotDict())

        self.px_ctd = SimpleCtdPublisher()
        self.px_ctd.last_time = 0

        self.tx_L0 = ctd_L0_all()
        self.tx_L0.streams = defaultdict(Mock)
        self.tx_L0.cond_publisher = Mock()
        self.tx_L0.temp_publisher = Mock()
        self.tx_L0.pres_publisher = Mock()

        self.tx_L1_C = CTDL1ConductivityTransform()
        self.tx_L1_C.streams = defaultdict(Mock)

        self.tx_L1_T = CTDL1TemperatureTransform()
        self.tx_L1_T.streams = defaultdict(Mock)

        self.tx_L1_P = CTDL1PressureTransform()
        self.tx_L1_P.streams = defaultdict(Mock)

        self.tx_L2_S = SalinityTransform()
        self.tx_L2_S.streams = defaultdict(Mock)

        self.tx_L2_D = DensityTransform()
        self.tx_L2_D.streams = defaultdict(Mock)

    def test_transforms(self):

        length = 1

        packet = self.px_ctd._get_new_ctd_packet("STR_ID", length)

        log.info("Packet: %s" % packet)

        self.tx_L0.process(packet)

        self.tx_L0.cond_publisher.publish = mocksignature(self.tx_L0.cond_publisher.publish)
        self.tx_L0.cond_publisher.publish.return_value = ''

        self.tx_L0.temp_publisher.publish = mocksignature(self.tx_L0.cond_publisher.publish)
        self.tx_L0.temp_publisher.publish.return_value = ''

        self.tx_L0.pres_publisher.publish = mocksignature(self.tx_L0.cond_publisher.publish)
        self.tx_L0.pres_publisher.publish.return_value = ''

        L0_cond = self.tx_L0.cond_publisher.publish.call_args[0][0]
        L0_temp = self.tx_L0.temp_publisher.publish.call_args[0][0]
        L0_pres = self.tx_L0.pres_publisher.publish.call_args[0][0]

        log.info("L0 cond: %s" % L0_cond)
        log.info("L0 temp: %s" % L0_temp)
        log.info("L0 pres: %s" % L0_pres)

        L1_cond = self.tx_L1_C.execute(L0_cond)
        log.info("L1 cond: %s" % L1_cond)

        L1_temp = self.tx_L1_T.execute(L0_temp)
        log.info("L1 temp: %s" % L1_temp)

        L1_pres = self.tx_L1_P.execute(L0_pres)
        log.info("L1 pres: %s" % L1_pres)

        L2_sal = self.tx_L2_S.execute(packet)
        log.info("L2 sal: %s" % L2_sal)

        L2_dens = self.tx_L2_D.execute(packet)
        log.info("L2 dens: %s" % L2_dens)

@attr('INT', group='dm')
class ScienceObjectCodecIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(ScienceObjectCodecIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.px_ctd = SimpleCtdPublisher()
        self.px_ctd.last_time = 0

        self.tx_L0 = ctd_L0_all()

        self.cond_L1 = CTDL1ConductivityTransform()

        self.pres_L1 = CTDL1PressureTransform()

        self.temp_L1 = CTDL1TemperatureTransform()

        self.dens_L2 = DensityTransform()

        self.sal_L2 = SalinityTransform()


    @unittest.skip('This version of L0 Transforms are deprecated this test needs to be rewritten')
    def test_process(self):
        '''
        Test that packets are processed by the ctd_L0_all transform
        '''
        length = 1

        packet = self.px_ctd._get_new_ctd_packet("STR_ID", length)
        log.info("Packet: %s" % packet)

        self.tx_L0.process(packet)

    @unittest.skip('write it later')
    def test_execute(self):
        '''
        Test that the other transforms (temperature, press, density) execute correctly
        '''

        pass

#        self.cond_L1.execute(granule=g)
#
#        self.pres_L1.execute(granule=g)
#
#        self.temp_L1.execute(granule=g)
#
#        self.dens_L2.execute(granule=g)
#
#        self.sal_L2.execute(granule=g)

