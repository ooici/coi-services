#!/usr/bin/env python

'''
@brief Test to check CTD
@author Michael Meisinger
'''

from mock import Mock, sentinel, patch

from pyon.util.containers import DotDict
from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr

from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.processes.data.transforms.ctd.ctd_L0_all import ctd_L0_all
from ion.processes.data.transforms.ctd.ctd_L1_conductivity import CTDL1ConductivityTransform
from ion.processes.data.transforms.ctd.ctd_L1_pressure import CTDL1PressureTransform
from ion.processes.data.transforms.ctd.ctd_L1_temperature import CTDL1TemperatureTransform


@attr('UNIT', group='ctd')
class TestScienceObjectCodec(IonUnitTestCase):
    pass

    def setUp(self):
        self.px_ctd = SimpleCtdPublisher()
        self.px_ctd.last_time = 0

        self.tx_L0 = ctd_L0_all()
        self.tx_L0.conductivity = Mock()
        self.tx_L0.pressure = Mock()
        self.tx_L0.temperature = Mock()

        self.tx_L1_C = CTDL1ConductivityTransform()
        self.tx_L1_T = CTDL1TemperatureTransform()
        self.tx_L1_P = CTDL1PressureTransform()

    def test_transforms(self):

        length = 1
        packet = self.px_ctd._get_ctd_packet("STR_ID", length)

        self.tx_L0.process(packet)

        log.info("Args: %s" & self.tx_L0.conductivity.publish.call_args[0][0])
