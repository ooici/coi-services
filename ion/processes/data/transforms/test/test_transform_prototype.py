#!/usr/bin/env python

'''
@brief Test the new transform prototype against streams and events
@author Swarbhanu Chatterjee
'''

from mock import Mock, sentinel, patch
from collections import defaultdict

from pyon.public import log
from pyon.util.containers import DotDict
from pyon.util.file_sys import FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import IonUnitTestCase
from nose.plugins.attrib import attr

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from ion.processes.data.transforms.transform import TransformEventListener, TransformEventPublisher

from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.processes.data.transforms.ctd.ctd_L0_all import ctd_L0_all
from ion.processes.data.transforms.ctd.ctd_L1_conductivity import CTDL1ConductivityTransform
from ion.processes.data.transforms.ctd.ctd_L1_pressure import CTDL1PressureTransform
from ion.processes.data.transforms.ctd.ctd_L1_temperature import CTDL1TemperatureTransform
from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform
from ion.processes.data.transforms.ctd.ctd_L2_density import DensityTransform

@attr('INT', group='dm')
class TransformPrototypeIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(TransformPrototypeIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrc = ResourceRegistryServiceClient()

        self.transform_listener = TransformEventListener()
        self.transform_publisher = TransformEventPublisher()

    def test_event_processing(self):
        '''
        Test that events are processed by the transforms according to a provided algorithm
        '''

        pass

    def test_stream_processing(self):
        '''
        Test that streams are processed by the transforms according to a provided algorithm
        '''


        pass