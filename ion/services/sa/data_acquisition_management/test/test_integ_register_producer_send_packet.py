#!/usr/bin/env python

'''
@file ion/services/sa/instrument_management/test/test_test_integ_register_producer_send_packet.py
@author Maurice Manning
@test ion.services.sa.instrument_management.instrument_management_service integration test
'''


from pyon.util.unit_test import PyonTestCase
from ion.services.sa.instrument_management.instrument_management_service import InstrumentManagementService
from ion.services.sa.data_acquisition_management.data_acquisition_management_service import DataAcquisitionManagementService
from nose.plugins.attrib import attr
from pyon.public import AT, RT
from pyon.public import Container

from pyon.core.exception import BadRequest, Conflict, Inconsistent, NotFound
import unittest

@attr('INT')
class Test_Bank(IonIntegrationTestCase):

    def test_register_and_send(self):
         # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ProcessRPCClient(node=self.container.node, name=self.container.name, iface=IContainerAgent, process=FakeProcess())
        container_client.start_rel_from_url('res/deploy/examples/r2deploy.yml')

        # Now create client to the data acquisition service
        client = ProcessRPCClient(node=self.container.node, name="DAMS", iface=IDataAcquisitionManagementService, process=FakeProcess())

        # Send some requests
        print 'Resister this data producer'
  