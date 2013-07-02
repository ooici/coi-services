#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from nose.plugins.attrib import attr
import unittest

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import Container, log, IonObject, RT, PRED
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.context import LocalContextMixin

from ion.services.sa.process.test.test_int_data_process_management_service import global_range_test_document
from ion.util.stored_values import StoredValueManager

from interface.services.sa.idata_acquisition_management_service import IDataAcquisitionManagementService, DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import Attachment, AttachmentType, ReferenceAttachmentContext, DataProduct, InstrumentDevice, Parser

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
class TestIntSimpleDatasetAgent(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.dams = DataAcquisitionManagementServiceClient()
        self.rr = ResourceRegistryServiceClient()

    def tearDown(self):
        pass

    def test_simple_agent(self):
        # Setup resources
        eda_obj = IonObject(RT.ExternalDatasetAgent,
                            name="Demo Agent",
                            poller_module='ion.agents.data.simple_dataset_agent',
                            poller_class='AdditiveSequentialFilePoller',
                            parser_module='ion.agents.data.handlers.sbe52_binary_handler',
                            parser_class='SBE52BinaryCTDParser')
        eda_id = self.dams.create_external_dataset_agent(eda_obj)

        ro = self.rr.read(eda_id)
        self.assertEquals(ro.name, eda_obj.name)

    
        

        edai_obj = IonObject(RT.ExternalDatasetAgentInstance,
                            name="Demo Agent Instance",

                            )
        edai_id = self.dams.create_external_dataset_agent_instance(edai_obj, external_dataset_agent_id=eda_id)

        ro = self.rr.read(edai_id)
        self.assertEquals(ro.name, edai_obj.name)

