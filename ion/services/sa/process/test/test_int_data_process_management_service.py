#!/usr/bin/env python

"""
@file ion/services/sa/process/test/test_int_data_process_management_service.py
@author Maurice Manning
@test ion.services.sa.process.DataProcessManagementService integration test
"""

import time
import numpy as np
from gevent.event import Event
from nose.plugins.attrib import attr
from mock import patch
import gevent
from sets import Set
import unittest
import os

from pyon.util.int_test import IonIntegrationTestCase
from pyon.ion.event import EventPublisher
from pyon.public import LCS
from pyon.public import log, IonObject
from pyon.public import CFG, RT, PRED, OT
from pyon.core.exception import BadRequest, NotFound
from pyon.util.context import LocalContextMixin
from pyon.util.poller import poll
from pyon.util.containers import DotDict
from pyon.ion.stream import StandaloneStreamPublisher, StandaloneStreamSubscriber

from ion.services.dm.utility.granule_utils import time_series_domain
from ion.util.stored_values import StoredValueManager
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.agents.port.port_agent_process import PortAgentProcessType, PortAgentType
from ion.services.cei.process_dispatcher_service import ProcessStateGate
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.utility.test.parameter_helper import ParameterHelper

from coverage_model import ParameterContext, QuantityType, NumexprFunction, ParameterFunctionType

from interface.objects import ProcessStateEnum, TransformFunction, TransformFunctionType, DataProcessDefinition, DataProcessTypeEnum, AgentCommand, Parser, ParameterFunction
from interface.objects import LastUpdate, ComputedValueAvailability, DataProduct, DataProducer, DataProcessProducerContext, Attachment, AttachmentType, ReferenceAttachmentContext
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.coi.iresource_management_service import ResourceManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
DRV_URI_GOOD = CFG.device.sbe37.dvr_egg

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
class TestIntDataProcessManagementServiceMultiOut(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient()
        self.damsclient = DataAcquisitionManagementServiceClient()
        self.pubsubclient =  PubsubManagementServiceClient()
        self.ingestclient = IngestionManagementServiceClient()
        self.imsclient = InstrumentManagementServiceClient()
        self.dataproductclient = DataProductManagementServiceClient()
        self.dataprocessclient = DataProcessManagementServiceClient()
        self.datasetclient =  DatasetManagementServiceClient()
        self.dataset_management = self.datasetclient
        self.process_dispatcher = ProcessDispatcherServiceClient()

    def test_create_data_process_definition(self):
        pfunc = ParameterFunction(name='test_func')
        func_id = self.dataset_management.create_parameter_function(pfunc)

        data_process_definition = DataProcessDefinition()
        data_process_definition.name = 'Simple'

        dpd_id = self.dataprocessclient.create_data_process_definition(data_process_definition, func_id)
        self.addCleanup(self.dataprocessclient.delete_data_process_definition, dpd_id)
        self.addCleanup(self.dataset_management.delete_parameter_function, func_id)

        objs, _ = self.rrclient.find_objects(dpd_id, PRED.hasParameterFunction, id_only=False)
        self.assertEquals(len(objs), 1)
        self.assertIsInstance(objs[0], ParameterFunction)



global_range_test_document = '''Array,Instrument Class,Reference Designator,Data Products,Units,Data Product Flagged,Minimum Range (lim(1)),Maximum Range (lim(2))
Array 1,SBE37,TEST,TEMPWAT,deg_C,TEMPWAT,10,28
Array 1,SBE37,TEST,CONDWAT,S m-1,CONDWAT,0,30
Array 1,SBE37,TEST,PRESWAT,dbar,PRESWAT,0,20'''
