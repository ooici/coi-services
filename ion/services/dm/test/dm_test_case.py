#!/usr/bin/env python
'''
@author Luke Campbell
@date Thu May  2 13:19:39 EDT 2013
@file ion/services/dm/test/dm_testcase.py
@description Integration Test that verifies instrument streaming through DM
'''

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient

from pyon.public import RT
from interface.objects import DataProduct
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.util.context import LocalContextMixin

from pyon.util.int_test import IonIntegrationTestCase

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

class DMTestCase(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.resource_registry = self.container.resource_registry
        self.RR2 = EnhancedResourceRegistryClient(self.resource_registry)
        self.data_acquisition_management = DataAcquisitionManagementServiceClient()
        self.pubsub_management =  PubsubManagementServiceClient()
        self.instrument_management = InstrumentManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.dataset_management =  DatasetManagementServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.data_process_management = DataProcessManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.data_retriever = DataRetrieverServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.user_notification = UserNotificationServiceClient()

    def create_stream_definition(self, *args, **kwargs):
        stream_def_id = self.pubsub_management.create_stream_definition(*args, **kwargs)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        return stream_def_id

    def create_data_product(self,name, stream_def_id='', param_dict_name='', pdict_id=''):
        if not (stream_def_id or param_dict_name or pdict_id):
            raise AssertionError('Attempted to create a Data Product without a parameter dictionary')

        tdom, sdom = time_series_domain()

        dp = DataProduct(name=name,
                spatial_domain = sdom.dump(),
                temporal_domain = tdom.dump(),
                )

        stream_def_id = stream_def_id or self.create_stream_definition('%s stream def' % name, 
                parameter_dictionary_id=pdict_id or self.RR2.find_resource_by_name(RT.ParameterDictionary, param_dict_name))

        data_product_id = self.data_product_management.create_data_product(dp, stream_definition_id=stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)
        return data_product_id

