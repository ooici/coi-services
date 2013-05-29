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
from interface.services.ans.iworkflow_management_service import WorkflowManagementServiceClient
from interface.services.ans.ivisualization_service import VisualizationServiceClient

from pyon.public import RT
from interface.objects import DataProduct
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.util.context import LocalContextMixin
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.breakpoint import breakpoint
from pyon.container.cc import Container
from ion.services.dm.utility.test.parameter_helper import ParameterHelper

import numpy as np
import time
import gevent
from gevent.event import Event

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
        self.workflow_management = WorkflowManagementServiceClient()
        self.visualization = VisualizationServiceClient()

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
                parameter_dictionary_id=pdict_id or self.RR2.find_resource_by_name(RT.ParameterDictionary,
                    param_dict_name, id_only=True))

        data_product_id = self.data_product_management.create_data_product(dp, stream_definition_id=stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)
        return data_product_id

    def activate_data_product(self, data_product_id):
        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)

class Streamer(object):
    def __init__(self, data_product_id, interval=1):
        self.resource_registry = Container.instance.resource_registry
        self.pubsub_management = PubsubManagementServiceClient()
        self.data_product_id = data_product_id
        self.i=0
        self.interval = interval
        self.finished = Event()
        self.g = gevent.spawn(self.run)

    def run(self):
        while not self.finished.wait(self.interval):
            gevent.sleep(self.interval)
            rdt = ParameterHelper.rdt_for_data_product(self.data_product_id)
            now = time.time()
            rdt['time'] = np.array([now + 2208988800])
            rdt['temp'] = self.float_range(10,14,np.array([now]))
            rdt['pressure'] = self.float_range(11,12,np.array([now]))
            rdt['lat'] = [41.205]
            rdt['lon'] = [-71.74]
            rdt['conductivity'] = self.float_range(3.3,3.5,np.array([now]))
            rdt['driver_timestamp'] = np.array([now + 2208988800])
            rdt['preferred_timestamp'] = ['driver_timestamp']
            ParameterHelper.publish_rdt_to_data_product(self.data_product_id, rdt)
            self.i += 1
    
    def stop(self):
        self.finished.set()
        self.g.join(5)
        self.g.kill()

    def start(self):
        self.finished.clear()
        self.g = gevent.spawn(self.run)
    
    @classmethod
    def float_range(cls, minvar, maxvar,t):
        '''
        Produces a signal with values between minvar and maxvar 
        at a frequency of 1/60 Hz centered at the midpoint 
        between minvar and maxvar.


        This method provides a deterministic function that 
        varies over time and is sinusoidal when graphed.
        '''
        a = (maxvar-minvar)/2
        return np.sin(np.pi * 2 * t /60) * a + (minvar + a)

