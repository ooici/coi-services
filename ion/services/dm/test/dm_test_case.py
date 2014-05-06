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
from interface.services.dm.idiscovery_service import DiscoveryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from interface.services.ans.iworkflow_management_service import WorkflowManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.ans.ivisualization_service import VisualizationServiceClient
from ion.processes.data.registration.registration_process import RegistrationProcess

from pyon.public import RT, PRED, CFG
from interface.objects import DataProduct, StreamConfiguration, StreamConfigurationType
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient
from pyon.util.context import LocalContextMixin
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.breakpoint import breakpoint
from pyon.container.cc import Container
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from pyon.util.containers import DotDict
from uuid import uuid4

import numpy as np
import os
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
        self.discovery = DiscoveryServiceClient()
        self.dataset_management =  DatasetManagementServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.data_process_management = DataProcessManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.data_retriever = DataRetrieverServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.user_notification = UserNotificationServiceClient()
        self.workflow_management = WorkflowManagementServiceClient()
        self.observatory_management = ObservatoryManagementServiceClient()
        self.visualization = VisualizationServiceClient()
        self.ph = ParameterHelper(self.dataset_management, self.addCleanup)
        self.ctd_count = 0

    def create_stream_definition(self, *args, **kwargs):
        stream_def_id = self.pubsub_management.create_stream_definition(*args, **kwargs)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)
        return stream_def_id

    def create_data_product(self,name, stream_def_id='', param_dict_name='', pdict_id='', stream_configuration=None):
        if not (stream_def_id or param_dict_name or pdict_id):
            raise AssertionError('Attempted to create a Data Product without a parameter dictionary')


        dp = DataProduct(name=name)

        stream_def_id = stream_def_id or self.create_stream_definition('%s stream def' % name, 
                parameter_dictionary_id=pdict_id or self.RR2.find_resource_by_name(RT.ParameterDictionary,
                    param_dict_name, id_only=True))


        stream_config = stream_configuration or StreamConfiguration(stream_name='parsed_ctd', stream_type=StreamConfigurationType.PARSED)

        data_product_id = self.data_product_management.create_data_product(dp, stream_definition_id=stream_def_id, default_stream_configuration=stream_config)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)
        return data_product_id

    def activate_data_product(self, data_product_id):
        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)
    
    def data_product_by_id(self, alt_id):
        data_products, _ = self.container.resource_registry.find_resources_ext(alt_id=alt_id, alt_id_ns='PRE', id_only=True)
        if data_products:
            return data_products[0]
        return None
    def dataset_of_data_product(self, data_product_id):
        return self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
    
    def make_ctd_data_product(self):
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict')
        stream_def_id = self.create_stream_definition('ctd %d' % self.ctd_count, parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product('ctd %d' % self.ctd_count, stream_def_id=stream_def_id)
        self.activate_data_product(data_product_id)
        self.ctd_count += 1
        return data_product_id
    
    def preload_ui(self):
        config = DotDict()
        config.op='loadui'
        config.loadui=True
        config.attachments='res/preload/r2_ioc/attachments'
        config.ui_path = "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)
    
    def strap_erddap(self, data_product_id=None):
        '''
        Copies the datasets.xml to /tmp
        '''
        datasets_xml_path = RegistrationProcess.get_datasets_xml_path(CFG)
        if os.path.lexists('/tmp/datasets.xml'):
            os.unlink('/tmp/datasets.xml')
        os.symlink(datasets_xml_path, '/tmp/datasets.xml')
        if data_product_id:
            with open('/tmp/erddap/flag/data%s' % data_product_id, 'a'):
                pass

        gevent.sleep(5)
        from subprocess import call
        call(['open', 'http://localhost:9000/erddap/tabledap/data%s.html' % data_product_id])
    
    def launch_ui_facepage(self, data_product_id):
        '''
        Opens the UI face page on localhost for a particular data product
        '''
        from subprocess import call
        call(['open', 'http://localhost:3000/DataProduct/face/%s/' % data_product_id])

class Streamer(object):
    def __init__(self, data_product_id, interval=1, simple_time=False, connection=False):
        self.resource_registry = Container.instance.resource_registry
        self.pubsub_management = PubsubManagementServiceClient()
        self.data_product_id = data_product_id
        self.i=0
        self.interval = interval
        self.simple_time = simple_time
        self.finished = Event()
        self.g = gevent.spawn(self.run)
        self.connection = connection

    def run(self):
        connection = uuid4().hex
        while not self.finished.wait(self.interval):
            rdt = ParameterHelper.rdt_for_data_product(self.data_product_id)
            now = time.time()
            if self.simple_time:
                rdt['time'] = [self.i]
            else:
                rdt['time'] = np.array([now + 2208988800])
            rdt['temp'] = self.float_range(10,14,np.array([now]))
            rdt['pressure'] = self.float_range(11,12,np.array([now]))
            rdt['lat'] = [41.205]
            rdt['lon'] = [-71.74]
            rdt['conductivity'] = self.float_range(3.3,3.5,np.array([now]))
            rdt['driver_timestamp'] = np.array([now + 2208988800])
            rdt['preferred_timestamp'] = ['driver_timestamp']
            if self.connection:
                ParameterHelper.publish_rdt_to_data_product(self.data_product_id, rdt, connection_id=connection, connection_index=self.i)
            else:
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

