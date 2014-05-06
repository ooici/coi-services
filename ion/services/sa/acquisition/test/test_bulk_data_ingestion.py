#!/usr/bin/env python

'''
@file ion/services/sa/instrument/test/test_int_data_acquisition_management_service.py
@author Maurice Manning
@test ion.services.sa.acquisition.DataAcquisitionManagementService integration test
'''

#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import log, IonObject, PRED, RT, OT
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.event.event import EventSubscriber

from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import QuantityType, ArrayType

from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.sa.idata_acquisition_management_service import  DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.objects import ExternalDataProvider, ExternalDatasetModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution 
from interface.objects import AgentCommand, ProcessDefinition, DataProduct

from ion.agents.instrument.instrument_agent import InstrumentAgentState
from ion.core.includes.mi import DriverEvent
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.utility.granule_utils import RecordDictionaryTool

from gevent.event import AsyncResult, Event
from nose.plugins.attrib import attr
import unittest
import numpy as np
import os

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
#@unittest.skip('Not done yet.')
class TestBulkIngest(IonIntegrationTestCase):

    EDA_MOD = 'ion.agents.data.external_dataset_agent'
    EDA_CLS = 'ExternalDatasetAgent'


    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataAcquisitionManagementService
        self.client = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dams_client = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsub_client = PubsubManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.data_retriever    = DataRetrieverServiceClient(node=self.container.node)

        self._container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)

        # Data async and subscription  TODO: Replace with new subscriber
        self._finished_count = None
        #TODO: Switch to gevent.queue.Queue
        self._async_finished_result = AsyncResult()
        self._finished_events_received = []
        self._finished_event_subscriber = None
        self._start_finished_event_subscriber()
        self.addCleanup(self._stop_finished_event_subscriber)


        self.DVR_CONFIG = {}
        self.DVR_CONFIG = {
            'dvr_mod' : 'ion.agents.data.handlers.slocum_data_handler',
            'dvr_cls' : 'SlocumDataHandler',
            }

        self._setup_resources()

        self.agent_config = {
            'driver_config' : self.DVR_CONFIG,
            'stream_config' : {},
            'agent'         : {'resource_id': self.EDA_RESOURCE_ID},
            'test_mode' : True
        }

        datasetagent_instance_obj = IonObject(RT.ExternalDatasetAgentInstance,  name='ExternalDatasetAgentInstance1', description='external data agent instance',
                                              handler_module=self.EDA_MOD, handler_class=self.EDA_CLS,
                                              dataset_driver_config=self.DVR_CONFIG, dataset_agent_config=self.agent_config )
        self.dataset_agent_instance_id = self.dams_client.create_external_dataset_agent_instance(external_dataset_agent_instance=datasetagent_instance_obj,
                                                                                                 external_dataset_agent_id=self.datasetagent_id, external_dataset_id=self.EDA_RESOURCE_ID)


        #TG: Setup/configure the granule logger to log granules as they're published
        pid = self.dams_client.start_external_dataset_agent_instance(self.dataset_agent_instance_id)

        dataset_agent_instance_obj= self.dams_client.read_external_dataset_agent_instance(self.dataset_agent_instance_id)
        print 'TestBulkIngest: Dataset agent instance obj: = ', dataset_agent_instance_obj


        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient('datasetagentclient', name=pid,  process=FakeProcess())
        log.debug(" test_createTransformsThenActivateInstrument:: got ia client %s", str(self._ia_client))



    def create_logger(self, name, stream_id=''):

        # logger process
        producer_definition = ProcessDefinition(name=name+'_logger')
        producer_definition.executable = {
            'module':'ion.processes.data.stream_granule_logger',
            'class':'StreamGranuleLogger'
        }

        logger_procdef_id = self.processdispatchclient.create_process_definition(process_definition=producer_definition)
        configuration = {
            'process':{
                'stream_id':stream_id,
                }
        }
        pid = self.processdispatchclient.schedule_process(process_definition_id= logger_procdef_id, configuration=configuration)

        return pid

    def _start_finished_event_subscriber(self):

        def consume_event(*args,**kwargs):
            log.debug('EventSubscriber event received: %s', str(args[0]) )
            if args[0].description == 'TestingFinished':
                log.debug('TestingFinished event received')
                self._finished_events_received.append(args[0])
                if self._finished_count and self._finished_count == len(self._finished_events_received):
                    log.debug('Finishing test...')
                    self._async_finished_result.set(len(self._finished_events_received))
                    log.debug('Called self._async_finished_result.set({0})'.format(len(self._finished_events_received)))

        self._finished_event_subscriber = EventSubscriber(event_type='DeviceEvent', callback=consume_event)
        self._finished_event_subscriber.start()

    def _stop_finished_event_subscriber(self):
        if self._finished_event_subscriber:
            self._finished_event_subscriber.stop()
            self._finished_event_subscriber = None


    def tearDown(self):
        pass


    @unittest.skip('Update to agent refactor.')
    def test_slocum_data_ingest(self):

        HIST_CONSTRAINTS_1 = {}
        # Test instrument driver execute interface to start and stop streaming mode.
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)




        # Make sure the polling interval is appropriate for a test
        params = {
            'POLLING_INTERVAL': 3
        }
        self._ia_client.set_param(params)

        self._finished_count = 1

        cmd = AgentCommand(command='acquire_data')
        self._ia_client.execute(cmd)

        # Assert that data was received
        self._async_finished_result.get(timeout=15)

        self.assertTrue(len(self._finished_events_received) >= 1)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)


        #todo enable after Luke's mor to retrieve, right now must have the Time axis called 'time'
        #        replay_granule = self.data_retriever.retrieve_last_data_points(self.dataset_id, 10)

        #        rdt = RecordDictionaryTool.load_from_granule(replay_granule)
        #
        #        comp = rdt['date_pattern'] == numpy.arange(10) + 10
        #
        #        log.debug("TestBulkIngest: comp: %s", comp)
        #
        #        self.assertTrue(comp.all())

        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)


    def _setup_resources(self):

        self.loggerpids = []

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
        dprov.contact.name = 'Christopher Mueller'
        dprov.contact.email = 'cmueller@asascience.com'

        # Create DataSetModel
        dataset_model = ExternalDatasetModel(name='slocum_model')
        dataset_model.datset_type = 'SLOCUM'
        dataset_model_id = self.dams_client.create_external_dataset_model(dataset_model)

        # Create ExternalDataset
        ds_name = 'slocum_test_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())


        dset.dataset_description.parameters['base_url'] = 'test_data/slocum/'
        dset.dataset_description.parameters['list_pattern'] = 'ru05-2012-021-0-0-sbd.dat'
        dset.dataset_description.parameters['date_pattern'] = '%Y %j'
        dset.dataset_description.parameters['date_extraction_pattern'] = 'ru05-([\d]{4})-([\d]{3})-\d-\d-sbd.dat'
        dset.dataset_description.parameters['temporal_dimension'] = None
        dset.dataset_description.parameters['zonal_dimension'] = None
        dset.dataset_description.parameters['meridional_dimension'] = None
        dset.dataset_description.parameters['vertical_dimension'] = None
        dset.dataset_description.parameters['variables'] = [
            'c_wpt_y_lmc',
            'sci_water_cond',
            'm_y_lmc',
            'u_hd_fin_ap_inflection_holdoff',
            'sci_m_present_time',
            'm_leakdetect_voltage_forward',
            'sci_bb3slo_b660_scaled',
            'c_science_send_all',
            'm_gps_status',
            'm_water_vx',
            'm_water_vy',
            'c_heading',
            'sci_fl3slo_chlor_units',
            'u_hd_fin_ap_gain',
            'm_vacuum',
            'u_min_water_depth',
            'm_gps_lat',
            'm_veh_temp',
            'f_fin_offset',
            'u_hd_fin_ap_hardover_holdoff',
            'c_alt_time',
            'm_present_time',
            'm_heading',
            'sci_bb3slo_b532_scaled',
            'sci_fl3slo_cdom_units',
            'm_fin',
            'x_cycle_overrun_in_ms',
            'sci_water_pressure',
            'u_hd_fin_ap_igain',
            'sci_fl3slo_phyco_units',
            'm_battpos',
            'sci_bb3slo_b470_scaled',
            'm_lat',
            'm_gps_lon',
            'sci_ctd41cp_timestamp',
            'm_pressure',
            'c_wpt_x_lmc',
            'c_ballast_pumped',
            'x_lmc_xy_source',
            'm_lon',
            'm_avg_speed',
            'sci_water_temp',
            'u_pitch_ap_gain',
            'm_roll',
            'm_tot_num_inflections',
            'm_x_lmc',
            'u_pitch_ap_deadband',
            'm_final_water_vy',
            'm_final_water_vx',
            'm_water_depth',
            'm_leakdetect_voltage',
            'u_pitch_max_delta_battpos',
            'm_coulomb_amphr',
            'm_pitch',
            ]



        ## Create the external dataset
        ds_id = self.dams_client.create_external_dataset(external_dataset=dset, external_dataset_model_id=dataset_model_id)
        ext_dprov_id = self.dams_client.create_external_data_provider(external_data_provider=dprov)

        # Register the ExternalDataset
        dproducer_id = self.dams_client.register_external_data_set(external_dataset_id=ds_id)

        ## Create the dataset agent
        datasetagent_obj = IonObject(RT.ExternalDatasetAgent,  name='ExternalDatasetAgent1', description='external data agent', handler_module=self.EDA_MOD, handler_class=self.EDA_CLS )
        self.datasetagent_id = self.dams_client.create_external_dataset_agent(external_dataset_agent=datasetagent_obj, external_dataset_model_id=dataset_model_id)

        # Generate the data product and associate it to the ExternalDataset
        pdict = DatasetManagementService.get_parameter_dictionary_by_name('ctd_parsed_param_dict')
        streamdef_id = self.pubsub_client.create_stream_definition(name="temp", parameter_dictionary_id=pdict.identifier)



        dprod = IonObject(RT.DataProduct,
                          name='slocum_parsed_product',
                          description='parsed slocum product')

        self.dproduct_id = self.dataproductclient.create_data_product(data_product=dprod,
                                                                      stream_definition_id=streamdef_id)

        self.dams_client.assign_data_product(input_resource_id=ds_id, data_product_id=self.dproduct_id)

        #save the incoming slocum data
        self.dataproductclient.activate_data_product_persistence(self.dproduct_id)
        self.addCleanup(self.dataproductclient.suspend_data_product_persistence, self.dproduct_id)
        
        stream_ids, assn = self.rrclient.find_objects(subject=self.dproduct_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_ids[0]

        dataset_id, assn = self.rrclient.find_objects(subject=self.dproduct_id, predicate=PRED.hasDataset, object_type=RT.Dataset, id_only=True)
        self.dataset_id = dataset_id[0]

        pid = self.create_logger('slocum_parsed_product', stream_id )
        self.loggerpids.append(pid)

        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,
            'param_dictionary':pdict.dump(),
            'data_producer_id':dproducer_id, #CBM: Should this be put in the main body of the config - with mod & cls?
            'max_records':20,
            }

        # Create the logger for receiving publications
        #self.create_stream_and_logger(name='slocum',stream_id=stream_id)
        # Create agent config.
        self.EDA_RESOURCE_ID = ds_id
        self.EDA_NAME = ds_name

class BulkIngestBase(object):
    """
    awkward, non-obvious test class!  subclasses will implement data-specific methods and
    this test class will parse sample file and assert data was read.

    test_data_ingest: create resources and call...
        start_agent: starts agent and then call...
            start_listener: starts listeners for data, including one that when granule is received calls...
                get_retrieve_client: asserts that callback had some data

    See replacement TestPreloadThenLoadDataset.  A little more declarative and straight-forward, but much slower (requires preload).
    """
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.pubsub_management    = PubsubManagementServiceClient()
        self.dataset_management   = DatasetManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.data_acquisition_management = DataAcquisitionManagementServiceClient()
        self.data_retriever = DataRetrieverServiceClient()
        self.process_dispatch_client = ProcessDispatcherServiceClient(node=self.container.node)
        self.resource_registry       = self.container.resource_registry
        self.context_ids = self.build_param_contexts()
        self.setup_resources()

    def build_param_contexts(self):
        raise NotImplementedError('build_param_contexts must be implemented in child classes')

    def create_external_dataset(self):
        raise NotImplementedError('create_external_dataset must be implemented in child classes')

    def get_dvr_config(self):
        raise NotImplementedError('get_dvr_config must be implemented in child classes')

    def get_retrieve_client(self, dataset_id=''):
        raise NotImplementedError('get_retrieve_client must be implemented in child classes')

    def test_data_ingest(self):
        self.pdict_id = self.create_parameter_dict(self.name)
        self.stream_def_id = self.create_stream_def(self.name, self.pdict_id)
        self.data_product_id = self.create_data_product(self.name, self.description, self.stream_def_id)
        self.dataset_id = self.get_dataset_id(self.data_product_id)
        self.stream_id, self.route = self.get_stream_id_and_route(self.data_product_id)
        self.external_dataset_id = self.create_external_dataset()
        self.data_producer_id = self.register_external_dataset(self.external_dataset_id)
        self.start_agent()

    def create_parameter_dict(self, name=''):
        return self.dataset_management.create_parameter_dictionary(name=name, parameter_context_ids=self.context_ids, temporal_context='time')

    def create_stream_def(self, name='', pdict_id=''):
        return self.pubsub_management.create_stream_definition(name=name, parameter_dictionary_id=pdict_id)

    def create_data_product(self, name='', description='', stream_def_id=''):
        dp_obj = DataProduct(
            name=name,
            description=description,
            processing_level_code='Parsed_Canonical')

        data_product_id = self.data_product_management.create_data_product(data_product=dp_obj, stream_definition_id=stream_def_id)
        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)
        return data_product_id

    def register_external_dataset(self, external_dataset_id=''):
        return self.data_acquisition_management.register_external_data_set(external_dataset_id=external_dataset_id)

    def get_dataset_id(self, data_product_id=''):
        dataset_ids, assocs = self.resource_registry.find_objects(subject=data_product_id, predicate='hasDataset', id_only=True)
        return dataset_ids[0]

    def get_stream_id_and_route(self, data_product_id):
        stream_ids, _ = self.resource_registry.find_objects(data_product_id, PRED.hasStream, RT.Stream, id_only=True)
        stream_id = stream_ids[0]
        route = self.pubsub_management.read_stream_route(stream_id)
        #self.create_logger(self.name, stream_id)
        return stream_id, route

    def start_agent(self):
        agent_config = {
            'driver_config': self.get_dvr_config(),
            'stream_config': {},
            'agent': {'resource_id': self.external_dataset_id},
            'test_mode': True
        }

        self._ia_pid = self.container.spawn_process(
            name=self.EDA_NAME,
            module=self.EDA_MOD,
            cls=self.EDA_CLS,
            config=agent_config)

        self._ia_client = ResourceAgentClient(self.external_dataset_id, process=FakeProcess())

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command=DriverEvent.START_AUTOSAMPLE)
        self._ia_client.execute_resource(command=cmd)

        self.start_listener(self.dataset_id)

    def stop_agent(self):
        cmd = AgentCommand(command=DriverEvent.STOP_AUTOSAMPLE)
        self._ia_client.execute_resource(cmd)
        cmd = AgentCommand(command=ResourceAgentEvent.RESET)
        self._ia_client.execute_agent(cmd)
        self.container.terminate_process(self._ia_pid)

    def start_listener(self, dataset_id=''):
        dataset_modified = Event()
        #callback to use retrieve to get data from the coverage
        def cb(*args, **kwargs):
            self.get_retrieve_client(dataset_id=dataset_id)

        #callback to keep execution going once dataset has been fully ingested
        def cb2(*args, **kwargs):
            dataset_modified.set()

        es = EventSubscriber(event_type=OT.DatasetModified, callback=cb, origin=dataset_id)
        es.start()

        es2 = EventSubscriber(event_type=OT.DeviceCommonLifecycleEvent, callback=cb2, origin='BaseDataHandler._acquire_sample')
        es2.start()

        self.addCleanup(es.stop)
        self.addCleanup(es2.stop)

        #let it go for up to 120 seconds, then stop the agent and reset it
        dataset_modified.wait(120)
        self.stop_agent()

    def create_logger(self, name, stream_id=''):

        # logger process
        producer_definition = ProcessDefinition(name=name+'_logger')
        producer_definition.executable = {
            'module':'ion.processes.data.stream_granule_logger',
            'class':'StreamGranuleLogger'
        }

        logger_procdef_id = self.process_dispatch_client.create_process_definition(process_definition=producer_definition)
        configuration = {
            'process':{
                'stream_id':stream_id,
                }
        }
        pid = self.process_dispatch_client.schedule_process(process_definition_id=logger_procdef_id, configuration=configuration)

        return pid

@attr('INT', group='sa')
class TestBulkIngest_Hypm_WPF_CTD(BulkIngestBase, IonIntegrationTestCase):

    def setup_resources(self):
        self.name = 'Bulk Data Ingest HYPM WPF CTD'
        self.description = 'Bulk Data Ingest HYPM WPF CTD Test'
        self.EDA_NAME = 'ExampleEDA'
        self.EDA_MOD = 'ion.agents.data.external_dataset_agent'
        self.EDA_CLS = 'ExternalDatasetAgent'

    def build_param_contexts(self):
        context_ids = []
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1970'
        context_ids.append(self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump()))

        cnd_ctxt = ParameterContext('conductivity', param_type=ArrayType())
        cnd_ctxt.uom = 'mmho/cm'
        context_ids.append(self.dataset_management.create_parameter_context(name='conductivity', parameter_context=cnd_ctxt.dump()))

        temp_ctxt = ParameterContext('temperature', param_type=ArrayType())
        temp_ctxt.uom = 'degC'
        context_ids.append(self.dataset_management.create_parameter_context(name='temperature', parameter_context=temp_ctxt.dump()))

        press_ctxt = ParameterContext('pressure', param_type=ArrayType())
        press_ctxt.uom = 'decibars'
        context_ids.append(self.dataset_management.create_parameter_context(name='pressure', parameter_context=press_ctxt.dump()))

        oxy_ctxt = ParameterContext('oxygen', param_type=ArrayType())
        oxy_ctxt.uom = 'Hz'
        context_ids.append(self.dataset_management.create_parameter_context(name='oxygen', parameter_context=oxy_ctxt.dump()))

        return context_ids

    def create_external_dataset(self):
        ds_name = 'hypm_01_wfp_ctd_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        dset.dataset_description.parameters['base_url'] = 'test_data'
        dset.dataset_description.parameters['list_pattern'] = 'C*.HEX'

        return self.data_acquisition_management.create_external_dataset(external_dataset=dset)

    def get_dvr_config(self):
        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.data.handlers.hypm_data_handler',
            'dvr_cls': 'HYPMDataHandler',
            'dh_cfg': {
                'parser_mod': 'ion.agents.data.handlers.hypm_data_handler',
                'parser_cls': 'HYPM_01_WFP_CTDParser',
                'stream_id': self.stream_id,
                'stream_route': self.route,
                'stream_def': self.stream_def_id,
                'data_producer_id': self.data_producer_id,
                'max_records': 4,
                'TESTING': True,
                }
        }
        return DVR_CONFIG

    def get_retrieve_client(self, dataset_id=''):
        replay_data = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        self.assertIsNotNone(rdt['temperature'])
        #need to compare rdt from retrieve with the one from ingest somehow

@attr('INT', group='sa')
class TestBulkIngest_Slocum(BulkIngestBase, IonIntegrationTestCase):

    def setup_resources(self):
        self.name = 'Bulk Data Ingest Slocum'
        self.description = 'Bulk Data Ingest Slocum Test'
        self.EDA_NAME = 'ExampleEDA'
        self.EDA_MOD = 'ion.agents.data.external_dataset_agent'
        self.EDA_CLS = 'ExternalDatasetAgent'

    def build_param_contexts(self):
        context_ids = []
        t_ctxt = ParameterContext('c_wpt_y_lmc', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='c_wpt_y_lmc', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_water_cond', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_water_cond', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_y_lmc', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_y_lmc', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('u_hd_fin_ap_inflection_holdoff', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='u_hd_fin_ap_inflection_holdoff', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_m_present_time', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_m_present_time', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_leakdetect_voltage_forward', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_leakdetect_voltage_forward', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_bb3slo_b660_scaled', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_bb3slo_b660_scaled', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('c_science_send_all', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='c_science_send_all', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_gps_status', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_gps_status', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_water_vx', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_water_vx', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_water_vy', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_water_vy', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('c_heading', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='c_heading', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_fl3slo_chlor_units', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_fl3slo_chlor_units', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('u_hd_fin_ap_gain', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='u_hd_fin_ap_gain', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_vacuum', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_vacuum', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('u_min_water_depth', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='u_min_water_depth', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_gps_lat', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_gps_lat', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_veh_temp', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_veh_temp', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('f_fin_offset', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='f_fin_offset', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('u_hd_fin_ap_hardover_holdoff', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='u_hd_fin_ap_hardover_holdoff', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('c_alt_time', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='c_alt_time', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_present_time', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_present_time', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_heading', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_heading', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_bb3slo_b532_scaled', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_bb3slo_b532_scaled', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_fl3slo_cdom_units', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_fl3slo_cdom_units', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_fin', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_fin', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('x_cycle_overrun_in_ms', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='x_cycle_overrun_in_ms', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_water_pressure', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_water_pressure', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('u_hd_fin_ap_igain', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='u_hd_fin_ap_igain', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_fl3slo_phyco_units', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_fl3slo_phyco_units', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_battpos', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_battpos', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_bb3slo_b470_scaled', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_bb3slo_b470_scaled', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_lat', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_lat', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_gps_lon', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_gps_lon', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_ctd41cp_timestamp', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_ctd41cp_timestamp', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_pressure', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_pressure', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('c_wpt_x_lmc', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='c_wpt_x_lmc', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('c_ballast_pumped', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='c_ballast_pumped', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('x_lmc_xy_source', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='x_lmc_xy_source', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_lon', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_lon', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_avg_speed', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_avg_speed', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('sci_water_temp', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='sci_water_temp', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('u_pitch_ap_gain', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='u_pitch_ap_gain', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_roll', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_roll', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_tot_num_inflections', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_tot_num_inflections', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_x_lmc', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_x_lmc', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('u_pitch_ap_deadband', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='u_pitch_ap_deadband', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_final_water_vy', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_final_water_vy', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_final_water_vx', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_final_water_vx', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_water_depth', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_water_depth', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_leakdetect_voltage', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_leakdetect_voltage', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('u_pitch_max_delta_battpos', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='u_pitch_max_delta_battpos', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_coulomb_amphr', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_coulomb_amphr', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('m_pitch', param_type=QuantityType(value_encoding=np.dtype('float32')))
        t_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='m_pitch', parameter_context=t_ctxt.dump()))

        return context_ids

    def create_external_dataset(self):
        ds_name = 'slocum_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        dset.dataset_description.parameters['base_url'] = 'test_data/slocum'
        dset.dataset_description.parameters['list_pattern'] = 'ru05-*-sbd.dat'
        dset.dataset_description.parameters['date_pattern'] = '%Y %j'
        dset.dataset_description.parameters['date_extraction_pattern'] = 'ru05-([\d]{4})-([\d]{3})-\d-\d-sbd.dat'

        return self.data_acquisition_management.create_external_dataset(external_dataset=dset)

    def get_dvr_config(self):
        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.data.handlers.slocum_data_handler',
            'dvr_cls': 'SlocumDataHandler',
            'dh_cfg': {
                'stream_id': self.stream_id,
                'stream_route': self.route,
                'stream_def': self.stream_def_id,
                'data_producer_id': self.data_producer_id,
                'max_records': 4,
                'TESTING': True,
                }
        }
        return DVR_CONFIG

    def get_retrieve_client(self, dataset_id=''):
        replay_data = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        self.assertIsNotNone(rdt['c_wpt_y_lmc'])
