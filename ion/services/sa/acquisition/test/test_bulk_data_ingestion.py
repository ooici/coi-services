#!/usr/bin/env python

'''
@file ion/services/sa/instrument/test/test_int_data_acquisition_management_service.py
@author Maurice Manning
@test ion.services.sa.acquisition.DataAcquisitionManagementService integration test
'''

#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject, PRED, RT
from pyon.public import RT
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.agent.agent import ResourceAgentClient
from pyon.event.event import EventSubscriber

from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.sa.idata_acquisition_management_service import  DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, ExternalDatasetModel, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource
from interface.objects import AgentCommand, ExternalDatasetAgent, ExternalDatasetAgentInstance

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

from ion.agents.instrument.instrument_agent import InstrumentAgentState

from gevent.event import AsyncResult
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
import unittest
import numpy

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
@unittest.skip('not working')
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

        #TG: Setup/configure the granule logger to log granules as they're published
        dataset_agent_instance = self.dams_client.start_external_dataset_agent_instance(self.dataset_agent_instance_id)

        log.debug("TestBulkIngest: dataset_agent_instance_id %s", str(dataset_agent_instance))

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = ResourceAgentClient(self.EDA_RESOURCE_ID,  process=FakeProcess())
        log.debug('TestBulkIngest: Got ia client %s.', str(self._ia_client))



    def _start_finished_event_subscriber(self):

        def consume_event(*args,**kwargs):
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


    @unittest.skip('Not done yet.')
    def test_slocum_data_ingest(self):

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

        self._finished_count = 3

        # Begin streaming.
        cmd = AgentCommand(command='go_streaming')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STREAMING)

        # Assert that data was received
        self._async_finished_result.get(timeout=15)
        self.assertTrue(len(self._finished_events_received) >= 3)

        # Halt streaming.
        cmd = AgentCommand(command='go_observatory')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)




    def _setup_resources(self):

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
        datasetagent_id = self.dams_client.create_external_dataset_agent(external_dataset_agent=datasetagent_obj, external_dataset_model_id=dataset_model_id)

        # Generate the data product and associate it to the ExternalDataset
        streamdef_id = self.pubsub_client.create_stream_definition(name="temp", description="temp")
        dprod = DataProduct(name='slocum_parsed_product', description='parsed slocum product')
        dproduct_id = self.dataproductclient.create_data_product(data_product=dprod, stream_definition_id=streamdef_id)
        self.dams_client.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id)

        stream_id, assn = self.rrclient.find_objects(subject=dproduct_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_id[0]

        pdict = ParameterDictionary()
        t_ctxt = ParameterContext('data', param_type=QuantityType(value_encoding=numpy.dtype('int64')))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 01-01-1970'
        pdict.add_context(t_ctxt)

        # Create the logger for receiving publications
        #self.create_stream_and_logger(name='slocum',stream_id=stream_id)
        # Create agent config.
        self.EDA_RESOURCE_ID = ds_id
        self.EDA_NAME = ds_name

        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,
            'param_dictionary':pdict.dump(),
            'data_producer_id':dproducer_id, #CBM: Should this be put in the main body of the config - with mod & cls?
            'max_records':20,
        }
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
                                                                                            external_dataset_agent_id=datasetagent_id, external_dataset_id=ds_id)



