#!/usr/bin/env python

import time
import numpy as np
from nose.plugins.attrib import attr
from gevent.event import Event

from pyon.event.event import EventSubscriber
from pyon.public import RT, log, PRED, OT
from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.util.int_test import IonIntegrationTestCase
from pyon.ion.stream import StandaloneStreamSubscriber
from pyon.core.exception import NotFound

from ion.services.sa.acquisition.test.test_bulk_data_ingestion import BulkIngestBase
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.sa.acquisition.test.test_bulk_data_ingestion import FakeProcess
from ion.core.includes.mi import DriverEvent

from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import QuantityType, ArrayType

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import  DataAcquisitionManagementServiceClient
from interface.objects import ExternalDataset, AgentCommand
from interface.objects import ContactInformation, UpdateDescription, DatasetDescription

MAX_AGENT_START_TIME = 300


@attr('INT', group='eoi')
class TestPreloadThenLoadDataset(IonIntegrationTestCase):
    """ Uses the preload system to define the ExternalDataset and related resources,
        then invokes services to perform the load
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        config = dict(op="load", scenario="NOSE", attachments="res/preload/r2_ioc/attachments")
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=config)
        self.pubsub = PubsubManagementServiceClient()
        self.dams = DataAcquisitionManagementServiceClient()

    def test_use_case(self):
        # setUp() has already started the container and performed the preload
#        self.assert_dataset_loaded('Test External CTD Dataset') # make sure we have the ExternalDataset resources
        self.assert_dataset_loaded('Unit Test SMB37')           # association changed -- now use device name
        self.do_listen_for_incoming()                           # listen for any data being received from the dataset
        self.do_read_dataset()                                  # call services to load dataset
        self.assert_data_received()                             # check that data was received as expected
        self.do_shutdown()

    def assert_dataset_loaded(self, name):
        rr = self.container.resource_registry
#        self.external_dataset = self.find_object_by_name(name, RT.ExternalDataset)
        devs, _ = rr.find_resources(RT.InstrumentDevice, name=name, id_only=False)
        self.assertEquals(len(devs), 1)
        self.device = devs[0]
        obj,_ = rr.find_objects(subject=self.device._id, predicate=PRED.hasAgentInstance, object_type=RT.ExternalDatasetAgentInstance)
        self.agent_instance = obj[0]
        obj,_ = rr.find_objects(object_type=RT.ExternalDatasetAgent, predicate=PRED.hasAgentDefinition, subject=self.agent_instance._id)
        self.agent = obj[0]

        driver_cfg = self.agent_instance.driver_config
        #stream_definition_id = driver_cfg['dh_cfg']['stream_def'] if 'dh_cfg' in driver_cfg else driver_cfg['stream_def']
        #self.stream_definition = rr.read(stream_definition_id)

        self.data_product = rr.read_object(subject=self.device._id, predicate=PRED.hasOutputProduct, object_type=RT.DataProduct)

        self.dataset_id = rr.read_object(subject=self.data_product._id, predicate=PRED.hasDataset, object_type=RT.Dataset, id_only=True)

        ids,_ = rr.find_objects(subject=self.data_product._id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        self.stream_id = ids[0]
        self.route = self.pubsub.read_stream_route(self.stream_id)

    def do_listen_for_incoming(self):
        subscription_id = self.pubsub.create_subscription('validator', data_product_ids=[self.data_product._id])
        self.addCleanup(self.pubsub.delete_subscription, subscription_id)

        self.granule_capture = []
        self.granule_count = 0
        def on_granule(msg, route, stream_id):
            self.granule_count += 1
            if self.granule_count < 5:
                self.granule_capture.append(msg)
        validator = StandaloneStreamSubscriber('validator', callback=on_granule)
        validator.start()
        self.addCleanup(validator.stop)

        self.pubsub.activate_subscription(subscription_id)
        self.addCleanup(self.pubsub.deactivate_subscription, subscription_id)

        self.dataset_modified = Event()
        def cb2(*args, **kwargs):
            self.dataset_modified.set()
            # TODO: event isn't using the ExternalDataset, but a different ID for a Dataset
        es = EventSubscriber(event_type=OT.DatasetModified, callback=cb2, origin=self.dataset_id)
        es.start()
        self.addCleanup(es.stop)

    def do_read_dataset(self):
        self.dams.start_external_dataset_agent_instance(self.agent_instance._id)
        #
        # should i wait for process (above) to start
        # before launching client (below)?
        #
        self.client = None
        end = time.time() + MAX_AGENT_START_TIME
        while not self.client and time.time() < end:
            try:
                self.client = ResourceAgentClient(self.device._id, process=FakeProcess())
            except NotFound:
                time.sleep(2)
        if not self.client:
            self.fail(msg='external dataset agent process did not start in %d seconds' % MAX_AGENT_START_TIME)
        self.client.execute_agent(AgentCommand(command=ResourceAgentEvent.INITIALIZE))
        self.client.execute_agent(AgentCommand(command=ResourceAgentEvent.GO_ACTIVE))
        self.client.execute_agent(AgentCommand(command=ResourceAgentEvent.RUN))
        self.client.execute_resource(command=AgentCommand(command=DriverEvent.START_AUTOSAMPLE))

    def assert_data_received(self):

        #let it go for up to 120 seconds, then stop the agent and reset it
        if not self.dataset_modified.is_set():
            self.dataset_modified.wait(30)
        self.assertTrue(self.granule_count > 2, msg='granule count = %d'%self.granule_count)

        rdt = RecordDictionaryTool.load_from_granule(self.granule_capture[0])
        self.assertAlmostEqual(0, rdt['oxygen'][0], delta=0.01)
        self.assertAlmostEqual(309.77, rdt['pressure'][0], delta=0.01)
        self.assertAlmostEqual(37.9848, rdt['conductivity'][0], delta=0.01)
        self.assertAlmostEqual(9.5163, rdt['temp'][0], delta=0.01)
        self.assertAlmostEqual(3527207897.0, rdt['time'][0], delta=1)

    def do_shutdown(self):
        self.dams.stop_external_dataset_agent_instance(self.agent_instance._id)


@attr('INT', group='eoi')
class TestBinaryCTD(BulkIngestBase, IonIntegrationTestCase):
    """
    use the existing, awkward framework for testing the binary parser.
    avoids preload but the test "flow" is not obvious.  See BulkIngestCase comments for hints.
    """
    def setup_resources(self):
        self.name = 'hypm_01_wpf_ctd'
        self.description = 'ctd instrument test'
        self.EDA_NAME = 'ExampleEDA'
        self.EDA_MOD = 'ion.agents.data.external_dataset_agent'
        self.EDA_CLS = 'ExternalDatasetAgent'

    def build_param_contexts(self):
        context_ids = []
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1970'
        context_ids.append(self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump()))

        t_ctxt = ParameterContext('upload_time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1970'
        context_ids.append(self.dataset_management.create_parameter_context(name='upload_time', parameter_context=t_ctxt.dump()))

        cnd_ctxt = ParameterContext('conductivity', param_type=ArrayType())  # param_type=QuantityType(value_encoding=np.dtype('float32')))
        cnd_ctxt.uom = 'mmho/cm'
        context_ids.append(self.dataset_management.create_parameter_context(name='conductivity', parameter_context=cnd_ctxt.dump()))

        temp_ctxt = ParameterContext('temp', param_type=ArrayType())  # param_type=QuantityType(value_encoding=np.dtype('float32')))
        temp_ctxt.uom = 'degC'
        context_ids.append(self.dataset_management.create_parameter_context(name='temp', parameter_context=temp_ctxt.dump()))

        press_ctxt = ParameterContext('pressure', param_type=ArrayType())  # param_type=QuantityType(value_encoding=np.dtype('float32')))
        press_ctxt.uom = 'decibars'
        context_ids.append(self.dataset_management.create_parameter_context(name='pressure', parameter_context=press_ctxt.dump()))

        oxy_ctxt = ParameterContext('oxygen', param_type=ArrayType())  # param_type=QuantityType(value_encoding=np.dtype('float32')))
        oxy_ctxt.uom = 'Hz'
        context_ids.append(self.dataset_management.create_parameter_context(name='oxygen', parameter_context=oxy_ctxt.dump()))

        return context_ids

    def create_external_dataset(self):
        ds_name = 'hypm_01_wfp_ctd_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        dset.dataset_description.parameters['base_url'] = 'test_data'
        dset.dataset_description.parameters['list_pattern'] = 'C*.DAT'

        return self.data_acquisition_management.create_external_dataset(external_dataset=dset)

    def get_dvr_config(self):
        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.data.handlers.sbe52_binary_handler',
            'dvr_cls': 'SBE52BinaryDataHandler',
            'dh_cfg': {
                'parser_mod': 'ion.agents.data.parsers.seabird.sbe52.binary_parser',
                'parser_cls': 'SBE52BinaryCTDParser',
                'stream_id': self.stream_id,
                'stream_route': self.route,
                'stream_def': self.stream_def_id,
                'data_producer_id': 'hypm_ctd_data_producer_id',
                'max_records': 4,
                'TESTING': True,
                }
        }
        return DVR_CONFIG

    def get_retrieve_client(self, dataset_id=''):
        replay_data = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        self.assertIsNotNone(rdt['temp'])


#DISABLED: attr('INT', group='eoi')
# these tests rely on the original handler mechanism which had several shortcomings leading to the poller/parser rewrite
class TestHypm_WPF_CTD(BulkIngestBase, IonIntegrationTestCase):

    def setup_resources(self):
        self.name = 'hypm_01_wpf_ctd'
        self.description = 'ctd instrument test'
        self.EDA_NAME = 'ExampleEDA'
        self.EDA_MOD = 'ion.agents.data.external_dataset_agent'
        self.EDA_CLS = 'ExternalDatasetAgent'

    def build_param_contexts(self):
        context_ids = []
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1970'
        context_ids.append(self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump()))

        cnd_ctxt = ParameterContext('conductivity', param_type=ArrayType())  # param_type=QuantityType(value_encoding=np.dtype('float32')))
        cnd_ctxt.uom = 'mmho/cm'
        context_ids.append(self.dataset_management.create_parameter_context(name='conductivity', parameter_context=cnd_ctxt.dump()))

        temp_ctxt = ParameterContext('temperature', param_type=ArrayType())  # param_type=QuantityType(value_encoding=np.dtype('float32')))
        temp_ctxt.uom = 'degC'
        context_ids.append(self.dataset_management.create_parameter_context(name='temperature', parameter_context=temp_ctxt.dump()))

        press_ctxt = ParameterContext('pressure', param_type=ArrayType())  # param_type=QuantityType(value_encoding=np.dtype('float32')))
        press_ctxt.uom = 'decibars'
        context_ids.append(self.dataset_management.create_parameter_context(name='pressure', parameter_context=press_ctxt.dump()))

        oxy_ctxt = ParameterContext('oxygen', param_type=ArrayType())  # param_type=QuantityType(value_encoding=np.dtype('float32')))
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
                'data_producer_id': 'hypm_ctd_data_producer_id',
                'max_records': 4,
                'TESTING': True,
                }
        }
        return DVR_CONFIG

    def get_retrieve_client(self, dataset_id=''):
        replay_data = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        self.assertIsNotNone(rdt['temperature'])

#PASSES BUT DISABLED: attr('INT', group='eoi')
# these tests rely on the original handler mechanism which had several shortcomings leading to the poller/parser rewrite
class TestHypm_WPF_ACM(BulkIngestBase, IonIntegrationTestCase):
    def setup_resources(self):
        self.name = 'hypm_01_wpf_acm'
        self.description = 'acm instrument test'
        self.EDA_NAME = 'ExampleEDA'
        self.EDA_MOD = 'ion.agents.data.external_dataset_agent'
        self.EDA_CLS = 'ExternalDatasetAgent'

    def build_param_contexts(self):
        context_ids = []
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1970'
        context_ids.append(self.dataset_management.create_parameter_context(name='time', parameter_context=t_ctxt.dump()))

        ut_ctxt = ParameterContext('upload_time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        ut_ctxt.uom = 'seconds since 01-01-1970'
        context_ids.append(self.dataset_management.create_parameter_context(name='upload_time', parameter_context=ut_ctxt.dump()))

        vela_ctxt = ParameterContext('VelA', param_type=ArrayType())
        vela_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='VelA', parameter_context=vela_ctxt.dump()))

        velb_ctxt = ParameterContext('VelB', param_type=ArrayType())
        velb_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='VelB', parameter_context=velb_ctxt.dump()))

        velc_ctxt = ParameterContext('VelC', param_type=ArrayType())
        velc_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='VelC', parameter_context=velc_ctxt.dump()))

        veld_ctxt = ParameterContext('VelD', param_type=ArrayType())
        veld_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='VelD', parameter_context=veld_ctxt.dump()))

        mx_ctxt = ParameterContext('Mx', param_type=ArrayType())
        mx_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Mx', parameter_context=mx_ctxt.dump()))

        my_ctxt = ParameterContext('My', param_type=ArrayType())
        my_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='My', parameter_context=my_ctxt.dump()))

        mz_ctxt = ParameterContext('Mz', param_type=ArrayType())
        mz_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Mz', parameter_context=mz_ctxt.dump()))

        pitch_ctxt = ParameterContext('Pitch', param_type=ArrayType())
        pitch_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Pitch', parameter_context=pitch_ctxt.dump()))

        roll_ctxt = ParameterContext('Roll', param_type=ArrayType())
        roll_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Roll', parameter_context=roll_ctxt.dump()))

        return context_ids

    def create_external_dataset(self):
        ds_name = 'hypm_01_wfp_acm_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        dset.dataset_description.parameters['base_url'] = 'test_data'
        dset.dataset_description.parameters['list_pattern'] = 'A*.HEX'

        return self.data_acquisition_management.create_external_dataset(external_dataset=dset)

    def get_dvr_config(self):
        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.data.handlers.hypm_data_handler',
            'dvr_cls': 'HYPMDataHandler',
            'dh_cfg': {
                'parser_mod': 'ion.agents.data.handlers.hypm_data_handler',
                'parser_cls': 'HYPM_01_WFP_ACMParser',
                'stream_id': self.stream_id,
                'stream_route': self.route,
                'stream_def': self.stream_def_id,
                'data_producer_id': 'hypm_acm_data_producer_id',
                'max_records': 4,
                'TESTING': True,
                }
        }
        return DVR_CONFIG

    def get_retrieve_client(self, dataset_id=''):
        replay_data = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        self.assertIsNotNone(rdt['time'])

#PASSES BUT DISABLED: attr('INT', group='eoi')
# these tests rely on the original handler mechanism which had several shortcomings leading to the poller/parser rewrite
class TestHypm_WPF_ENG(BulkIngestBase, IonIntegrationTestCase):

    def setup_resources(self):
        self.name = 'hypm_01_wpf_eng'
        self.description = 'eng instrument test'
        self.EDA_NAME = 'ExampleEDA'
        self.EDA_MOD = 'ion.agents.data.external_dataset_agent'
        self.EDA_CLS = 'ExternalDatasetAgent'

    def build_param_contexts(self):
        context_ids = []
        t_ctxt = ParameterContext('Time_Time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.uom = 'seconds since 01-01-1970'
        context_ids.append(self.dataset_management.create_parameter_context(name='Time_Time', parameter_context=t_ctxt.dump()))

        core_current_ctxt = ParameterContext('Core_Current', param_type=QuantityType(value_encoding=np.dtype('float32')))
        core_current_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Core_Current', parameter_context=core_current_ctxt.dump()))

        core_voltage_ctxt = ParameterContext('Core_Voltage', param_type=QuantityType(value_encoding=np.dtype('float32')))
        core_voltage_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Core_Voltage', parameter_context=core_voltage_ctxt.dump()))

        core_pressure_ctxt = ParameterContext('Core_Pressure', param_type=QuantityType(value_encoding=np.dtype('float32')))
        core_pressure_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Core_Pressure', parameter_context=core_pressure_ctxt.dump()))

        fluorometer_value_ctxt = ParameterContext('Fluorometer_Value', param_type=QuantityType(value_encoding=np.dtype('float32')))
        fluorometer_value_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Fluorometer_Value', parameter_context=fluorometer_value_ctxt.dump()))

        fluorometer_gain_ctxt = ParameterContext('Fluorometer_Gain', param_type=QuantityType(value_encoding=np.dtype('int32')))
        fluorometer_gain_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Fluorometer_Gain', parameter_context=fluorometer_gain_ctxt.dump()))

        turbidity_value_ctxt = ParameterContext('Turbidity_Value', param_type=QuantityType(value_encoding=np.dtype('float32')))
        turbidity_value_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Turbidity_Value', parameter_context=turbidity_value_ctxt.dump()))

        turbidity_gain_ctxt = ParameterContext('Turbidity_Gain', param_type=QuantityType(value_encoding=np.dtype('int32')))
        turbidity_gain_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Turbidity_Gain', parameter_context=turbidity_gain_ctxt.dump()))

        optode_oxygen_ctxt = ParameterContext('Optode_Oxygen', param_type=QuantityType(value_encoding=np.dtype('float32')))
        optode_oxygen_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Optode_Oxygen', parameter_context=optode_oxygen_ctxt.dump()))

        optode_temp_ctxt = ParameterContext('Optode_Temp', param_type=QuantityType(value_encoding=np.dtype('float32')))
        optode_temp_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Optode_Temp', parameter_context=optode_temp_ctxt.dump()))

        par_value_ctxt = ParameterContext('Par_Value', param_type=QuantityType(value_encoding=np.dtype('float32')))
        par_value_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Par_Value', parameter_context=par_value_ctxt.dump()))

        puck_scatter_ctxt = ParameterContext('Puck_Scatter', param_type=QuantityType(value_encoding=np.dtype('int16')))
        puck_scatter_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Puck_Scatter', parameter_context=puck_scatter_ctxt.dump()))

        puck_chla_ctxt = ParameterContext('Puck_Chla', param_type=QuantityType(value_encoding=np.dtype('int16')))
        puck_chla_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Puck_Chla', parameter_context=puck_chla_ctxt.dump()))

        puck_cdom_ctxt = ParameterContext('Puck_CDOM', param_type=QuantityType(value_encoding=np.dtype('int16')))
        puck_cdom_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Puck_CDOM', parameter_context=puck_cdom_ctxt.dump()))

        biosuite_scatter_ctxt = ParameterContext('BioSuite_Scatter', param_type=QuantityType(value_encoding=np.dtype('int16')))
        biosuite_scatter_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='BioSuite_Scatter', parameter_context=biosuite_scatter_ctxt.dump()))

        biosuite_chla_ctxt = ParameterContext('BioSuite_Chla', param_type=QuantityType(value_encoding=np.dtype('int16')))
        biosuite_chla_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='BioSuite_Chla', parameter_context=biosuite_chla_ctxt.dump()))

        biosuite_cdom_ctxt = ParameterContext('BioSuite_CDOM', param_type=QuantityType(value_encoding=np.dtype('int16')))
        biosuite_cdom_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='BioSuite_CDOM', parameter_context=biosuite_cdom_ctxt.dump()))

        biosuite_temp_ctxt = ParameterContext('BioSuite_Temp', param_type=QuantityType(value_encoding=np.dtype('int16')))
        biosuite_temp_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='BioSuite_Temp', parameter_context=biosuite_temp_ctxt.dump()))

        biosuite_par_ctxt = ParameterContext('BioSuite_Par', param_type=QuantityType(value_encoding=np.dtype('int16')))
        biosuite_par_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='BioSuite_Par', parameter_context=biosuite_par_ctxt.dump()))

        flbb_chla_ctxt = ParameterContext('FLBB_Chla', param_type=QuantityType(value_encoding=np.dtype('int16')))
        flbb_chla_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='FLBB_Chla', parameter_context=flbb_chla_ctxt.dump()))

        flbb_turb_ctxt = ParameterContext('FLBB_Turb', param_type=QuantityType(value_encoding=np.dtype('int16')))
        flbb_turb_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='FLBB_Turb', parameter_context=flbb_turb_ctxt.dump()))

        flbb_temp_ctxt = ParameterContext('FLBB_Temp', param_type=QuantityType(value_encoding=np.dtype('int16')))
        flbb_temp_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='FLBB_Temp', parameter_context=flbb_temp_ctxt.dump()))

        return context_ids

    def create_external_dataset(self):
        ds_name = 'hypm_01_wfp_eng_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        dset.dataset_description.parameters['base_url'] = 'test_data'
        dset.dataset_description.parameters['list_pattern'] = 'E*.HEX'

        return self.data_acquisition_management.create_external_dataset(external_dataset=dset)

    def get_dvr_config(self):
        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.data.handlers.hypm_data_handler',
            'dvr_cls': 'HYPMDataHandler',
            'dh_cfg': {
                'parser_mod': 'ion.agents.data.handlers.hypm_data_handler',
                'parser_cls': 'HYPM_01_WFP_ENGParser',
                'stream_id': self.stream_id,
                'stream_route': self.route,
                'stream_def': self.stream_def_id,
                'data_producer_id': 'hypm_eng_data_producer_id',
                'max_records': 4,
                'TESTING': True,
                }
        }
        return DVR_CONFIG

    def get_retrieve_client(self, dataset_id=''):
        replay_data = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
