from ion.services.sa.acquisition.test.test_bulk_data_ingestion import BulkIngestBase
from interface.objects import ExternalDataset, AgentCommand
from interface.objects import ContactInformation, UpdateDescription, DatasetDescription
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.event.event import EventSubscriber
from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import QuantityType, ArrayType
from pyon.public import RT, log, PRED, OT
from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.util.context import LocalContextMixin
import numpy as np
from nose.plugins.attrib import attr
from gevent.event import Event
from pyon.util.int_test import IonIntegrationTestCase
from pyon.ion.stream import StandaloneStreamSubscriber
from ion.services.sa.acquisition.test.test_bulk_data_ingestion import FakeProcess
from ion.core.includes.mi import DriverEvent

@attr('INT', group='eoi')
class TestPreloadThenLoadDataset(IonIntegrationTestCase):
    """ replicates the TestHypm_WPF_CTD test (same handler/parser/data file)
        but uses the preload system to define the ExternalDataset and related resources,
        then invokes services to perform the load
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        config = dict(op="load", scenario="BETA,NOSE", attachments="res/preload/r2_ioc/attachments", path='master')
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=config)
        self.pubsub = PubsubManagementServiceClient()

    def find_object_by_name(self, name, resource_type):
        objects,_ = self.container.resource_registry.find_resources(resource_type)
        self.assertGreaterEqual(len(objects), 1)
        filtered_objs = [obj for obj in objects if obj.name == name]
        self.assertEquals(len(filtered_objs), 1)
        return filtered_objs[0]

    def test_use_case(self):
        # setUp() has already started the container and performed the preload
        self.assert_dataset_loaded('Test External CTD Dataset') # make sure we have the ExternalDataset resources
        self.do_configure()                                     # update agent configuration with runtime parameters TODO: should be done by services?
        self.do_listen_for_incoming()                           # listen for any data being received from the dataset
        self.do_read_dataset()                                  # call services to load dataset
        self.assert_data_received()                             # check that data was received as expected
        self.do_shutdown()

    def assert_dataset_loaded(self, name):
        self.external_dataset = self.find_object_by_name(name, RT.ExternalDataset)
        rr = self.container.resource_registry
        obj,_ = rr.find_objects(subject=self.external_dataset._id, predicate=PRED.hasAgentInstance, object_type=RT.ExternalDatasetAgentInstance)
        self.agent_instance = obj[0]
        obj,_ = rr.find_objects(object_type=RT.ExternalDatasetAgent, predicate=PRED.hasAgentDefinition, subject=self.agent_instance._id)
        self.agent = obj[0]
        stream_definition_id = self.agent_instance.dataset_driver_config['dh_cfg']['stream_def']
        self.stream_definition = rr.read(stream_definition_id)
        data_producer_id = self.agent_instance.dataset_driver_config['dh_cfg']['data_producer_id']
        self.data_producer = rr.read(data_producer_id) #subject="", predicate="", object_type="", assoc="", id_only=False)
        self.data_product = rr.read_object(object_type=RT.DataProduct, predicate=PRED.hasOutputProduct, subject=self.external_dataset._id)
        ids,_ = rr.find_objects(self.data_product._id, PRED.hasStream, RT.Stream, id_only=True)
        self.stream_id = ids[0]
        self.route = self.pubsub.read_stream_route(self.stream_id)

    def do_configure(self):
        self.agent_instance.dataset_agent_config['driver_config']['dh_cfg']['stream_id'] = self.stream_id
        self.agent_instance.dataset_agent_config['driver_config']['stream_id'] = self.stream_id
        self.agent_instance.dataset_agent_config['driver_config']['dh_cfg']['stream_route'] = self.route

    def do_listen_for_incoming(self):
        subscription_id = self.pubsub.create_subscription('validator', data_product_ids=[self.data_product._id])
        self.addCleanup(self.pubsub.delete_subscription, subscription_id)

        self.granule_capture = []
        self.granule_count = 0
        def on_granule(msg, route, stream_id):
            self.granule_count += 1
            if self.granule_count<5:
                self.granule_capture.append(msg)
        validator = StandaloneStreamSubscriber('validator', callback=on_granule)
        validator.start()
        self.addCleanup(validator.stop)

        self.pubsub.activate_subscription(subscription_id)
        self.addCleanup(self.pubsub.deactivate_subscription, subscription_id)

    def do_read_dataset(self):
        # WORKING TEST:
        #current config {'dh_cfg': {'TESTING': True,
        #                           'data_producer_id': 'hypm_ctd_data_producer_id',
        #                           'max_records': 4,
        #                           'parser_cls': 'HYPM_01_WFP_CTDParser',
        #                           'parser_mod': 'ion.agents.data.handlers.hypm_data_handler',
        #                           'stream_def': '76d86451011c4bc1b62975301ff2f6e6',
        #                           'stream_id': '85da7a7f7322467f96b67cc9e0cd4f2a',
        #                           'stream_route': <interface.objects.StreamRoute object at 0x108e0b050>},
        #               'dvr_cls': 'HYPMDataHandler',
        #               'dvr_mod': 'ion.agents.data.handlers.hypm_data_handler'}

        # MY CONFIG:
        #'driver_config': {'dh_cfg': {'data_producer_id': 'b455e2648f8f48c3987d73369b5f1e61',
        #                             'stream_def': 'f1f20512cd6e4bbc926e3c7e1b13e735'},
        #                  'dvr_cls': 'ExternalDatasetAgent',
        #                  'dvr_mod': 'ion.agents.data.external_dataset_agent',
        #                  'key2': 'value2',
        #                  'key3': 'value3'},

        import pprint
        log.info('launching agent with config: %s', pprint.pformat(self.agent_instance.dataset_agent_config))
        self.container.spawn_process(name=self.agent_instance.name,
            module=self.agent.handler_module,
            cls=self.agent.handler_class,
            config=self.agent_instance.dataset_agent_config)
        #
        # should i wait for process (above) to start
        # before launching client (below)?
        #
        self.client = ResourceAgentClient(self.external_dataset._id, process=FakeProcess())
        self.client.execute_agent(AgentCommand(command=ResourceAgentEvent.INITIALIZE))
        self.client.execute_agent(AgentCommand(command=ResourceAgentEvent.GO_ACTIVE))
        self.client.execute_agent(AgentCommand(command=ResourceAgentEvent.RUN))
        self.client.execute_resource(command=AgentCommand(command=DriverEvent.START_AUTOSAMPLE))

    def assert_data_received(self):
        dataset_modified = Event()
        def cb2(*args, **kwargs):
            dataset_modified.set()
        es = EventSubscriber(event_type=OT.DeviceCommonLifecycleEvent, callback=cb2, origin='BaseDataHandler._acquire_sample')
        es.start()
        self.addCleanup(es.stop)

        #let it go for up to 120 seconds, then stop the agent and reset it
        dataset_modified.wait(120)
        self.assertTrue(self.granule_count>2)

        rdt = RecordDictionaryTool.load_from_granule(self.granule_capture[0])
        self.assertAlmostEqual(0, rdt['oxygen'][0], delta=0.01)
        self.assertAlmostEqual(309.77, rdt['pressure'][0], delta=0.01)
        self.assertAlmostEqual(37.9848, rdt['conductivity'][0], delta=0.01)
        self.assertAlmostEqual(9.5163, rdt['temp'][0], delta=0.01)
        self.assertAlmostEqual(1500353102, rdt['time'][0], delta=1)


    def do_shutdown(self):
        pass # might be nice to stop the driver...


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
        dset.dataset_description.parameters['list_pattern'] = 'C*.DAT'

        return self.data_acquisition_management.create_external_dataset(external_dataset=dset)

    def get_dvr_config(self):
        DVR_CONFIG = {
            'dvr_mod': 'ion.agents.data.handlers.sbe52_binary_parser',
            'dvr_cls': 'SBE52BinaryDataHandler',
            'dh_cfg': {
                'parser_mod': 'ion.agents.data.handlers.sbe52_binary_parser',
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
        self.assertIsNotNone(rdt['temperature'])



@attr('INT', group='eoi')
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

@attr('INT', group='eoi')
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

        ut_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64')))
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

@attr('INT', group='eoi')
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
        #print rdt['Time_Time']
        #print rdt['Core_Current']
