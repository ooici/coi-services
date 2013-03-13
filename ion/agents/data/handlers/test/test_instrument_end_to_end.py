from ion.services.dm.utility.granule_utils import time_series_domain
from ion.core.includes.mi import DriverEvent
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.objects import DataProduct, AgentCommand, ExternalDataset
from interface.objects import ContactInformation, UpdateDescription, DatasetDescription
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import QuantityType
from pyon.public import RT, log, PRED
from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.util.context import LocalContextMixin
import numpy as np
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id = ''
    process_type = ''


class HypmBase(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.pubsub_management    = PubsubManagementServiceClient()
        self.dataset_management   = DatasetManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.data_acquisition_management = DataAcquisitionManagementServiceClient()
        self.ingestion_management = IngestionManagementServiceClient()
        self.resource_registry       = self.container.resource_registry
        self.context_ids = self.build_param_contexts()
        self.setup_resources()

    def build_param_contexts(self):
        raise NotImplementedError('build_param_contexts must be implemented in child classes')

    def create_external_dataset(self):
        raise NotImplementedError('create_external_dataset must be implemented in child classes')

    def get_dvr_config(self):
        raise NotImplementedError('get_dvr_config must be implemented in child classes')

    def test_end_to_end(self):
        self.pdict_id = self.create_parameter_dict(self.name)
        self.stream_def_id = self.create_stream_def(self.name, self.pdict_id)
        self.data_product_id = self.create_data_product(self.name, self.description, self.stream_def_id)
        self.dataset_id = self.get_dataset_id(self.data_product_id)
        self.stream_id, self.route = self.get_stream_id_and_route(self.data_product_id)
        self.external_dataset_id = self.create_external_dataset()
        self.start_agent()

    def create_parameter_dict(self, name=''):
        return self.dataset_management.create_parameter_dictionary(name=name, parameter_context_ids=self.context_ids, temporal_context='time')

    def create_stream_def(self, name='', pdict_id=''):
        return self.pubsub_management.create_stream_definition(name='hypm_01_wpf_ctd', parameter_dictionary_id=pdict_id)

    def create_data_product(self, name='', description='', stream_def_id=''):
        tdom, sdom = time_series_domain()
        tdom = tdom.dump()
        sdom = sdom.dump()
        dp_obj = DataProduct(
            name=name,
            description=description,
            processing_level_code='Parsed_Canonical',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id = self.data_product_management.create_data_product(data_product=dp_obj, stream_definition_id=stream_def_id)
        self.data_product_management.activate_data_product_persistence(data_product_id)
        return data_product_id

    def get_dataset_id(self, data_product_id=''):
        dataset_ids, assocs = self.resource_registry.find_objects(subject=data_product_id, predicate='hasDataset', id_only=True)
        return dataset_ids[0]

    def get_stream_id_and_route(self, data_product_id):
        stream_ids, _ = self.resource_registry.find_objects(data_product_id, PRED.hasStream, RT.Stream, id_only=True)
        stream_id = stream_ids[0]
        route = self.pubsub_management.read_stream_route(stream_id)
        return stream_id, route

    def start_agent(self):
        agent_config = {
            'driver_config': self.get_dvr_config(),
            'stream_config': {},
            'agent': {'resource_id': self.external_dataset_id},
            'test_mode': True
        }

        _ia_pid = self.container.spawn_process(
            name=self.EDA_NAME,
            module=self.EDA_MOD,
            cls=self.EDA_CLS,
            config=agent_config)

        _ia_client = ResourceAgentClient(self.external_dataset_id, process=FakeProcess())
        log.info('Got ia client %s.', str(_ia_client))

        cmd = AgentCommand(command=ResourceAgentEvent.INITIALIZE)
        _ia_client.execute_agent(cmd)
        cmd = AgentCommand(command=ResourceAgentEvent.GO_ACTIVE)
        _ia_client.execute_agent(cmd)
        cmd = AgentCommand(command=ResourceAgentEvent.RUN)
        _ia_client.execute_agent(cmd)
        cmd = AgentCommand(command=DriverEvent.ACQUIRE_SAMPLE)
        _ia_client.execute_resource(command=cmd)

@attr('INT', group='eoi')
class TestHypm_WPF_CTD(HypmBase):

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

        cnd_ctxt = ParameterContext('conductivity', param_type=QuantityType(value_encoding=np.dtype('float32')))
        cnd_ctxt.uom = 'mmho/cm'
        context_ids.append(self.dataset_management.create_parameter_context(name='conductivity', parameter_context=cnd_ctxt.dump()))

        temp_ctxt = ParameterContext('temperature', param_type=QuantityType(value_encoding=np.dtype('float32')))
        temp_ctxt.uom = 'degC'
        context_ids.append(self.dataset_management.create_parameter_context(name='temperature', parameter_context=temp_ctxt.dump()))

        press_ctxt = ParameterContext('pressure', param_type=QuantityType(value_encoding=np.dtype('float32')))
        press_ctxt.uom = 'decibars'
        context_ids.append(self.dataset_management.create_parameter_context(name='pressure', parameter_context=press_ctxt.dump()))

        oxy_ctxt = ParameterContext('oxygen', param_type=QuantityType(value_encoding=np.dtype('float32')))
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
                }
        }
        return DVR_CONFIG

@attr('INT', group='eoi')
class TestHypm_WPF_ACM(HypmBase):

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

        vela_ctxt = ParameterContext('VelA', param_type=QuantityType(value_encoding=np.dtype('int16')))
        vela_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='VelA', parameter_context=vela_ctxt.dump()))

        velb_ctxt = ParameterContext('VelB', param_type=QuantityType(value_encoding=np.dtype('int16')))
        velb_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='VelB', parameter_context=velb_ctxt.dump()))

        velc_ctxt = ParameterContext('VelC', param_type=QuantityType(value_encoding=np.dtype('int16')))
        velc_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='VelC', parameter_context=velc_ctxt.dump()))

        veld_ctxt = ParameterContext('VelD', param_type=QuantityType(value_encoding=np.dtype('int16')))
        veld_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='VelD', parameter_context=veld_ctxt.dump()))

        mx_ctxt = ParameterContext('Mx', param_type=QuantityType(value_encoding=np.dtype('int16')))
        mx_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Mx', parameter_context=mx_ctxt.dump()))

        my_ctxt = ParameterContext('My', param_type=QuantityType(value_encoding=np.dtype('int16')))
        my_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='My', parameter_context=my_ctxt.dump()))

        mz_ctxt = ParameterContext('Mz', param_type=QuantityType(value_encoding=np.dtype('int16')))
        mz_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Mz', parameter_context=mz_ctxt.dump()))

        pitch_ctxt = ParameterContext('Pitch', param_type=QuantityType(value_encoding=np.dtype('int16')))
        pitch_ctxt.uom = 'unknown'
        context_ids.append(self.dataset_management.create_parameter_context(name='Pitch', parameter_context=pitch_ctxt.dump()))

        roll_ctxt = ParameterContext('Roll', param_type=QuantityType(value_encoding=np.dtype('int16')))
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
                }
        }
        return DVR_CONFIG

#ingest_configs, _  = resource_registry.find_resources(restype=RT.IngestionConfiguration,id_only=True)

#ingest_config_id = ingest_configs[0]
#ingestion_management.persist_data_stream(stream_id=stream_id, ingestion_configuration_id=ingest_config_id, dataset_id=dataset_id)

# Create ExternalDataset

#pid = cc.spawn_process(
#	name='test_logger',
#	module='ion.processes.data.stream_granule_logger',
#	cls='StreamGranuleLogger',
#	config={'process': {'stream_id': stream_id}}
#)
#log.info('Started StreamGranuleLogger \'{0}\' subscribed to stream_id={1}'.format(pid, stream_id))

