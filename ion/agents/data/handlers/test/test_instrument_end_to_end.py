from ion.services.sa.acquisition.test.test_bulk_data_ingestion import TestBulkIngestBase
from interface.objects import ExternalDataset
from interface.objects import ContactInformation, UpdateDescription, DatasetDescription
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.event.event import EventSubscriber
from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import QuantityType, ArrayType
from pyon.public import RT, log, PRED, OT
from pyon.agent.agent import ResourceAgentClient, ResourceAgentEvent
from pyon.util.context import LocalContextMixin
import numpy as np
from nose.plugins.attrib import attr
from gevent.event import Event
from pyon.util.int_test import IonIntegrationTestCase


@attr('INT', group='eoi')
class TestHypm_WPF_CTD(TestBulkIngestBase, IonIntegrationTestCase):

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
class TestHypm_WPF_ACM(TestBulkIngestBase, IonIntegrationTestCase):

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
                'TESTING': True,
                }
        }
        return DVR_CONFIG

    def get_retrieve_client(self, dataset_id=''):
        replay_data = self.data_retriever.retrieve(dataset_id)
        rdt = RecordDictionaryTool.load_from_granule(replay_data)
        print rdt

@attr('INT', group='eoi')
class TestHypm_WPF_ENG(TestBulkIngestBase, IonIntegrationTestCase):

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
        print rdt
