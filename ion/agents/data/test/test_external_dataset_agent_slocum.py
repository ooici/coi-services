#!/usr/bin/env python

"""
@package ion.agents.data.test.test_external_dataset_agent_slocum
@file ion/agents/data/test/test_external_dataset_agent_slocum.py
@author Christopher Mueller
@brief
"""

# Import pyon first for monkey patching.
from pyon.public import log, IonObject
from pyon.ion.resource import PRED, RT
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.agents.data.test.test_external_dataset_agent import ExternalDatasetAgentTestBase, IonIntegrationTestCase
from nose.plugins.attrib import attr

#temp until stream defs are completed
from interface.services.dm.ipubsub_management_service import\
    PubsubManagementServiceClient

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType

import numpy


@attr('INT_LONG', group='now')
class TestExternalDatasetAgent_Slocum(ExternalDatasetAgentTestBase,
    IonIntegrationTestCase):
    DVR_CONFIG = {
        'dvr_mod': 'ion.agents.data.handlers.slocum_data_handler',
        'dvr_cls': 'SlocumDataHandler', }

    HIST_CONSTRAINTS_1 = {}

    HIST_CONSTRAINTS_2 = {}

    def _setup_resources(self):
        # TODO: some or all of this (or some variation) should move to DAMS'

        # Build the test resources for the dataset
        dms_cli = DatasetManagementServiceClient()
        dams_cli = DataAcquisitionManagementServiceClient()
        dpms_cli = DataProductManagementServiceClient()
        rr_cli = ResourceRegistryServiceClient()
        pubsub_cli = PubsubManagementServiceClient()

        eda = ExternalDatasetAgent(handler_module=self.DVR_CONFIG['dvr_mod'],
            handler_class=self.DVR_CONFIG['dvr_cls'])
        eda_id = dams_cli.create_external_dataset_agent(eda)

        eda_inst = ExternalDatasetAgentInstance()
        eda_inst_id = dams_cli.create_external_dataset_agent_instance(eda_inst, external_dataset_agent_id=eda_id)

        # Create and register the necessary resources/objects

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
        dprov.contact.individual_names_given = 'Christopher Mueller'
        dprov.contact.email = 'cmueller@asascience.com'

        # Create DataSource
        dsrc = DataSource(protocol_type='FILE', institution=Institution(), contact=ContactInformation())
        dsrc.connection_params['base_data_url'] = ''
        dsrc.contact.individual_names_given = 'Tim Giguere'
        dsrc.contact.email = 'tgiguere@asascience.com'

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
            'm_pitch', ]

        # Create DataSourceModel
        dsrc_model = DataSourceModel(name='slocum_model')
        #        dsrc_model.model = 'SLOCUM'
        dsrc_model.data_handler_module = 'N/A'
        dsrc_model.data_handler_class = 'N/A'

        ## Run everything through DAMS
        ds_id = dams_cli.create_external_dataset(external_dataset=dset)
        ext_dprov_id = dams_cli.create_external_data_provider(external_data_provider=dprov)
        ext_dsrc_id = dams_cli.create_data_source(data_source=dsrc)
        ext_dsrc_model_id = dams_cli.create_data_source_model(dsrc_model)

        # Register the ExternalDataset
        dproducer_id = dams_cli.register_external_data_set(external_dataset_id=ds_id)

        # Or using each method
        dams_cli.assign_data_source_to_external_data_provider(data_source_id=ext_dsrc_id, external_data_provider_id=ext_dprov_id)
        dams_cli.assign_data_source_to_data_model(data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id)
        dams_cli.assign_external_dataset_to_data_source(external_dataset_id=ds_id, data_source_id=ext_dsrc_id)
        dams_cli.assign_external_dataset_to_agent_instance(external_dataset_id=ds_id, agent_instance_id=eda_inst_id)
        #        dams_cli.assign_external_data_agent_to_agent_instance(external_data_agent_id=self.eda_id, agent_instance_id=self.eda_inst_id)

        #create temp streamdef so the data product can create the stream
        pc_list = []
        for pc_k, pc in self._create_parameter_dictionary().iteritems():
            pc_list.append(dms_cli.create_parameter_context(pc_k, pc[1].dump()))

        pdict_id = dms_cli.create_parameter_dictionary('slocum_param_dict', pc_list)

        streamdef_id = pubsub_cli.create_stream_definition(name="slocum_stream_def", description="stream def for slocum testing", parameter_dictionary_id=pdict_id)

        #        dpms_cli.create_data_product()

        # Generate the data product and associate it to the ExternalDataset

        tdom, sdom = time_series_domain()
        tdom, sdom = tdom.dump(), sdom.dump()

        dprod = IonObject(RT.DataProduct,
            name='slocum_parsed_product',
            description='parsed slocum product',
            temporal_domain=tdom,
            spatial_domain=sdom)

        dproduct_id = dpms_cli.create_data_product(data_product=dprod,
            stream_definition_id=streamdef_id)

        dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id)

        stream_id, assn = rr_cli.find_objects(subject=dproduct_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_id[0]

        log.info('Created resources: {0}'.format({'ExternalDataset': ds_id, 'ExternalDataProvider': ext_dprov_id, 'DataSource': ext_dsrc_id, 'DataSourceModel': ext_dsrc_model_id, 'DataProducer': dproducer_id, 'DataProduct': dproduct_id, 'Stream': stream_id}))

        # Create the logger for receiving publications
        _, stream_route, _ = self.create_stream_and_logger(name='slocum', stream_id=stream_id)

        self.EDA_RESOURCE_ID = ds_id
        self.EDA_NAME = ds_name
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING': True,
            'stream_id': stream_id,
            'stream_route': stream_route,
            'stream_def': streamdef_id,
            'external_dataset_res': dset,
            'data_producer_id': dproducer_id,  # CBM: Should this be put in the main body of the config - with mod & cls?
            'max_records': 20,
            }

    def _create_parameter_dictionary(self):
        pdict = ParameterDictionary()

        t_ctxt = ParameterContext('c_wpt_y_lmc', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_water_cond', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_y_lmc', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('u_hd_fin_ap_inflection_holdoff', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_m_present_time', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_leakdetect_voltage_forward', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_bb3slo_b660_scaled', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('c_science_send_all', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_gps_status', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_water_vx', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_water_vy', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('c_heading', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_fl3slo_chlor_units', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('u_hd_fin_ap_gain', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_vacuum', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('u_min_water_depth', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_gps_lat', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_veh_temp', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('f_fin_offset', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('u_hd_fin_ap_hardover_holdoff', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('c_alt_time', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_present_time', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_heading', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_bb3slo_b532_scaled', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_fl3slo_cdom_units', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_fin', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('x_cycle_overrun_in_ms', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_water_pressure', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('u_hd_fin_ap_igain', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_fl3slo_phyco_units', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_battpos', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_bb3slo_b470_scaled', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_lat', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_gps_lon', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_ctd41cp_timestamp', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_pressure', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('c_wpt_x_lmc', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('c_ballast_pumped', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('x_lmc_xy_source', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_lon', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_avg_speed', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('sci_water_temp', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('u_pitch_ap_gain', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_roll', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_tot_num_inflections', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_x_lmc', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('u_pitch_ap_deadband', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_final_water_vy', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_final_water_vx', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_water_depth', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_leakdetect_voltage', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('u_pitch_max_delta_battpos', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_coulomb_amphr', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        t_ctxt = ParameterContext('m_pitch', param_type=QuantityType(value_encoding=numpy.dtype('float32')))
        t_ctxt.uom = 'unknown'
        pdict.add_context(t_ctxt)

        return pdict
