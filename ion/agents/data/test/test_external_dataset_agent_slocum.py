#!/usr/bin/env python

"""
@package ion.agents.data.test.test_external_dataset_agent_slocum
@file ion/agents/data/test/test_external_dataset_agent_slocum.py
@author Christopher Mueller
@brief 
"""

# Import pyon first for monkey patching.
from pyon.public import log
from pyon.ion.resource import PRED, RT
from pyon.ion.granule.taxonomy import TaxyTool
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource

from ion.agents.data.test.test_external_dataset_agent import ExternalDatasetAgentTestBase, IonIntegrationTestCase
from nose.plugins.attrib import attr

#temp until stream defs are completed
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

@attr('INT_LONG', group='eoi')
class TestExternalDatasetAgent_Slocum(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.slocum_data_handler',
        'dvr_cls' : 'SlocumDataHandler',
        }

    HIST_CONSTRAINTS_1 = {}

    HIST_CONSTRAINTS_2 = {}

    def _setup_resources(self):
        # TODO: some or all of this (or some variation) should move to DAMS'

        # Build the test resources for the dataset
        dams_cli = DataAcquisitionManagementServiceClient()
        dpms_cli = DataProductManagementServiceClient()
        rr_cli = ResourceRegistryServiceClient()
        pubsub_cli = PubsubManagementServiceClient()

        eda = ExternalDatasetAgent()
        eda_id = dams_cli.create_external_dataset_agent(eda)

        eda_inst = ExternalDatasetAgentInstance()
        eda_inst_id = dams_cli.create_external_dataset_agent_instance(eda_inst, external_dataset_agent_id=eda_id)

        # Create and register the necessary resources/objects

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
        dprov.contact.name = 'Christopher Mueller'
        dprov.contact.email = 'cmueller@asascience.com'

        # Create DataSource
        dsrc = DataSource(protocol_type='FILE', institution=Institution(), contact=ContactInformation())
        dsrc.connection_params['base_data_url'] = ''
        dsrc.contact.name='Tim Giguere'
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
            'm_pitch',
        ]

        # Create DataSourceModel
        dsrc_model = DataSourceModel(name='slocum_model')
        dsrc_model.model = 'SLOCUM'
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
        streamdef_id = pubsub_cli.create_stream_definition(name="temp", description="temp")

        # Generate the data product and associate it to the ExternalDataset
        dprod = DataProduct(name='slocum_parsed_product', description='parsed slocum product')
        dproduct_id = dpms_cli.create_data_product(data_product=dprod, stream_definition_id=streamdef_id)

        dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id)

        stream_id, assn = rr_cli.find_objects(subject=dproduct_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_id[0]

        log.info('Created resources: {0}'.format({'ExternalDataset':ds_id, 'ExternalDataProvider':ext_dprov_id, 'DataSource':ext_dsrc_id, 'DataSourceModel':ext_dsrc_model_id, 'DataProducer':dproducer_id, 'DataProduct':dproduct_id, 'Stream':stream_id}))

        #CBM: Use CF standard_names

        ttool = TaxyTool()

        ttool.add_taxonomy_set('c_wpt_y_lmc'),
        ttool.add_taxonomy_set('sci_water_cond'),
        ttool.add_taxonomy_set('m_y_lmc'),
        ttool.add_taxonomy_set('u_hd_fin_ap_inflection_holdoff'),
        ttool.add_taxonomy_set('sci_m_present_time'),
        ttool.add_taxonomy_set('m_leakdetect_voltage_forward'),
        ttool.add_taxonomy_set('sci_bb3slo_b660_scaled'),
        ttool.add_taxonomy_set('c_science_send_all'),
        ttool.add_taxonomy_set('m_gps_status'),
        ttool.add_taxonomy_set('m_water_vx'),
        ttool.add_taxonomy_set('m_water_vy'),
        ttool.add_taxonomy_set('c_heading'),
        ttool.add_taxonomy_set('sci_fl3slo_chlor_units'),
        ttool.add_taxonomy_set('u_hd_fin_ap_gain'),
        ttool.add_taxonomy_set('m_vacuum'),
        ttool.add_taxonomy_set('u_min_water_depth'),
        ttool.add_taxonomy_set('m_gps_lat'),
        ttool.add_taxonomy_set('m_veh_temp'),
        ttool.add_taxonomy_set('f_fin_offset'),
        ttool.add_taxonomy_set('u_hd_fin_ap_hardover_holdoff'),
        ttool.add_taxonomy_set('c_alt_time'),
        ttool.add_taxonomy_set('m_present_time'),
        ttool.add_taxonomy_set('m_heading'),
        ttool.add_taxonomy_set('sci_bb3slo_b532_scaled'),
        ttool.add_taxonomy_set('sci_fl3slo_cdom_units'),
        ttool.add_taxonomy_set('m_fin'),
        ttool.add_taxonomy_set('x_cycle_overrun_in_ms'),
        ttool.add_taxonomy_set('sci_water_pressure'),
        ttool.add_taxonomy_set('u_hd_fin_ap_igain'),
        ttool.add_taxonomy_set('sci_fl3slo_phyco_units'),
        ttool.add_taxonomy_set('m_battpos'),
        ttool.add_taxonomy_set('sci_bb3slo_b470_scaled'),
        ttool.add_taxonomy_set('m_lat'),
        ttool.add_taxonomy_set('m_gps_lon'),
        ttool.add_taxonomy_set('sci_ctd41cp_timestamp'),
        ttool.add_taxonomy_set('m_pressure'),
        ttool.add_taxonomy_set('c_wpt_x_lmc'),
        ttool.add_taxonomy_set('c_ballast_pumped'),
        ttool.add_taxonomy_set('x_lmc_xy_source'),
        ttool.add_taxonomy_set('m_lon'),
        ttool.add_taxonomy_set('m_avg_speed'),
        ttool.add_taxonomy_set('sci_water_temp'),
        ttool.add_taxonomy_set('u_pitch_ap_gain'),
        ttool.add_taxonomy_set('m_roll'),
        ttool.add_taxonomy_set('m_tot_num_inflections'),
        ttool.add_taxonomy_set('m_x_lmc'),
        ttool.add_taxonomy_set('u_pitch_ap_deadband'),
        ttool.add_taxonomy_set('m_final_water_vy'),
        ttool.add_taxonomy_set('m_final_water_vx'),
        ttool.add_taxonomy_set('m_water_depth'),
        ttool.add_taxonomy_set('m_leakdetect_voltage'),
        ttool.add_taxonomy_set('u_pitch_max_delta_battpos'),
        ttool.add_taxonomy_set('m_coulomb_amphr'),
        ttool.add_taxonomy_set('m_pitch'),

        #CBM: Eventually, probably want to group this crap somehow - not sure how yet...

        # Create the logger for receiving publications
        self.create_stream_and_logger(name='slocum',stream_id=stream_id)

        self.EDA_RESOURCE_ID = ds_id
        self.EDA_NAME = ds_name
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,
            'external_dataset_res':dset,
            'taxonomy':ttool.dump(),
            'data_producer_id':dproducer_id,#CBM: Should this be put in the main body of the config - with mod & cls?
            'max_records':20,
        }



