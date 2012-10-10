#!/usr/bin/env python

"""
@package ion.agents.data.test.test_external_dataset_agent_ruv
@file ion/agents/data/test/test_external_dataset_agent_ruv.py
@author Christopher Mueller
@brief 
"""

# Import pyon first for monkey patching.
from pyon.public import log, IonObject
from pyon.ion.resource import PRED, RT
#from ion.services.dm.utility.granule.taxonomy import TaxyTool
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource
from ion.services.dm.utility.granule_utils import CoverageCraft

from ion.agents.data.test.test_external_dataset_agent import ExternalDatasetAgentTestBase, IonIntegrationTestCase
from nose.plugins.attrib import attr

#temp until stream defs are completed
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum, MutabilityEnum
from ion.util.parameter_yaml_IO import get_param_dict
from coverage_model.coverage import GridDomain, GridShape, CRS

import numpy

@attr('INT_LONG', group='eoi')
class TestExternalDatasetAgent_Ruv(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.ruv_data_handler',
        'dvr_cls' : 'RuvDataHandler',
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
        dprov.contact.individual_names_given = 'Christopher Mueller'
        dprov.contact.email = 'cmueller@asascience.com'

        # Create DataSource
        dsrc = DataSource(protocol_type='FILE', institution=Institution(), contact=ContactInformation())
        dsrc.connection_params['base_data_url'] = ''
        dsrc.contact.individual_names_given = 'Tim Giguere'
        dsrc.contact.email = 'tgiguere@asascience.com'

        # Create ExternalDataset
        ds_name = 'ruv_test_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        dset.dataset_description.parameters['base_url'] = 'test_data/ruv/'
        dset.dataset_description.parameters['list_pattern'] = 'RDLi_SEAB_2011_08_24_1600.ruv'
        dset.dataset_description.parameters['date_pattern'] = '%Y %m %d %H %M'
        dset.dataset_description.parameters['date_extraction_pattern'] = 'RDLi_SEAB_([\d]{4})_([\d]{2})_([\d]{2})_([\d]{2})([\d]{2}).ruv'
        dset.dataset_description.parameters['temporal_dimension'] = None
        dset.dataset_description.parameters['zonal_dimension'] = None
        dset.dataset_description.parameters['meridional_dimension'] = None
        dset.dataset_description.parameters['vertical_dimension'] = None
        dset.dataset_description.parameters['variables'] = [
        ]

        # Create DataSourceModel
        dsrc_model = DataSourceModel(name='ruv_model')
        dsrc_model.model = 'RUV'
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

        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 1d spatial topology (station/trajectory)

        sdom = sdom.dump()
        tdom = tdom.dump()

        parameter_dictionary = get_param_dict('ctd_parsed_param_dict')

        parameter_dictionary = parameter_dictionary.dump()

        dprod = IonObject(RT.DataProduct,
            name='usgs_parsed_product',
            description='parsed usgs product',
            temporal_domain = tdom,
            spatial_domain = sdom)

        dprod = IonObject(RT.DataProduct,
            name='ruv_parsed_product',
            description='parsed ruv product',
            temporal_domain = tdom,
            spatial_domain = sdom)

        pdict = ParameterDictionary()

        t_ctxt = ParameterContext('data', param_type=QuantityType(value_encoding=numpy.dtype('int64')))
        t_ctxt.reference_frame = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 01-01-1970'
        pdict.add_context(t_ctxt)

        streamdef_id = pubsub_cli.create_stream_definition(name="temp", description="temp", parameter_dictionary=pdict.dump())

        # Generate the data product and associate it to the ExternalDataset
        dproduct_id = dpms_cli.create_data_product(data_product=dprod,
                                                    stream_definition_id=streamdef_id,
                                                    parameter_dictionary=parameter_dictionary)

        dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id)

        stream_id, assn = rr_cli.find_objects(subject=dproduct_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_id[0]

        log.info('Created resources: {0}'.format({'ExternalDataset':ds_id, 'ExternalDataProvider':ext_dprov_id, 'DataSource':ext_dsrc_id, 'DataSourceModel':ext_dsrc_model_id, 'DataProducer':dproducer_id, 'DataProduct':dproduct_id, 'Stream':stream_id}))

        #CBM: Eventually, probably want to group this crap somehow - not sure how yet...

        # Create the logger for receiving publications
        _, stream_route, _ = self.create_stream_and_logger(name='ruv',stream_id=stream_id)

        self.EDA_RESOURCE_ID = ds_id
        self.EDA_NAME = ds_name
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,
            'stream_route':stream_route,
            'stream_def':streamdef_id,
            'external_dataset_res':dset,
            'data_producer_id':dproducer_id,#CBM: Should this be put in the main body of the config - with mod & cls?
            'max_records':20,
        }



