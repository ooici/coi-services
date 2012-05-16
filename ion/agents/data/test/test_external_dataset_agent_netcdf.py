#!/usr/bin/env python

"""
@package ion.agents.data.test.test_external_dataset_agent_netcdf
@file ion/agents/data/test/test_external_dataset_agent_netcdf
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

@attr('INT_EOI', group='eoi')
class TestExternalDatasetAgent_Netcdf(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.netcdf_data_handler',
        'dvr_cls' : 'NetcdfDataHandler',
        }

    HIST_CONSTRAINTS_1 = {
        'count':15,
        'temporal_slice':'(slice(4,10))',
        }

    HIST_CONSTRAINTS_2 = {
        'count':10,
        'temporal_slice':'(slice(0,16,2))',
        }

    NDC = {
        'BASE':'\x83\xa7content\xdc\x00\xc9\xceM\xa0\xf3\x00\xceM\xa2D\x80\xceM\xa3\x96\x00\xceM\xa4\xe7\x80\xceM\xa69\x00\xceM\xa7\x8a\x80\xceM\xa8\xdc\x00\xceM\xaa-\x80\xceM\xab\x7f\x00\xceM\xac\xd0\x80\xceM\xae"\x00\xceM\xafs\x80\xceM\xb0\xc5\x00\xceM\xb2\x16\x80\xceM\xb3h\x00\xceM\xb4\xb9\x80\xceM\xb6\x0b\x00\xceM\xb7\\\x80\xceM\xb8\xae\x00\xceM\xb9\xff\x80\xceM\xbbQ\x00\xceM\xbc\xa2\x80\xceM\xbd\xf4\x00\xceM\xbfE\x80\xceM\xc0\x97\x00\xceM\xc1\xe8\x80\xceM\xc3:\x00\xceM\xc4\x8b\x80\xceM\xc5\xdd\x00\xceM\xc7.\x80\xceM\xc8\x80\x00\xceM\xc9\xd1\x80\xceM\xcb#\x00\xceM\xcct\x80\xceM\xcd\xc6\x00\xceM\xcf\x17\x80\xceM\xd0i\x00\xceM\xd1\xba\x80\xceM\xd3\x0c\x00\xceM\xd4]\x80\xceM\xd5\xaf\x00\xceM\xd7\x00\x80\xceM\xd8R\x00\xceM\xd9\xa3\x80\xceM\xda\xf5\x00\xceM\xdcF\x80\xceM\xdd\x98\x00\xceM\xde\xe9\x80\xceM\xe0;\x00\xceM\xe1\x8c\x80\xceM\xe2\xde\x00\xceM\xe4/\x80\xceM\xe5\x81\x00\xceM\xe6\xd2\x80\xceM\xe8$\x00\xceM\xe9u\x80\xceM\xea\xc7\x00\xceM\xec\x18\x80\xceM\xedj\x00\xceM\xee\xbb\x80\xceM\xf0\r\x00\xceM\xf1^\x80\xceM\xf2\xb0\x00\xceM\xf4\x01\x80\xceM\xf5S\x00\xceM\xf6\xa4\x80\xceM\xf7\xf6\x00\xceM\xf9G\x80\xceM\xfa\x99\x00\xceM\xfb\xea\x80\xceM\xfd<\x00\xceM\xfe\x8d\x80\xceM\xff\xdf\x00\xceN\x010\x80\xceN\x02\x82\x00\xceN\x03\xd3\x80\xceN\x05%\x00\xceN\x06v\x80\xceN\x07\xc8\x00\xceN\t\x19\x80\xceN\nk\x00\xceN\x0b\xbc\x80\xceN\r\x0e\x00\xceN\x0e_\x80\xceN\x0f\xb1\x00\xceN\x11\x02\x80\xceN\x12T\x00\xceN\x13\xa5\x80\xceN\x14\xf7\x00\xceN\x16H\x80\xceN\x17\x9a\x00\xceN\x18\xeb\x80\xceN\x1a=\x00\xceN\x1b\x8e\x80\xceN\x1c\xe0\x00\xceN\x1e1\x80\xceN\x1f\x83\x00\xceN \xd4\x80\xceN"&\x00\xceN#w\x80\xceN$\xc9\x00\xceN&\x1a\x80\xceN\'l\x00\xceN(\xbd\x80\xceN*\x0f\x00\xceN+`\x80\xceN,\xb2\x00\xceN.\x03\x80\xceN/U\x00\xceN0\xa6\x80\xceN1\xf8\x00\xceN3I\x80\xceN4\x9b\x00\xceN5\xec\x80\xceN7>\x00\xceN8\x8f\x80\xceN9\xe1\x00\xceN;2\x80\xceN<\x84\x00\xceN=\xd5\x80\xceN?\'\x00\xceN@x\x80\xceNA\xca\x00\xceNC\x1b\x80\xceNDm\x00\xceNE\xbe\x80\xceNG\x10\x00\xceNHa\x80\xceNI\xb3\x00\xceNK\x04\x80\xceNLV\x00\xceNM\xa7\x80\xceNN\xf9\x00\xceNPJ\x80\xceNQ\x9c\x00\xceNR\xed\x80\xceNT?\x00\xceNU\x90\x80\xceNV\xe2\x00\xceNX3\x80\xceNY\x85\x00\xceNZ\xd6\x80\xceN\\(\x00\xceN]y\x80\xceN^\xcb\x00\xceN`\x1c\x80\xceNan\x00\xceNb\xbf\x80\xceNd\x11\x00\xceNeb\x80\xceNf\xb4\x00\xceNh\x05\x80\xceNiW\x00\xceNj\xa8\x80\xceNk\xfa\x00\xceNmK\x80\xceNn\x9d\x00\xceNo\xee\x80\xceNq@\x00\xceNr\x91\x80\xceNs\xe3\x00\xceNu4\x80\xceNv\x86\x00\xceNw\xd7\x80\xceNy)\x00\xceNzz\x80\xceN{\xcc\x00\xceN}\x1d\x80\xceN~o\x00\xceN\x7f\xc0\x80\xceN\x81\x12\x00\xceN\x82c\x80\xceN\x83\xb5\x00\xceN\x85\x06\x80\xceN\x86X\x00\xceN\x87\xa9\x80\xceN\x88\xfb\x00\xceN\x8aL\x80\xceN\x8b\x9e\x00\xceN\x8c\xef\x80\xceN\x8eA\x00\xceN\x8f\x92\x80\xceN\x90\xe4\x00\xceN\x925\x80\xceN\x93\x87\x00\xceN\x94\xd8\x80\xceN\x96*\x00\xceN\x97{\x80\xceN\x98\xcd\x00\xceN\x9a\x1e\x80\xceN\x9bp\x00\xceN\x9c\xc1\x80\xceN\x9e\x13\x00\xceN\x9fd\x80\xceN\xa0\xb6\x00\xceN\xa2\x07\x80\xceN\xa3Y\x00\xceN\xa4\xaa\x80\xceN\xa5\xfc\x00\xceN\xa7M\x80\xceN\xa8\x9f\x00\xa6header\x83\xa2nd\x01\xa5shape\x91\xcc\xc9\xa4type\xa5int32\xad__ion_array__\xc3',
        'WINDOWED':'encoded time array for the windowed sample'
    }

    def _setup_resources(self):
        # TODO: some or all of this (or some variation) should move to DAMS'

        # Build the test resources for the dataset
        dams_cli = DataAcquisitionManagementServiceClient()
        dpms_cli = DataProductManagementServiceClient()
        rr_cli = ResourceRegistryServiceClient()

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
        dsrc = DataSource(protocol_type='DAP', institution=Institution(), contact=ContactInformation())
        dsrc.connection_params['base_data_url'] = ''
        dsrc.contact.name='Tim Giguere'
        dsrc.contact.email = 'tgiguere@asascience.com'

        # Create ExternalDataset
        ds_name = 'usgs_test_dataset'
        dset = ExternalDataset(name=ds_name, dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        # The usgs.nc test dataset is a download of the R1 dataset found here:
        # http://thredds-test.oceanobservatories.org/thredds/dodsC/ooiciData/E66B1A74-A684-454A-9ADE-8388C2C634E5.ncml
        dset.dataset_description.parameters['dataset_path'] = 'test_data/usgs.nc'
        dset.dataset_description.parameters['temporal_dimension'] = 'time'
        dset.dataset_description.parameters['zonal_dimension'] = 'lon'
        dset.dataset_description.parameters['meridional_dimension'] = 'lat'
        dset.dataset_description.parameters['vertical_dimension'] = 'z'
        dset.dataset_description.parameters['variables'] = [
            'water_temperature',
            'streamflow',
            'water_temperature_bottom',
            'water_temperature_middle',
            'specific_conductance',
            'data_qualifier',
            ]

        # Create DataSourceModel
        dsrc_model = DataSourceModel(name='dap_model')
        dsrc_model.model = 'DAP'
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

        # Generate the data product and associate it to the ExternalDataset
        dprod = DataProduct(name='usgs_parsed_product', description='parsed usgs product')
        dproduct_id = dpms_cli.create_data_product(data_product=dprod)

        dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id, create_stream=True)

        stream_id, assn = rr_cli.find_objects(subject=dproduct_id, predicate=PRED.hasStream, object_type=RT.Stream, id_only=True)
        stream_id = stream_id[0]

        log.info('Created resources: {0}'.format({'ExternalDataset':ds_id, 'ExternalDataProvider':ext_dprov_id, 'DataSource':ext_dsrc_id, 'DataSourceModel':ext_dsrc_model_id, 'DataProducer':dproducer_id, 'DataProduct':dproduct_id, 'Stream':stream_id}))

        #CBM: Use CF standard_names

        ttool = TaxyTool()
        ttool.add_taxonomy_set('time','time')
        ttool.add_taxonomy_set('lon','longitude')
        ttool.add_taxonomy_set('lat','latitude')
        ttool.add_taxonomy_set('z','water depth')
        ttool.add_taxonomy_set('water_temperature', 'average water temperature')
        ttool.add_taxonomy_set('water_temperature_bottom','water temperature at bottom of water column')
        ttool.add_taxonomy_set('water_temperature_middle', 'water temperature at middle of water column')
        ttool.add_taxonomy_set('streamflow', 'flow velocity of stream')
        ttool.add_taxonomy_set('specific_conductance', 'specific conductance of water')
        ttool.add_taxonomy_set('data_qualifier','data qualifier flag')

        ttool.add_taxonomy_set('coords','This group contains coordinate parameters')
        ttool.add_taxonomy_set('data','This group contains data parameters')

        # Create the logger for receiving publications
        self.create_stream_and_logger(name='usgs',stream_id=stream_id)

        self.EDA_RESOURCE_ID = ds_id
        self.EDA_NAME = ds_name
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,
            'taxonomy':ttool.dump(),
            'data_producer_id':dproducer_id,#CBM: Should this be put in the main body of the config - with mod & cls?
            'max_records':4,
        }



