#!/usr/bin/env python

"""
@package ion.agents.eoi.test
@file test_external_observatory_agent
@author Christopher Mueller
@brief 
"""

__author__ = 'Christopher Mueller'
__licence__ = 'Apache 2.0'

#from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.util.context import LocalContextMixin
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from pyon.agent.agent import ResourceAgentClient
from pyon.public import log, PRED
from mock import Mock
import unittest
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, DataSourceModel, ExternalDataset, ExternalDataProvider, DataSource, Institution, ContactInformation, DatasetDescription, UpdateDescription, AgentCommand, DataProduct
from interface.messages import external_observatory_agent_execute_in, external_observatory_agent_get_capabilities_in

class FakeProcess(LocalContextMixin):
    name = ''
    id='someid'

@attr('INT', group='eoi')
class TestIntExternalObservatoryAgent(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url(rel_url='res/deploy/r2eoi.yml')

        self.dams_cli = DataAcquisitionManagementServiceClient()
        self.dpms_cli = DataProductManagementServiceClient()

        eda = ExternalDatasetAgent()
        self.eda_id = self.dams_cli.create_external_dataset_agent(eda)

        eda_inst = ExternalDatasetAgentInstance()
        self.eda_inst_id = self.dams_cli.create_external_dataset_agent_instance(eda_inst, external_dataset_agent_id=self.eda_id)


        self._setup_ncom()
        proc_name = self.ncom_ds_id+'_worker'
        config = {}
        config['process']={'name':proc_name,'type':'agent'}
        config['process']['eoa']={'dataset_id':self.ncom_ds_id}
        pid = self.container.spawn_process(name=proc_name, module='ion.agents.eoi.external_observatory_agent', cls='ExternalObservatoryAgent', config=config)

        queue_id = "%s.%s" % (self.container.id, pid)

        log.debug("Spawned worker process ==> proc_name: %s\tproc_id: %s\tqueue_id: %s" % (proc_name, pid, queue_id))

        self._agent_cli = ResourceAgentClient(self.ncom_ds_id, name=pid, process=FakeProcess())
        log.debug("Got a ResourceAgentClient: res_id=%s" % self._agent_cli.resource_id)

    def _setup_ncom(self):
        # TODO: some or all of this (or some variation) should move to DAMS

        # Create and register the necessary resources/objects

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
        #        dprov.institution.name = "OOI CGSN"
        dprov.contact.name = "Robert Weller"
        dprov.contact.email = "rweller@whoi.edu"

        # Create DataSource
        dsrc = DataSource(protocol_type="DAP", institution=Institution(), contact=ContactInformation())
        #        dsrc.connection_params["base_data_url"] = "http://ooi.whoi.edu/thredds/dodsC/"
        dsrc.connection_params["base_data_url"] = ""
        dsrc.contact.name="Rich Signell"
        dsrc.contact.email = "rsignell@usgs.gov"

        # Create ExternalDataset
        dset = ExternalDataset(name="test", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())

        #        dset.dataset_description.parameters["dataset_path"] = "ooi/AS02CPSM_R_M.nc"
        dset.dataset_description.parameters["dataset_path"] = "test_data/ncom.nc"
        dset.dataset_description.parameters["temporal_dimension"] = "time"
        dset.dataset_description.parameters["zonal_dimension"] = "lon"
        dset.dataset_description.parameters["meridional_dimension"] = "lat"

        # Create DataSourceModel
        dsrc_model = DataSourceModel(name="dap_model")
        dsrc_model.model = "DAP"
        dsrc_model.data_handler_module = "ion.agents.eoi.handler.dap_external_data_handler"
        dsrc_model.data_handler_class = "DapExternalDataHandler"

        ## Run everything through DAMS
        ds_id = self.ncom_ds_id = self.dams_cli.create_external_dataset(external_dataset=dset)
        ext_dprov_id = self.dams_cli.create_external_data_provider(external_data_provider=dprov)
        ext_dsrc_id = self.dams_cli.create_data_source(data_source=dsrc)
        ext_dsrc_model_id = self.dams_cli.create_data_source_model(dsrc_model)

        # Register the ExternalDataset
        dproducer_id = self.dams_cli.register_external_data_set(external_dataset_id=ds_id)

        ## Associate everything
        # Convenience method
#        self.dams_cli.assign_eoi_resources(external_data_provider_id=ext_dprov_id, data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id, external_dataset_id=ds_id, external_data_agent_id=self.eda_id, agent_instance_id=self.eda_inst_id)

        # Or using each method
        self.dams_cli.assign_data_source_to_external_data_provider(data_source_id=ext_dsrc_id, external_data_provider_id=ext_dprov_id)
        self.dams_cli.assign_data_source_to_data_model(data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id)
        self.dams_cli.assign_external_dataset_to_data_source(external_dataset_id=ds_id, data_source_id=ext_dsrc_id)
        self.dams_cli.assign_external_dataset_to_agent_instance(external_dataset_id=ds_id, agent_instance_id=self.eda_inst_id)
#        self.dams_cli.assign_external_data_agent_to_agent_instance(external_data_agent_id=self.eda_id, agent_instance_id=self.eda_inst_id)

        # Generate the data product and associate it to the ExternalDataset
        dprod = DataProduct(name='ncom_product', description='raw ncom product')
        dproduct_id = self.dpms_cli.create_data_product(data_product=dprod)

        self.dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id, create_stream=True)


########## Tests ##########

#    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_get_capabilities(self):
        # Get all the capabilities
        caps = self._agent_cli.get_capabilities()
        log.debug("all capabilities: %s" % caps)
        lst=[['RES_CMD', 'acquire_data'], ['RES_CMD', 'acquire_data_by_request'],
            ['RES_CMD', 'acquire_new_data'], ['RES_CMD', 'close'], ['RES_CMD', 'compare'],
            ['RES_CMD', 'get_attributes'], ['RES_CMD', 'get_fingerprint'], ['RES_CMD', 'get_status'],
            ['RES_CMD', 'has_new_data']]
        self.assertEquals(caps, lst)

        caps = self._agent_cli.get_capabilities(capability_types=['RES_CMD'])
        log.debug("resource commands: %s" % caps)
        lst=[['RES_CMD', 'acquire_data'], ['RES_CMD', 'acquire_data_by_request'],
            ['RES_CMD', 'acquire_new_data'], ['RES_CMD', 'close'], ['RES_CMD', 'compare'],
            ['RES_CMD', 'get_attributes'], ['RES_CMD', 'get_fingerprint'], ['RES_CMD', 'get_status'],
            ['RES_CMD', 'has_new_data']]
        self.assertEquals(caps, lst)

        caps = self._agent_cli.get_capabilities(capability_types=['RES_PAR'])
        log.debug("resource commands: %s" % caps)
        self.assertEqual(type(caps), list)

        caps = self._agent_cli.get_capabilities(capability_types=['AGT_CMD'])
        log.debug("resource commands: %s" % caps)
        self.assertEqual(type(caps), list)

        caps = self._agent_cli.get_capabilities(capability_types=['AGT_PAR'])
        log.debug("resource commands: %s" % caps)
        self.assertEqual(type(caps), list)

    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_execute_get_attrs(self):
        cmd = AgentCommand(command_id="111", command="get_attributes")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._agent_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)
