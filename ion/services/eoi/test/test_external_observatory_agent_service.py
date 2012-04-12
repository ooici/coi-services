#!/usr/bin/env python

"""
@package eoi.agent.services.test
@file test_external_observatory_agent_service
@author Christopher Mueller
@brief 
"""

#from interface.services.eoi.iexternal_observatory_agent_service import ExternalObservatoryAgentServiceClient
#from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from pyon.agent.agent import ResourceAgentClient
from pyon.util.containers import DotDict
from pyon.public import log, PRED
from pyon.core.exception import IonException
from pyon.util.context import LocalContextMixin
from pyon.util.unit_test import PyonTestCase
from mock import Mock
from nose.plugins.attrib import attr
import unittest
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.eoi.external_observatory_agent_service import ExternalObservatoryAgentService
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, DataSourceModel, ExternalDataset, ExternalDataProvider, DataSource, Institution, ContactInformation, DatasetDescription, UpdateDescription, AgentCommand, DataProduct

__author__ = 'Christopher Mueller'
__licence__ = 'Apache 2.0'

@attr('UNIT',group='eoi')
class TestExternalObservatoryAgentService(PyonTestCase):
    
    def setUp(self):
        mock_clients = self._create_service_mock('external_observatory_agent')
        self.ext_obs_service = ExternalObservatoryAgentService()
        self.ext_obs_service.clients = mock_clients
        self.ext_obs_service.clients.pubsub_management = DotDict()
        self.ext_obs_service.clients.pubsub_management['XP'] = 'science.data'
        self.ext_obs_service.clients.pubsub_management['create_stream'] = Mock()
        self.ext_obs_service.clients.pubsub_management['create_subscription'] = Mock()
        self.ext_obs_service.clients.pubsub_management['register_producer'] = Mock()
        self.ext_obs_service.clients.pubsub_management['activate_subscription'] = Mock()
        self.ext_obs_service.clients.pubsub_management['read_subscription'] = Mock()
        self.ext_obs_service.container = DotDict()
        self.ext_obs_service.container['spawn_process'] = Mock()
        self.ext_obs_service.container['id'] = 'mock_container_id'
        self.ext_obs_service.container['proc_manager'] = DotDict()
        self.ext_obs_service.container.proc_manager['terminate_process'] = Mock()

        # CRUD Shortcuts
#        self.mock_rr_create = self.ext_obs_service.clients.resource_registry.create
#        self.mock_rr_read = self.ext_obs_service.clients.resource_registry.read
#        self.mock_rr_update = self.ext_obs_service.clients.resource_registry.update
#        self.mock_rr_delete = self.ext_obs_service.clients.resource_registry.delete
#        self.mock_rr_find = self.ext_obs_service.clients.resource_registry.find_objects
#        self.mock_rr_assoc = self.ext_obs_service.clients.resource_registry.find_associations
#        self.mock_rr_create_assoc = self.ext_obs_service.clients.resource_registry.create_association
#        self.mock_rr_del_assoc = self.ext_obs_service.clients.resource_registry.delete_association

        self.mock_pd_create = self.ext_obs_service.clients.process_dispatcher_service.create_process_definition
        self.mock_pd_read = self.ext_obs_service.clients.process_dispatcher_service.read_process_definition
        self.mock_pd_update = self.ext_obs_service.clients.process_dispatcher_service.update_process_definition
        self.mock_pd_delete = self.ext_obs_service.clients.process_dispatcher_service.delete_process_definition
        self.mock_pd_schedule = self.ext_obs_service.clients.process_dispatcher_service.schedule_process
        self.mock_pd_cancel = self.ext_obs_service.clients.process_dispatcher_service.cancel_process

        self.mock_ps_create_stream = self.ext_obs_service.clients.pubsub_management.create_stream
        self.mock_ps_create_sub = self.ext_obs_service.clients.pubsub_management.create_subscription
        self.mock_ps_register = self.ext_obs_service.clients.pubsub_management.register_producer
        self.mock_ps_activate = self.ext_obs_service.clients.pubsub_management.activate_subscription
        self.mock_ps_read_sub = self.ext_obs_service.clients.pubsub_management.read_subscription

        self.mock_cc_spawn = self.ext_obs_service.container.spawn_process
        self.mock_cc_terminate = self.ext_obs_service.container.proc_manager.terminate_process

    @unittest.skip("Not Working")
    def test_spawn_worker(self):

        #mocks
#        self.mock_cc_spawn.return_value = 'mock_pid'
#        self.mock_ps_read_sub.return_value = DotDict({'exchange_name':'mock_exchange'})
##        self.mock_rr_create.return_value = ('mock_eoas_id','junk')
#
#        #execution
#        res = self.ext_obs_service.spawn_worker('resource_id')
#        log.debug(res)
#        print res
#
#        #assertions
##        self.mock_ps_read_sub.assert_called_once_with(resource_id='resource_id')
#        self.assertTrue(False)
#        self.assertTrue(self.mock_cc_spawn.called)
#        self.assertEquals(res, 'mock_pid')
        pass


class FakeProcess(LocalContextMixin):
    name = ''
    id='someid'

@attr('INT', group='eoi')
class TestIntExternalObservatoryAgentService(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url(rel_url='res/deploy/r2eoi.yml')

#        self.eoas_cli = ExternalObservatoryAgentServiceClient()
#        self.rr_cli = ResourceRegistryServiceClient()
        self.dams_cli = DataAcquisitionManagementServiceClient()
        self.dpms_cli = DataProductManagementServiceClient()

        self._setup_ncom()
        self._setup_hfr()

#        eoas_proc = self.container.proc_manager.procs_by_name['external_data_agent_management']
#        log.debug("Got EOAS Process: %s" % eoas_proc)
        self._ncom_agt_cli = ResourceAgentClient(resource_id=self.ncom_ds_id, name='external_observatory_agent', process=FakeProcess())
        log.debug("Got a ResourceAgentClient: res_id=%s" % self._ncom_agt_cli.resource_id)

        self._hfr_agt_cli = ResourceAgentClient(resource_id=self.hfr_ds_id, name='external_observatory_agent', process=FakeProcess())
        log.debug("Got a ResourceAgentClient: res_id=%s" % self._hfr_agt_cli.resource_id)

    def _setup_ncom(self):
        # TODO: some or all of this (or some variation) should move to DAMS

        # Create and register the necessary resources/objects

        eda = ExternalDatasetAgent()
        eda_id = self.dams_cli.create_external_dataset_agent(eda)

        eda_inst = ExternalDatasetAgentInstance()
        eda_inst_id = self.dams_cli.create_external_dataset_agent_instance(eda_inst, external_dataset_agent_id=eda_id)


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
        dsrc_model.data_handler_module = "eoi.agent.handler.dap_external_data_handler"
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
#        self.dams_cli.assign_eoi_resources(external_data_provider_id=ext_dprov_id, data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id, external_dataset_id=ds_id, external_data_agent_id=eda_id, agent_instance_id=eda_inst_id)


        # Or using each method
        self.dams_cli.assign_data_source_to_external_data_provider(data_source_id=ext_dsrc_id, external_data_provider_id=ext_dprov_id)
        self.dams_cli.assign_data_source_to_data_model(data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id)
        self.dams_cli.assign_external_dataset_to_data_source(external_dataset_id=ds_id, data_source_id=ext_dsrc_id)
        self.dams_cli.assign_external_dataset_to_agent_instance(external_dataset_id=ds_id, agent_instance_id=eda_inst_id)
#        self.dams_cli.assign_external_dataset_agent_to_data_model(external_data_agent_id=eda_id, data_source_model_id=ext_dsrc_model_id)
#        self.dams_cli.assign_external_data_agent_to_agent_instance(external_data_agent_id=eda_id, agent_instance_id=eda_inst_id)

        # Generate the data product and associate it to the ExternalDataset
        dprod = DataProduct(name='ncom_product', description='raw ncom product')
        dproduct_id = self.dpms_cli.create_data_product(data_product=dprod)

        self.dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id, create_stream=True)

    def _setup_hfr(self):
        # TODO: some or all of this (or some variation) should move to DAMS

        # Create and register the necessary resources/objects

        eda = ExternalDatasetAgent()
        eda_id = self.dams_cli.create_external_dataset_agent(eda)

        eda_inst = ExternalDatasetAgentInstance()
        eda_inst_id = self.dams_cli.create_external_dataset_agent_instance(eda_inst, external_dataset_agent_id=eda_id)

        # Create DataProvider
        dprov = ExternalDataProvider(institution=Institution(), contact=ContactInformation())
#        dprov.institution.name = "HFR UCSD"

        # Create DataSource
        dsrc = DataSource(protocol_type="DAP", institution=Institution(), contact=ContactInformation())
        dsrc.connection_params["base_data_url"] = "http://hfrnet.ucsd.edu:8080/thredds/dodsC/"

        # Create ExternalDataset
        dset = ExternalDataset(name="UCSD HFR", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        dset.dataset_description.parameters["dataset_path"] = "HFRNet/USEGC/6km/hourly/RTV"
#        dset.dataset_description.parameters["dataset_path"] = "test_data/hfr.nc"
        dset.dataset_description.parameters["temporal_dimension"] = "time"
        dset.dataset_description.parameters["zonal_dimension"] = "lon"
        dset.dataset_description.parameters["meridional_dimension"] = "lat"

        # Create DataSourceModel
        dsrc_model = DataSourceModel(name="dap_model")
        dsrc_model.model = "DAP"
        dsrc_model.data_handler_module = "eoi.agent.handler.dap_external_data_handler"
        dsrc_model.data_handler_class = "DapExternalDataHandler"

        ## Run everything through DAMS
        ds_id = self.hfr_ds_id = self.dams_cli.create_external_dataset(external_dataset=dset)
        ext_dprov_id = self.dams_cli.create_external_data_provider(external_data_provider=dprov)
        ext_dsrc_id = self.dams_cli.create_data_source(data_source=dsrc)
        ext_dsrc_model_id = self.dams_cli.create_data_source_model(dsrc_model)

        # Register the ExternalDataset
        dproducer_id = self.dams_cli.register_external_data_set(external_dataset_id=ds_id)

        ## Associate everything
        # Convenience method
#        self.dams_cli.assign_eoi_resources(external_data_provider_id=ext_dprov_id, data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id, external_dataset_id=ds_id, external_data_agent_id=eda_id, agent_instance_id=eda_inst_id)

        # Or using each method
        self.dams_cli.assign_data_source_to_external_data_provider(data_source_id=ext_dsrc_id, external_data_provider_id=ext_dprov_id)
        self.dams_cli.assign_data_source_to_data_model(data_source_id=ext_dsrc_id, data_source_model_id=ext_dsrc_model_id)
        self.dams_cli.assign_external_dataset_to_data_source(external_dataset_id=ds_id, data_source_id=ext_dsrc_id)
        self.dams_cli.assign_external_dataset_to_agent_instance(external_dataset_id=ds_id, agent_instance_id=eda_inst_id)
#        self.dams_cli.assign_external_dataset_agent_to_data_model(external_data_agent_id=eda_id, data_source_model_id=ext_dsrc_model_id)
#        self.dams_cli.assign_external_data_agent_to_agent_instance(external_data_agent_id=eda_id, agent_instance_id=eda_inst_id)

        # Generate the data product and associate it to the ExternalDataset
        dprod = DataProduct(name='hfr_product', description='raw hfr product')
        dproduct_id = self.dpms_cli.create_data_product(data_product=dprod)

        self.dams_cli.assign_data_product(input_resource_id=ds_id, data_product_id=dproduct_id, create_stream=True)



########## Tests ##########

    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_get_capabilities(self):
        # Get all the capabilities
        caps = self._ncom_agt_cli.get_capabilities()
        log.debug("all capabilities: %s" % caps)
        lst=[['RES_CMD', 'acquire_data'], ['RES_CMD', 'acquire_data_by_request'],
            ['RES_CMD', 'acquire_new_data'], ['RES_CMD', 'close'], ['RES_CMD', 'compare'],
            ['RES_CMD', 'get_attributes'], ['RES_CMD', 'get_fingerprint'], ['RES_CMD', 'get_status'],
            ['RES_CMD', 'has_new_data']]
        self.assertEquals(caps, lst)

        caps = self._ncom_agt_cli.get_capabilities(capability_types=['RES_CMD'])
        log.debug("resource commands: %s" % caps)
        lst=[['RES_CMD', 'acquire_data'], ['RES_CMD', 'acquire_data_by_request'],
            ['RES_CMD', 'acquire_new_data'], ['RES_CMD', 'close'], ['RES_CMD', 'compare'],
            ['RES_CMD', 'get_attributes'], ['RES_CMD', 'get_fingerprint'], ['RES_CMD', 'get_status'],
            ['RES_CMD', 'has_new_data']]
        self.assertEquals(caps, lst)

        caps = self._ncom_agt_cli.get_capabilities(capability_types=['RES_PAR'])
        log.debug("resource commands: %s" % caps)
        self.assertEqual(type(caps), list)

        caps = self._ncom_agt_cli.get_capabilities(capability_types=['AGT_CMD'])
        log.debug("resource commands: %s" % caps)
        self.assertEqual(type(caps), list)

        caps = self._ncom_agt_cli.get_capabilities(capability_types=['AGT_PAR'])
        log.debug("resource commands: %s" % caps)
        self.assertEqual(type(caps), list)

    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_execute_get_attrs(self):
        cmd = AgentCommand(command_id="111", command="get_attributes")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._ncom_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)

    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_execute_get_fingerprint(self):
        cmd = AgentCommand(command_id="111", command="get_fingerprint")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._ncom_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)

    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_execute_single_worker(self):
        cmd = AgentCommand(command_id="111", command="get_attributes")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._ncom_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)

        cmd = AgentCommand(command_id="112", command="get_fingerprint")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._ncom_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)

    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_execute_multi_worker(self):
        cmd = AgentCommand(command_id="111", command="has_new_data")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._ncom_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)

        cmd = AgentCommand(command_id="111", command="has_new_data")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._hfr_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)

        cmd = AgentCommand(command_id="112", command="get_fingerprint")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._ncom_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)

        cmd = AgentCommand(command_id="112", command="get_fingerprint")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._hfr_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)
        self.assertTrue(type(ret.result), dict)

    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_execute_acquire_data(self):
        cmd = AgentCommand(command_id="113", command="acquire_data")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._ncom_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)

    @unittest.skip("Currently broken due to resource/agent refactorings")
    def test_execute_acquire_new_data(self):
        cmd = AgentCommand(command_id="113", command="acquire_new_data")
        log.debug("Execute AgentCommand: %s" % cmd)
        ret = self._ncom_agt_cli.execute(cmd)
        log.debug("Returned: %s" % ret)
        self.assertEquals(ret.status, 0)

    @unittest.skip("Underlying method not yet implemented")
    def test_set_param(self):
        res_id = self.ncom_ds_id

        log.debug("test_get_worker with res_id: %s" % res_id)
        res = self.eoas_cli.get_worker(res_id)

        with self.assertRaises(IonException):
            self.eoas_cli.set_param(resource_id=res_id, name="param", value="value")

    @unittest.skip("Underlying method not yet implemented")
    def test_get_param(self):
        res_id = self.ncom_ds_id

        log.debug("test_get_worker with res_id: %s" % res_id)
        res = self.eoas_cli.get_worker(res_id)

        with self.assertRaises(IonException):
            self.eoas_cli.get_param(resource_id=res_id, name="param")

    @unittest.skip("Underlying method not yet implemented")
    def test_execute_agent(self):
        res_id = self.ncom_ds_id

        log.debug("test_get_worker with res_id: %s" % res_id)
        res = self.eoas_cli.get_worker(res_id)

        with self.assertRaises(IonException):
            self.eoas_cli.execute_agent(resource_id=res_id)

    @unittest.skip("Underlying method not yet implemented")
    def test_set_agent_param(self):
        res_id = self.ncom_ds_id

        log.debug("test_get_worker with res_id: %s" % res_id)
        res = self.eoas_cli.get_worker(res_id)

        with self.assertRaises(IonException):
            self.eoas_cli.set_agent_param(resource_id=res_id, name="param", value="value")

    @unittest.skip("Underlying method not yet implemented")
    def test_get_agent_param(self):
        res_id = self.ncom_ds_id

        log.debug("test_get_worker with res_id: %s" % res_id)
        res = self.eoas_cli.get_worker(res_id)

        with self.assertRaises(IonException):
            self.eoas_cli.get_agent_param(resource_id=res_id, name="param")
