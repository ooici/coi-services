from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import IDataProductManagementService, DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.objects import HdfStorage, CouchStorage

from pyon.public import log
from nose.plugins.attrib import attr

from ion.services.dm.utility.granule.taxonomy import TaxyTool

from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG

# MI imports
from ion.agents.instrument.instrument_agent import InstrumentAgentState

from pyon.public import CFG
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS, PRED
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from coverage_model.parameter import ParameterDictionary
import unittest
import time

from ion.services.sa.product.data_product_impl import DataProductImpl
from ion.services.sa.resource_impl.resource_impl_metatest import ResourceImplMetatest


# Agent parameters.
EDA_RESOURCE_ID = '123xyz'
EDA_NAME = 'ExampleEDA'
EDA_MOD = 'ion.agents.data.external_dataset_agent'
EDA_CLS = 'ExternalDatasetAgent'


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
#@unittest.skip('not working yet...')
class TestExternalDatasetAgentMgmt(IonIntegrationTestCase):

    # DataHandler config
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.base_data_handler',
        'dvr_cls' : 'DummyDataHandler',
    }

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        log.debug("TestExternalDatasetAgentMgmt: started services")

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)

    def test_activateDatasetAgent(self):

        # Create ExternalDatasetModel
        datsetModel_obj = IonObject(RT.ExternalDatasetModel, name='ExampleDatasetModel', description="ExampleDatasetModel", datset_type="FibSeries" )
        try:
            datasetModel_id = self.damsclient.create_external_dataset_model(datsetModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new ExternalDatasetModel: %s" %ex)
        log.debug("TestExternalDatasetAgentMgmt: new ExternalDatasetModel id = %s", str(datasetModel_id) )

        # Create ExternalDatasetAgent
        datasetAgent_obj = IonObject(RT.ExternalDatasetAgent, name='datasetagent007', description="datasetagent007", handler_module=EDA_MOD, handler_class=EDA_CLS )
        try:
            datasetAgent_id = self.damsclient.create_external_dataset_agent(datasetAgent_obj, datasetModel_id)
        except BadRequest as ex:
            self.fail("failed to create new ExternalDatasetAgent: %s" %ex)
        log.debug("TestExternalDatasetAgentMgmt: new ExternalDatasetAgent id = %s", str(datasetAgent_id) )


        # Create ExternalDataset
        log.debug('TestExternalDatasetAgentMgmt: Create external dataset resource ')
        extDataset_obj = IonObject(RT.ExternalDataset, name='ExtDataset', description="ExtDataset" )
        try:
            extDataset_id = self.damsclient.create_external_dataset(extDataset_obj, datasetModel_id)
        except BadRequest as ex:
            self.fail("failed to create new external dataset resource: %s" %ex)

        log.debug("TestExternalDatasetAgentMgmt: new ExternalDataset id = %s  ", str(extDataset_id))

        #register the dataset as a data producer
        dproducer_id = self.damsclient.register_external_data_set(extDataset_id)

        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def)

        log.debug("TestExternalDatasetAgentMgmt: new Stream Definition id = %s", str(ctd_stream_def_id))

        log.debug("TestExternalDatasetAgentMgmt: Creating new data product with a stream definition")
        dp_obj = IonObject(RT.DataProduct,name='eoi dataset data',description=' stream test')
        try:
            data_product_id1 = self.dpclient.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        log.debug("TestExternalDatasetAgentMgmt: new dp_id = %s", str(data_product_id1) )

        self.damsclient.assign_data_product(input_resource_id=extDataset_id, data_product_id=data_product_id1)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug("TestExternalDatasetAgentMgmt: Data product streams1 = %s", str(stream_ids) )
        stream_id = stream_ids[0]

        # Build a taxonomy for the dataset
        tx = TaxyTool()
        tx.add_taxonomy_set('data', 'external_data')

        # Augment the DVR_CONFIG with the necessary pieces
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,#TODO: This should probably be a 'stream_config' dict with stream_name:stream_id members
            'data_producer_id':dproducer_id,
#            'external_dataset_res':extDataset_obj, # Not needed - retrieved by EDA based on resource_id
            'taxonomy':tx.dump(), #TODO: Currently does not support sets
            'max_records':4,
            }

        # Create agent config.
        self._stream_config = {}
        agent_config = {
            'driver_config' : self.DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': EDA_RESOURCE_ID},
            'test_mode' : True
        }

        extDatasetAgentInstance_obj = IonObject(RT.ExternalDatasetAgentInstance, name='DatasetAgentInstance', description="DatasetAgentInstance", dataset_driver_config = self.DVR_CONFIG, dataset_agent_config = agent_config)
        extDatasetAgentInstance_id = self.damsclient.create_external_dataset_agent_instance(external_dataset_agent_instance=extDatasetAgentInstance_obj, external_dataset_agent_id=datasetAgent_id, external_dataset_id=extDataset_id)
        log.debug("TestExternalDatasetAgentMgmt: Dataset agent instance obj: = %s", str(extDatasetAgentInstance_obj) )
        log.debug("TestExternalDatasetAgentMgmt: Dataset agent instance id: = %s", str(extDatasetAgentInstance_id) )

        #Check that the instance is currently not active
        id, active = self.damsclient.retrieve_external_dataset_agent_instance(extDataset_id)
        log.debug("TestExternalDatasetAgentMgmt: Dataset agent instance id: = %s    active 1 = %s ", str(id), str(active) )

        self.damsclient.start_external_dataset_agent_instance(extDatasetAgentInstance_id)


        dataset_agent_instance_obj= self.damsclient.read_external_dataset_agent_instance(extDatasetAgentInstance_id)
        log.debug("TestExternalDatasetAgentMgmt: Dataset agent instance obj: = %s", str(dataset_agent_instance_obj) )

        # now the instance process should be active
        id, active = self.damsclient.retrieve_external_dataset_agent_instance(extDataset_id)
        log.debug("TestExternalDatasetAgentMgmt: Dataset agent instance id: = %s    active 2 = %s ", str(id), str(active) )

        # Start a resource agent client to talk with the instrument agent.
        self._dsa_client = ResourceAgentClient(extDataset_id,  process=FakeProcess())
        print 'TestExternalDatasetAgentMgmt: got ia client %s', self._dsa_client
        log.debug("TestExternalDatasetAgentMgmt: got dataset client %s", str(self._dsa_client))

#        cmd=AgentCommand(command='initialize')
#        _ = self._dsa_client.execute_agent(cmd)
#
#        cmd = AgentCommand(command='go_active')
#        _ = self._dsa_client.execute_agent(cmd)
#
#        cmd = AgentCommand(command='run')
#        _ = self._dsa_client.execute_agent(cmd)
#
#        log.info('Send an unconstrained request for data (\'new data\')')
#        cmd = AgentCommand(command='acquire_data')
#        self._dsa_client.execute(cmd)
#
#        log.info('Send a second unconstrained request for data (\'new data\'), should be rejected')
#        cmd = AgentCommand(command='acquire_data')
#        self._dsa_client.execute(cmd)
#
#        cmd = AgentCommand(command='reset')
#        _ = self._dsa_client.execute_agent(cmd)
#        cmd = AgentCommand(command='get_current_state')
#        retval = self._dsa_client.execute_agent(cmd)
#        state = retval.result

        # TODO: Think about what we really should be testing at this point
        # The following is taken from ion.agents.data.test.test_external_dataset_agent.ExternalDatasetAgentTestBase.test_states()
        # TODO: Do we also need to show data retrieval?
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='resume')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='clear')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='clear')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='reset')
        retval = self._dsa_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._dsa_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)




        #-------------------------------
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.damsclient.stop_external_dataset_agent_instance(extDatasetAgentInstance_id)


  
