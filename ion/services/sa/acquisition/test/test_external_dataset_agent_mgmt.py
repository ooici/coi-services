from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import IDataProductManagementService, DataProductManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.objects import HdfStorage, CouchStorage

from pyon.public import log
from nose.plugins.attrib import attr

from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
#from ion.services.mi.drivers.sbe37_driver import SBE37Channel
#from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
#from ion.services.mi.drivers.sbe37_driver import PACKET_CONFIG
from pyon.public import CFG



from pyon.public import CFG
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, LCS, PRED
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
import time

from ion.services.sa.resource_impl.data_product_impl import DataProductImpl
from ion.services.sa.resource_impl.resource_impl_metatest import ResourceImplMetatest

# DataHandler config
DVR_CONFIG = {
    'dvr_mod' : 'ion.agents.eoi.handler.base_data_handler',
#    'dvr_cls' : 'BaseDataHandler',
    'dvr_cls' : 'FibonacciDataHandler',
#    'dvr_cls' : 'DummyDataHandler',
#    'dvr_mod' : 'ion.agents.eoi.handler.netcdf_data_handler',
#    'dvr_cls' : 'NetcdfDataHandler'
}

# Agent parameters.
EDA_RESOURCE_ID = '123xyz'
EDA_NAME = 'ExampleEDA'
EDA_MOD = 'ion.agents.eoi.external_dataset_agent'
EDA_CLS = 'ExternalDatasetAgent'


class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('INT', group='sa')
@unittest.skip('not working yet...')
class TestExternalDatasetAgentMgmt(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)

    def test_activateDatasetAgent(self):

        # Create ExternalDatasetModel
        datsetModel_obj = IonObject(RT.ExternalDatasetModel, name='ExampleDatasetModel', description="ExampleDatasetModel", datset_type="FibSeries" )
        try:
            datasetModel_id = self.damsclient.create_external_dataset_model(datsetModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new ExternalDatasetModel: %s" %ex)
        print 'new ExternalDatasetModel id = ', datasetModel_id

        # Create ExternalDatasetAgent
        datasetAgent_obj = IonObject(RT.ExternalDatasetAgent, name='datasetagent007', description="datasetagent007", handler_module="ion.agents.eoi", handler_class="ExternalDatasetAgent" )
        try:
            datasetAgent_id = self.damsclient.create_external_dataset_agent(datasetAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new ExternalDatasetAgent: %s" %ex)
        print 'new ExternalDatasetAgent id = ', datasetAgent_id

        self.damsclient.assign_dataset_agent_to_external_dataset_model(datasetAgent_id, datasetModel_id)

        # Create ExternalDataset
        log.debug('test_activateDatasetAgent: Create external dataset resource ')
        extDataset_obj = IonObject(RT.ExternalDataset, name='ExtDataset', description="ExtDataset", serial_number="12345" )
        try:
            extDataset_id = self.damsclient.create_external_dataset(extDataset_obj)
        except BadRequest as ex:
            self.fail("failed to create new external dataset resource: %s" %ex)

        log.debug("test_activateDatasetAgent: new ExternalDataset id = %s  ", extDataset_id)

        #register the dataset as a data producer
        self.damsclient.register_external_data_set(extDataset_id)


        # Create agent config.
        self._stream_config = {}
        agent_config = {
            'driver_config' : DVR_CONFIG,
            'stream_config' : self._stream_config,
            'agent'         : {'resource_id': EDA_RESOURCE_ID},
            'test_mode' : True
        }

        extDatasetInstance_obj = IonObject(RT.ExternalDatasetAgentInstance, name='DatasetAgentInstance', description="DatasetAgentInstance", dataset_driver_config = DVR_CONFIG, dataset_agent_config = agent_config)
        extDatasetInstance_id = self.damsclient.create_external_dataset_agent_instance(extDatasetInstance_obj)

        self.damsclient.assign_external_dataset_to_agent_instance(extDataset_id, extDatasetInstance_id)
        self.damsclient.assign_external_data_agent_to_agent_instance(datasetAgent_id, extDatasetInstance_id)


        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def)

        print 'new Stream Definition id = ', ctd_stream_def_id

        print 'Creating new data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='eoi dataset data',description=' stream test')
        try:
            data_product_id1 = self.dpclient.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', data_product_id1

        self.damsclient.assign_data_product(input_resource_id=extDataset_id, data_product_id=data_product_id1)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        print 'Data product streams1 = ', stream_ids


        self.damsclient.start_external_dataset_agent_instance(extDatasetInstance_id)


        dataset_agent_instance_obj= self.damsclient.read_external_dataset_agent_instance()
        print 'Dataset agent instance obj: = ', dataset_agent_instance_obj

        # Start a resource agent client to talk with the instrument agent.
        self._dsa_client = ResourceAgentClient(instDevice_id,  process=FakeProcess())
        print 'activate_instrument: got ia client %s', self._dsa_client
        log.debug("test_activateInstrument: got dataset client %s", str(self._dsa_client))




        #-------------------------------
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.damsclient.stop_external_dataset_agent_instance(extDatasetInstance_id)




  