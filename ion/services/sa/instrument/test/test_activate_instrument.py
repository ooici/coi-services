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
from ion.services.mi.drivers.sbe37_driver import SBE37Channel
from ion.services.mi.drivers.sbe37_driver import SBE37Parameter
from ion.services.mi.drivers.sbe37_driver import PACKET_CONFIG
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



class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='sa')
#@unittest.skip('run locally only')
class TestActivateInstrumentIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        self.container.start_rel_from_url('res/deploy/r2sa.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)

    def test_activateInstrument(self):

        # Set up the preconditions
        # ingestion configuration parameters
        self.exchange_point_id = 'science_data'
        self.number_of_workers = 2
        self.hdf_storage = HdfStorage(relative_path='ingest')
        self.couch_storage = CouchStorage(datastore_name='test_datastore')
        self.XP = 'science_data'
        self.exchange_name = 'ingestion_queue'

        # Create ingestion configuration and activate it
        ingestion_configuration_id =  self.ingestclient.create_ingestion_configuration(
            exchange_point_id=self.exchange_point_id,
            couch_storage=self.couch_storage,
            hdf_storage=self.hdf_storage,
            number_of_workers=self.number_of_workers
        )
        print 'test_activateInstrument: ingestion_configuration_id', ingestion_configuration_id

        # activate an ingestion configuration
        ret = self.ingestclient.activate_ingestion_configuration(ingestion_configuration_id)
        log.debug("test_activateInstrument: activate = %s"  % str(ret))

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model_label="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        print 'new InstrumentModel id = ', instModel_id

        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.services.mi.instrument_agent", driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        print 'new InstrumentAgent id = ', instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)
            
        print 'new InstrumentDevice id = ', instDevice_id

        # Create InstrumentAgentInstance to hold configuration information
        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance", svr_addr="localhost",
                                          driver_module="ion.services.mi.drivers.sbe37_driver", driver_class="SBE37Driver",
                                          cmd_port=5556, evt_port=5557, comms_method="ethernet", comms_device_address=CFG.device.sbe37.host, comms_device_port=CFG.device.sbe37.port,
                                          comms_server_address="localhost", comms_server_port=8888)
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)


        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def)

        print 'new Stream Definition id = ', instDevice_id

        print 'Creating new CDM data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='ctd_parsed',description='ctd stream test')
        try:
            data_product_id1 = self.dpclient.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', data_product_id1

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        print 'Data product streams1 = ', stream_ids


        print 'Creating new RAW data product with a stream definition'
        raw_stream_def = SBE37_RAW_stream_definition()
        raw_stream_def_id = self.pubsubcli.create_stream_definition(container=raw_stream_def)

        dp_obj = IonObject(RT.DataProduct,name='ctd_raw',description='raw stream test')
        try:
            data_product_id2 = self.dpclient.create_data_product(dp_obj, raw_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', data_product_id2

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasStream, None, True)
        print 'Data product streams2 = ', stream_ids


        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)


        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        print 'Instrument agent instance obj: = ', inst_agent_instance_obj

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = ResourceAgentClient('123xyz', name=inst_agent_instance_obj.agent_process_id,  process=FakeProcess())
        print 'activate_instrument: got ia client %s', self._ia_client
        log.debug("test_activateInstrument: got ia client %s", str(self._ia_client))




        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        print retval
        log.debug("test_activateInstrument: initialize %s", str(retval))

        time.sleep(2)

        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: go_active %s", str(reply))
        time.sleep(2)

        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: run %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling acquire_sample ")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrument: return from acquire_sample %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling acquire_sample 2")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrument: return from acquire_sample 2   %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling acquire_sample 3")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrument: return from acquire_sample 3   %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling go_inactive ")
        cmd = AgentCommand(command='go_inactive')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: return from go_inactive %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrument: calling reset ")
        cmd = AgentCommand(command='reset')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: return from reset %s", str(reply))
        time.sleep(2)


        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)

        #get the dataset id of the ctd_parsed product from the dataproduct  data_product_id1
        ctd_parsed_data_product_obj = self.dpclient.read_data_product(data_product_id1)
        log.debug("test_activateInstrument: ctd_parsed_data_product dataset id %s", str(ctd_parsed_data_product_obj.dataset_id))

        # ask for the dataset bounds from the datasetmgmtsvc
        bounds = self.datasetclient.get_dataset_bounds(ctd_parsed_data_product_obj.dataset_id)
        log.debug("test_activateInstrument: ctd_parsed_data_product dataset bounds %s", str(bounds))
        print 'activate_instrument: got dataset bounds %s', str(bounds)

