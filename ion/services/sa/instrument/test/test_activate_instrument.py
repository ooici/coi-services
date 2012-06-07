from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_product_management_service import IDataProductManagementService, DataProductManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.objects import StreamQuery

from pyon.public import log
from nose.plugins.attrib import attr

from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG
from gevent.event import AsyncResult

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

from ion.services.sa.instrument.data_product_impl import DataProductImpl
from ion.services.sa.resource_impl.resource_impl_metatest import ResourceImplMetatest



class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


@attr('HARDWARE', group='sa')
@unittest.skip('run locally only')
class TestActivateInstrumentIntegration(IonIntegrationTestCase):

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
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.datasetclient =  DatasetManagementServiceClient(node=self.container.node)

        #setup listerner vars
        self._data_greenlets = []
        self._no_samples = None
        self._samples_received = []

    @unittest.skip("TBD")
    def test_activateInstrumentSample(self):

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model_label="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        print 'new InstrumentModel id = ', instModel_id

        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.agents.instrument.instrument_agent", driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        print 'new InstrumentAgent id = ', instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_activateInstrumentSample: Create instrument resource to represent the SBE37 (SA Req: L4-CI-SA-RQ-241) ')
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)
            
        log.debug("test_activateInstrumentSample: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ", instDevice_id)

        driver_config = {
            'dvr_mod' : 'ion.agents.instrument.drivers.sbe37.sbe37_driver',
            'dvr_cls' : 'SBE37Driver',
            'workdir' : '/tmp/',
        }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance", driver_config = driver_config,
                                          comms_device_address='sbe37-simulator.oceanobservatories.org',   comms_device_port=4001,  port_agent_work_dir='/tmp/', port_agent_delimeter=['<<','>>'] )
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)


        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def)

        print 'new Stream Definition id = ', instDevice_id

        print 'Creating new CDM data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='the parsed data',description='ctd stream test')
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

        dp_obj = IonObject(RT.DataProduct,name='the raw data',description='raw stream test')
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
        #self._ia_client = ResourceAgentClient('123xyz', name=inst_agent_instance_obj.agent_process_id,  process=FakeProcess())
        self._ia_client = ResourceAgentClient(instDevice_id,  process=FakeProcess())
        print 'activate_instrument: got ia client %s', self._ia_client
        log.debug("test_activateInstrumentSample: got ia client %s", str(self._ia_client))




        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        print retval
        log.debug("test_activateInstrumentSample: initialize %s", str(retval))

        time.sleep(2)

        log.debug("test_activateInstrumentSample: Sending go_active command (L4-CI-SA-RQ-334)")
        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: return value from go_active %s", str(reply))
        time.sleep(2)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("test_activateInstrumentSample: current state after sending go_active command %s    (L4-CI-SA-RQ-334)", str(state))

        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: run %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrumentSample: calling acquire_sample ")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrumentSample: return from acquire_sample %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrumentSample: calling acquire_sample 2")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrumentSample: return from acquire_sample 2   %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrumentSample: calling acquire_sample 3")
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrumentSample: return from acquire_sample 3   %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrumentSample: calling reset ")
        cmd = AgentCommand(command='reset')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentSample: return from reset %s", str(reply))
        time.sleep(2)

        #-------------------------------
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)



    def test_activateInstrumentStream(self):

        # Create InstrumentModel
        instModel_obj = IonObject(RT.InstrumentModel, name='SBE37IMModel', description="SBE37IMModel", model_label="SBE37IMModel" )
        try:
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentModel: %s" %ex)
        print 'new InstrumentModel id = ', instModel_id

        # Create InstrumentAgent
        instAgent_obj = IonObject(RT.InstrumentAgent, name='agent007', description="SBE37IMAgent", driver_module="ion.agents.instrument.instrument_agent", driver_class="InstrumentAgent" )
        try:
            instAgent_id = self.imsclient.create_instrument_agent(instAgent_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentAgent: %s" %ex)
        print 'new InstrumentAgent id = ', instAgent_id

        self.imsclient.assign_instrument_model_to_instrument_agent(instModel_id, instAgent_id)

        # Create InstrumentDevice
        log.debug('test_activateInstrumentStream: Create instrument resource to represent the SBE37 (SA Req: L4-CI-SA-RQ-241) ')
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)

        log.debug("test_activateInstrumentStream: new InstrumentDevice id = %s    (SA Req: L4-CI-SA-RQ-241) ", instDevice_id)

        driver_config = {
            'dvr_mod' : 'ion.agents.instrument.drivers.sbe37.sbe37_driver',
            'dvr_cls' : 'SBE37Driver',
            'workdir' : '/tmp/',
        }

        instAgentInstance_obj = IonObject(RT.InstrumentAgentInstance, name='SBE37IMAgentInstance', description="SBE37IMAgentInstance", driver_config = driver_config,
                                          comms_device_address='sbe37-simulator.oceanobservatories.org',   comms_device_port=4001,  port_agent_work_dir='/tmp/', port_agent_delimeter=['<<','>>'] )
        instAgentInstance_id = self.imsclient.create_instrument_agent_instance(instAgentInstance_obj, instAgent_id, instDevice_id)


        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def)

        log.debug( 'test_activateInstrumentStream new Stream Definition id = %s', instDevice_id )

        log.debug( 'test_activateInstrumentStream Creating new CDM data product with a stream definition' )
        dp_obj = IonObject(RT.DataProduct,name='the parsed data',description='ctd stream test')
        try:
            data_product_id1 = self.dpclient.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        log.debug( 'test_activateInstrumentStream new dp_id = %s', str(data_product_id1) )

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id1)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id1, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'test_activateInstrumentStream Data product streams1 = %s', str(stream_ids) )



        simdata_subscription_id = self.pubsubcli.create_subscription(
            query=StreamQuery([stream_ids[0]]),
            exchange_name='Sim_data_queue',
            name='SimDataSubscription',
            description='SimData SubscriptionDescription'
        )


        def simdata_message_received(message, headers):
            input = str(message)
            log.debug("test_activateInstrumentStream: granule received: %s", input)


        subscriber_registrar = StreamSubscriberRegistrar(process=self.container, node=self.container.node)
        simdata_subscriber = subscriber_registrar.create_subscriber(exchange_name='Sim_data_queue', callback=simdata_message_received)

        # Start subscribers
        simdata_subscriber.start()

        # Activate subscriptions
        self.pubsubcli.activate_subscription(simdata_subscription_id)


        log.debug( 'test_activateInstrumentStream Creating new RAW data product with a stream definition' )
        raw_stream_def = SBE37_RAW_stream_definition()
        raw_stream_def_id = self.pubsubcli.create_stream_definition(container=raw_stream_def)

        dp_obj = IonObject(RT.DataProduct,name='the raw data',description='raw stream test')
        try:
            data_product_id2 = self.dpclient.create_data_product(dp_obj, raw_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        log.debug( 'test_activateInstrumentStream new dp_id = %s', str(data_product_id2) )

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id2)

        self.dpclient.activate_data_product_persistence(data_product_id=data_product_id2, persist_data=True, persist_metadata=True)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id2, PRED.hasStream, None, True)
        log.debug( 'test_activateInstrumentStream Data product streams2 = %s', str(stream_ids) )


        self.imsclient.start_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)


        inst_agent_instance_obj= self.imsclient.read_instrument_agent_instance(instAgentInstance_id)
        log.debug( 'test_activateInstrumentStream Instrument agent instance obj: = %s', str(inst_agent_instance_obj) )

        # Start a resource agent client to talk with the instrument agent.
        #self._ia_client = ResourceAgentClient('123xyz', name=inst_agent_instance_obj.agent_process_id,  process=FakeProcess())
        self._ia_client = ResourceAgentClient(instDevice_id,  process=FakeProcess())
        log.debug( 'test_activateInstrumentStream: got ia client %s', str(self._ia_client ))
        log.debug("test_activateInstrumentStream: got ia client %s", str(self._ia_client))




        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentStream: initialize %s", str(retval))

        time.sleep(2)

        log.debug("test_activateInstrumentStream: Sending go_active command (L4-CI-SA-RQ-334)")
        cmd = AgentCommand(command='go_active')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentStream: return value from go_active %s", str(reply))
        time.sleep(2)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        log.debug("test_activateInstrumentStream: current state after sending go_active command %s    (L4-CI-SA-RQ-334)", str(state))

        cmd = AgentCommand(command='run')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrument: run %s", str(reply))
        time.sleep(2)

        log.debug("test_activateInstrumentStream: calling go_streaming ")
        cmd = AgentCommand(command='go_streaming')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrumentStream: return from go_streaming %s", str(reply))


        time.sleep(15)

        log.debug("test_activateInstrumentStream: calling go_observatory")
        cmd = AgentCommand(command='go_observatory')
        reply = self._ia_client.execute(cmd)
        log.debug("test_activateInstrumentStream: return from go_observatory   %s", str(reply))
        time.sleep(2)


        log.debug("test_activateInstrumentStream: calling reset ")
        cmd = AgentCommand(command='reset')
        reply = self._ia_client.execute_agent(cmd)
        log.debug("test_activateInstrumentStream: return from reset %s", str(reply))
        time.sleep(2)

        #-------------------------------
        # Deactivate InstrumentAgentInstance
        #-------------------------------
        self.imsclient.stop_instrument_agent_instance(instrument_agent_instance_id=instAgentInstance_id)


