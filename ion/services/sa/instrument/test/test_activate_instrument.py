from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import IDataProductManagementService, DataProductManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from prototype.sci_data.ctd_stream import ctd_stream_definition
from interface.objects import HdfStorage, CouchStorage

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
    name = ''



@attr('INT', group='sa')
#@unittest.skip('not working')
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
        self.client = DataProductManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)

    def test_createDataProduct(self):
        client = self.client

        # Set up the preconditions

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

        self.rrclient.create_association(instAgent_id,  PRED.hasModel, instModel_id)

        # Create InstrumentDevice
        instDevice_obj = IonObject(RT.InstrumentDevice, name='SBE37IMDevice', description="SBE37IMDevice", serial_number="12345" )
        try:
            instDevice_id = self.imsclient.create_instrument_device(instDevice_obj)
        except BadRequest as ex:
            self.fail("failed to create new InstrumentDevice: %s" %ex)
        print 'new InstrumentDevice id = ', instDevice_id
        self.rrclient.create_association(instDevice_id,  PRED.hasModel, instModel_id)
        #register this instrument as a Producer
        self.damsclient.register_instrument(instDevice_id)


        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = ctd_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')


        print 'Creating new data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,name='ctd parsed stream',description='ctd stream test')
        try:
            data_product_id = client.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', data_product_id

        self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id)

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id, PRED.hasStream, None, True)
        print 'Data product streams = ', stream_ids



        inst_agent_instance_id = self.imsclient.activate_instrument(instrument_device_id=instDevice_id)
        print 'Instrument agent instance id: = ', inst_agent_instance_id







  