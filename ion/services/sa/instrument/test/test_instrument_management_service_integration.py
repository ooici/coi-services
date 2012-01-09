from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.sa.instrument_management.instrument_management_service import InstrumentManagementService
from interface.services.sa.iinstrument_management_service import IInstrumentManagementService, InstrumentManagementServiceClient

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, AT, LCS
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
#@unittest.skip('coi/dm/sa services not working yet for integration tests to pass')
class TestInstrumentManagementServiceIntegration(IonIntegrationTestCase):

    def my_test_init(self):
        # Start container
        #print 'instantiating container'
        self._start_container()
        #container = Container()
        #print 'starting container'
        #container.start()
        #print 'started container'

        # Establish endpoint with container
        container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)
        #container_client = ProcessRPCClient(node=container.node, name=container.name, iface=IContainerAgent, process=FakeProcess())
        #print 'got CC client'
        container_client.start_rel_from_url('res/deploy/r2ims.yml')
        
        print 'started services'

    def notest_createInstrument(self):
        self.my_test_init()

        # Now create client to DataProductManagementService
        #client = ProcessRPCClient(node=self.container.node, name="instrument_management", iface=IDataProductManagementService)
        client = DataProductManagementServiceClient(node=self.container.node)

        # test creating a new data product w/o a data producer
        print 'Creating new data product w/o a data producer'
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP1', 
                           description='some new dp')
        try:
            dp_id = client.create_data_product(dp_obj)
        except BadRequest as ex:
            self.fail("failed to create new data product")
        print 'new dp_id = ', dp_id
        
        # test creating a duplicate data product
        print 'Creating the same data product a second time (duplicate)'
        dp_obj.description = 'the first dp'
        try:
            dp_id = client.create_data_product(dp_obj)
        except BadRequest as ex:
            print ex
        else:
            self.fail("duplicate data product was created with the same name")
        
        """
        # This is broken until the interceptor handles lists properly (w/o converting them to constants)
        # and DAMS works with pubsub_management.register_producer() correctly
        # test creating a new data product with a data producer
        print 'Creating new data product with a data producer'
        dp_obj = IonObject(RT.DataProduct, 
                           name='DP2', 
                           description='another new dp')
        data_producer_obj = IonObject(RT.DataProducer, 
                                      name='DataProducer1', 
                                      description='a new data producer')
        try:
            dp_id = client.create_data_product(dp_obj, data_producer_obj)
        except BadRequest as ex:
            self.fail("failed to create new data product")
        print 'new dp_id = ', dp_id
        """
        
        # test reading a non-existent data product
        print 'reading non-existent data product' 
        try:
            dp_obj = client.read_data_product('some_fake_id')
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data product was found during read: %s" %dp_obj)

        # update a data product (tests read also)
        print 'Updating data product' 
        # first get the existing dp object      
        try:
            dp_obj = client.read_data_product(dp_id)
        except NotFound as ex:
            self.fail("existing data product was not found during read")
        else:
            pass
            #print 'dp_obj = ', dp_obj
        # now tweak the object
        dp_obj.description = 'the very first dp'
        # now write the dp back to the registry
        try:
            update_result = client.update_data_product(dp_obj)
        except NotFound as ex:
            self.fail("existing data product was not found during update")
        except Conflict as ex:
            self.fail("revision conflict exception during data product update")
        else:
            self.assertTrue(update_result == True)           
        # now get the dp back to see if it was updated
        try:
            dp_obj = client.read_data_product(dp_id)
        except NotFound as ex:
            self.fail("existing data product was not found during read")
        else:
            pass
            #print 'dp_obj = ', dp_obj
        self.assertTrue(dp_obj.description == 'the very first dp')
        
        # now 'delete' the data product 
        print "deleting data product"
        try:
            delete_result = client.delete_data_product(dp_id)
        except NotFound as ex:
            self.fail("existing data product was not found during delete")
        self.assertTrue(delete_result == True)
        # now try to get the deleted dp object      
        try:
            dp_obj = client.read_data_product(dp_id)
        except NotFound as ex:
            pass
        else:
            self.fail("deleted data product was found during read")

        # now try to delete the already deleted dp object
        print "deleting non-existing data product"      
        try:
            delete_result = client.delete_data_product(dp_id)
        except NotFound as ex:
            pass
        else:
            self.fail("non-existing data product was found during delete")

        # Shut down container
        #container.stop()
