#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import  log, IonObject
from pyon.util.int_test import IonIntegrationTestCase
from ion.services.sa.product.data_product_management_service import DataProductManagementService
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from prototype.sci_data.stream_defs import ctd_stream_definition, SBE37_CDM_stream_definition
from interface.objects import HdfStorage, CouchStorage, DataProduct, LastUpdate

from pyon.util.context import LocalContextMixin
from pyon.util.containers import DotDict
from pyon.core.exception import BadRequest, NotFound, Conflict
from pyon.public import RT, PRED
from mock import Mock
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from interface.objects import ProcessDefinition
import unittest
import time


class FakeProcess(LocalContextMixin):
    name = ''



@attr('INT', group='sa')
#@unittest.skip('not working')
class TestDataProductManagementServiceIntegration(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        #print 'instantiating container'
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        print 'started services'

        # Now create client to DataProductManagementService
        self.client = DataProductManagementServiceClient(node=self.container.node)
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.process_dispatcher   = ProcessDispatcherServiceClient()

    @unittest.skip('OBE')
    def test_get_last_update(self):
        from ion.processes.data.last_update_cache import CACHE_DATASTORE_NAME

        #------------------------------------------
        # Create the environment
        #------------------------------------------

        definition = SBE37_CDM_stream_definition()
        datastore_name = CACHE_DATASTORE_NAME
        db = self.container.datastore_manager.get_datastore(datastore_name)
        stream_def_id = self.pubsubcli.create_stream_definition(container=definition)

        dp = DataProduct(name='dp1')

        data_product_id = self.client.create_data_product(data_product=dp, stream_definition_id=stream_def_id)
        stream_ids, garbage = self.rrclient.find_objects(data_product_id, PRED.hasStream, id_only=True)
        stream_id = stream_ids[0]

        fake_lu = LastUpdate()
        fake_lu_doc = db._ion_object_to_persistence_dict(fake_lu)
        db.create_doc(fake_lu_doc, object_id=stream_id)

        #------------------------------------------
        # Now execute
        #------------------------------------------
        res = self.client.get_last_update(data_product_id=data_product_id)
        self.assertTrue(isinstance(res[stream_id], LastUpdate), 'retrieving documents failed')



    def test_createDataProduct(self):
        client = self.client



        self.process_definitions  = {}
        ingestion_worker_definition = ProcessDefinition(name='ingestion worker')
        ingestion_worker_definition.executable = {
            'module':'ion.processes.data.ingestion.science_granule_ingestion_worker',
            'class' :'ScienceGranuleIngestionWorker'
        }
        process_definition_id = self.process_dispatcher.create_process_definition(process_definition=ingestion_worker_definition)
        self.process_definitions['ingestion_worker'] = process_definition_id
        

        # First launch the ingestors
        self.exchange_space       = 'science_granule_ingestion'
        self.exchange_point       = 'science_data'
        config = DotDict()
        config.process.datastore_name = 'datasets'
        config.process.queue_name = self.exchange_space

        self.process_dispatcher.schedule_process(self.process_definitions['ingestion_worker'],configuration=config)

        # create a stream definition for the data from the ctd simulator
        ctd_stream_def = ctd_stream_definition()
        ctd_stream_def_id = self.pubsubcli.create_stream_definition(container=ctd_stream_def, name='Simulated CTD data')
        print ("Created stream def id %s" % ctd_stream_def_id)


        # test creating a new data product w/o a stream definition
        print 'test_createDataProduct: Creating new data product w/o a stream definition (L4-CI-SA-RQ-308)'
        dp_obj = IonObject(RT.DataProduct,
                           name='DP1',
                           description='some new dp')
        try:
            dp_id = client.create_data_product(dp_obj, '')
            dp_obj = client.read_data_product(dp_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', dp_id
        log.debug("test_createDataProduct: Data product info from registry %s (L4-CI-SA-RQ-308)", str(dp_obj))


        # test creating a new data product with  a stream definition
        print 'Creating new data product with a stream definition'
        dp_obj = IonObject(RT.DataProduct,
                           name='DP2',
                           description='some new dp')
        try:
            dp_id2 = client.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            self.fail("failed to create new data product: %s" %ex)
        print 'new dp_id = ', dp_id2

        #make sure data product is associated with stream def
        streamdefs = []
        streams, _ = self.rrclient.find_objects(dp_id2, PRED.hasStream, RT.Stream, True)
        for s in streams:
            print ("Checking stream %s" % s)
            sdefs, _ = self.rrclient.find_objects(s, PRED.hasStreamDefinition, RT.StreamDefinition, True)
            for sd in sdefs:
                print ("Checking streamdef %s" % sd)
                streamdefs.append(sd)
        self.assertIn(ctd_stream_def_id, streamdefs)


        # test activate and suspend data product persistence
        try:
            client.activate_data_product_persistence(dp_id2, persist_data=True, persist_metadata=True)
        except BadRequest as ex:
            self.fail("failed to activate  data product persistence : %s" %ex)


        # test suspend data product persistence
        try:
            client.suspend_data_product_persistence(dp_id2)
        except BadRequest as ex:
            self.fail("failed to suspend deactivate data product persistence : %s" %ex)

        pid = self.container.spawn_process(name='dummy_process_for_test',
                                           module='pyon.ion.process',
                                           cls='SimpleProcess',
                                           config={})
        dummy_process = self.container.proc_manager.procs[pid]


        # test creating a duplicate data product
        print 'Creating the same data product a second time (duplicate)'
        dp_obj.description = 'the first dp'
        try:
            dp_id = client.create_data_product(dp_obj, ctd_stream_def_id)
        except BadRequest as ex:
            print ex
        else:
            self.fail("duplicate data product was created with the same name")


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
        print "deleting data product: ", dp_id
        try:
            client.delete_data_product(dp_id)
        except NotFound as ex:
            self.fail("existing data product was not found during delete")

        # now try to get the deleted dp object

        #todo: the RR should perhaps not return retired data products
#        try:
#            dp_obj = client.read_data_product(dp_id)
#        except NotFound as ex:
#            pass
#        else:
#            self.fail("deleted data product was found during read")

        # now try to delete the already deleted dp object
#        print "deleting non-existing data product"
#        try:
#            client.delete_data_product(dp_id)
#        except NotFound as ex:
#            pass
#        else:
#            self.fail("non-existing data product was found during delete")

            # Shut down container
            #container.stop()
