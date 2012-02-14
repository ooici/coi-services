#!/usr/bin/env python

'''
@file ion/services/dm/ingestion/test/test_ingestion.py
@author Swarbhanu Chatterjee
@test ion.services.dm.ingestion.ingestion_management_service test suite to cover all ingestion mgmt service code
'''

import gevent
from mock import Mock, sentinel, patch
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.core.exception import NotFound, BadRequest
import unittest
from pyon.public import CFG, IonObject, log, RT, PRED, LCS, StreamPublisher, StreamSubscriber
from interface.objects import ProcessDefinition, StreamQuery, ExchangeQuery, HdfStorage, CouchStorage, StreamPolicy
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.icontainer_agent import ContainerAgentClient

from interface.objects import BlogPost, BlogComment
from pyon.datastore.couchdb.couchdb_datastore import CouchDB_DataStore
from pyon.datastore.datastore import DataStore

@attr('INT', group='dm')
class IngestionManagementServiceIntTest(IonIntegrationTestCase):

    def setUp(self):
        # set up the container for testing

        #------------------------------------------------------------------------
        # Container
        #----------------------------------------------------------------------
        self._start_container()

        self.cc = ContainerAgentClient(node=self.container.node,name=self.container.name)

        self.cc.start_rel_from_url('res/deploy/r2dm.yml')

        #------------------------------------------------------------------------
        # Service clients
        #----------------------------------------------------------------------
        self.pubsub_cli = PubsubManagementServiceClient(node=self.cc.node)
        self.tms_cli = TransformManagementServiceClient(node=self.cc.node)
        self.ingestion_cli = IngestionManagementServiceClient(node=self.cc.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.cc.node)

        #------------------------------------------------------------------------
        # Configuration parameters
        #----------------------------------------------------------------------
        self.exchange_point_id = 'science_data'
        self.datastore_name = 'dm_datastore'
        self.number_of_workers = 2
        self.hdf_storage = HdfStorage()
        self.couch_storage = CouchStorage(datastore_name = self.datastore_name)
        self.XP = 'science_data'
        self.exchange_name = 'ingestion_queue'

        #------------------------------------------------------------------------
        # Refresh datastore before testing
        #----------------------------------------------------------------------

        self.db = self.container.datastore_manager.get_datastore(self.datastore_name, DataStore.DS_PROFILE.EXAMPLES, CFG)

        #------------------------------------------------------------------------
        # Stream publisher
        #----------------------------------------------------------------------

        self.input_stream_id = self.pubsub_cli.create_stream(name='input_stream',original=True)
        stream_route = self.pubsub_cli.register_producer(exchange_name=self.exchange_name, stream_id=self.input_stream_id)
        self.ctd_stream1_publisher = StreamPublisher(node=self.cc.node, name=('science_data',stream_route.routing_key), \
                                                                                        process=self.cc)

        self.default_policy = StreamPolicy(stream_id=self.input_stream_id)

    def tearDown(self):
        """
        Cleanup. Delete Subscription, Stream, Process Definition
        """
        self._stop_container()


    def test_ingestion_workers_writes_to_couch(self):
        """
        Test that the ingestion workers are writing messages to couch
        """

        #------------------------------------------------------------------------
        # Create ingestion configuration and activate it
        #----------------------------------------------------------------------
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id, \
            self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)
        self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)

        #------------------------------------------------------------------------
        # Publish messages
        #----------------------------------------------------------------------

        post = BlogPost( post_id = '1234', title = 'The beautiful life',author = {'name' : 'Jacques', 'email' : 'jacques@cluseaou.com'}, updated = 'too early', content ='summer', stream_id=self.input_stream_id )

        self.ctd_stream1_publisher.publish(post)

        comment = BlogComment(ref_id = '1234',author = {'name': 'Roger', 'email' : 'roger@rabbit.com'}, updated = 'too late',content = 'when summer comes', stream_id=self.input_stream_id)

        self.ctd_stream1_publisher.publish(comment)


        #------------------------------------------------------------------------
        # List the posts and the comments that should have been written to couch
        #----------------------------------------------------------------------

        objs = self.db.list_objects()

        # the list of ion_objects... in our case BlogPost and BlogComment
        ion_objs = []

        for obj in objs:

            # read the document returned by list
            result = self.db.read_doc(objs[0])

            # convert the persistence dict to an ion_object
            ion_obj = self.db._persistence_dict_to_ion_object(result)

            if isinstance(ion_obj, BlogPost):
                log.debug("ION OBJECT: %s\n" % ion_obj)
                log.debug("POST: %s\n" % post)

                # since the retrieved document has an extra attribute, rev_id, which the orginal post did not have
                # it is easier to compare the attributes than the whole objects
                self.assertTrue(ion_obj.post_id == post.post_id), "The post is not to be found in couch storage"
                self.assertTrue(ion_obj.author == post.author), "The post is not to be found in couch storage"
                self.assertTrue(ion_obj.title == post.title), "The post is not to be found in couch storage"
                self.assertTrue(ion_obj.updated == post.updated), "The post is not to be found in couch storage"
                self.assertTrue(ion_obj.content == post.content), "The post is not to be found in couch storage"

            elif isinstance(ion_obj, BlogComment):
                log.debug("ION OBJECT: %s\n" % ion_obj)
                log.debug("COMMENT: %s\n" % comment)

                # since the retrieved document has an extra attribute, rev_id, which the orginal post did not have
                # it is easier to compare the attributes than the whole objects
                self.assertTrue(ion_obj.author == comment.author), "The comment is not to be found in couch storage"
                self.assertTrue(ion_obj.content == comment.content), "The comment is not to be found in couch storage"
                self.assertTrue(ion_obj.ref_id == comment.ref_id), "The comment is not to be found in couch storage"
                self.assertTrue(ion_obj.updated == comment.updated), "The comment is not to be found in couch storage"

        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------

        self.ingestion_cli.deactivate_ingestion_configuration(ingestion_configuration_id)
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)

    def test_default_policy(self):
        """
        Test that the default policy is being used properly
        """
        """
        Test that the ingestion workers are writing messages to couch
        """

        #------------------------------------------------------------------------
        # Create ingestion configuration and activate it
        #----------------------------------------------------------------------
        ingestion_configuration_id =  self.ingestion_cli.create_ingestion_configuration(self.exchange_point_id,\
            self.couch_storage, self.hdf_storage, self.number_of_workers, self.default_policy)
        self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)

        #------------------------------------------------------------------------
        # Publish messages
        #----------------------------------------------------------------------

        post = BlogPost( post_id = '1234', title = 'The beautiful life',author = {'name' : 'Jacques', 'email' : 'jacques@cluseaou.com'}, updated = 'too early', content ='summer', stream_id=self.input_stream_id )

        self.ctd_stream1_publisher.publish(post)

        comment = BlogComment(ref_id = '1234',author = {'name': 'Roger', 'email' : 'roger@rabbit.com'}, updated = 'too late',content = 'when summer comes', stream_id=self.input_stream_id)

        self.ctd_stream1_publisher.publish(comment)

        #------------------------------------------------------------------------
        # Test that the policy is
        #----------------------------------------------------------------------

        #@todo after we have implemented how we handle stream depending on how policy gets evaluated, test the implementation


        #------------------------------------------------------------------------
        # Cleanup
        #----------------------------------------------------------------------

        self.ingestion_cli.deactivate_ingestion_configuration(ingestion_configuration_id)
        self.ingestion_cli.delete_ingestion_configuration(ingestion_configuration_id)


    def test_ingestion_worker_receives_message(self):
        """
        Test the activation of the ingestion configuration
        """
        pass



