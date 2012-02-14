#!/usr/bin/env python

'''
@file ion/services/dm/test/test_blog_integration.py
@author Swarbhanu Chatterjee
@author David Stuebe
@test ion.services.dm.test.test_blog_integration Covers a demonstration of the basic capability to ingest and replay
simple data from a blog consisting of posts and comments.

This test starts the DM services and sets up ingestion for the science_data exachange point. Data is published by a
scapper providing input to the system and then replayed as part of the test. The test also subscribes to the incoming
data and verifies that the initial input matched the replayed data.

'''

import gevent
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
from pyon.public import CFG, IonObject, log, RT, PRED, LCS, StreamPublisher, StreamSubscriber, StreamPublisherRegistrar, StreamSubscriberRegistrar
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.icontainer_agent import ContainerAgentClient

from interface.objects import StreamQuery, ExchangeQuery, ProcessDefinition
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService
from interface.objects import BlogPost, BlogComment
import time

class BlogListener(object):

    def __init__(self):

        #-----------------------------------------------------------------------------------------------------------
        # Variables that are used for debugging tests... use find to see how these variables in the course of tests
        #-----------------------------------------------------------------------------------------------------------
        self.num_of_messages = 0

        self.blogs ={}

        # Contains the subscriber that calls back on the blog_store method
        self.subscriber = None



    def blog_store(self, message, headers):
        """
        Use a method in BlogListener object to hold state as we receive blog messages
        """

        log.debug('blog_store message received' )

        # store all posts... since there are very few posts
        if isinstance(message, BlogPost):

            # make a dictionary to contain the post and comments if it does not already exist
            self.blogs[message.post_id] = self.blogs.get(message.post_id, {})

            self.blogs[message.post_id]['post'] = message

        # store only 3 comments
        elif isinstance(message, BlogComment):

            # make a dictionary to contain the post and comments if it does not already exist
            self.blogs[message.ref_id] = self.blogs.get(message.ref_id, {})

            # make a dictionary to contain the comments if it doesn't already exist...
            self.blogs[message.ref_id]['comments'] = self.blogs[message.ref_id].get('comments', {})

            self.blogs[message.ref_id]['comments'][message.updated] = (message)




@attr('INT', group='dm')
class BlogIntegrationTest(IonIntegrationTestCase):

    def setUp(self):
        # set up the container for testing

        #-------------------------------------------------------------------------------------------------------
        # Container
        #-------------------------------------------------------------------------------------------------------
        self._start_container()

        self.cc = ContainerAgentClient(node=self.container.node,name=self.container.name)

        self.cc.start_rel_from_url('res/deploy/r2dm.yml')

        # make a registrar object - this is work usually done for you by the container in a transform or data stream process
        self.subscriber_registrar = StreamSubscriberRegistrar(process=self.container, node=self.container.node)

        #-----------------------------------------------------------------------------------------------------
        # Service clients
        #-----------------------------------------------------------------------------------------------------
        self.pubsub_cli = PubsubManagementServiceClient(node=self.cc.node)
        self.tms_cli = TransformManagementServiceClient(node=self.cc.node)
        self.ingestion_cli = IngestionManagementServiceClient(node=self.cc.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.cc.node)
        self.dr_cli = DataRetrieverServiceClient(node=self.cc.node)
        self.dsm_cli = DatasetManagementServiceClient(node=self.cc.node)




    def test_blog_ingestion_replay(self):

        #-------------------------------------------------------------------------------------------------------
        # Create and activate ingestion configuration
        #-------------------------------------------------------------------------------------------------------

        ingestion_configuration_id = self.ingestion_cli.create_ingestion_configuration(exchange_point_id='science_data', couch_storage={},\
            hdf_storage={},  number_of_workers=6, default_policy={})
        # activates the transforms... so bindings will be created in this step
        self.ingestion_cli.activate_ingestion_configuration(ingestion_configuration_id)

        #------------------------------------------------------------------------------------------------------
        # Create subscriber to listen to the messages published to the ingestion
        #------------------------------------------------------------------------------------------------------

        # Define the query we want
        query = ExchangeQuery()

        # Create the stateful listener to hold the captured data for comparison with replay
        captured_input = BlogListener()

        # Start the subscription
        self._create_subscriber(subscription_name='input_capture_queue', subscription_query=query , blog_listener = captured_input)

        #-------------------------------------------------------------------------------------------------------
        # Launching blog scraper
        #-------------------------------------------------------------------------------------------------------
        self._launch_blog_scraper()

        # wait ten seconds for some data to come in...
        log.warn('Sleeping for 10 seconds to wait for some input')
        time.sleep(10)


        #------------------------------------------------------------------------------------------------------
        # For 3 posts captured, make 3 replays and verify we get back what came in
        #------------------------------------------------------------------------------------------------------


        # Cute list comprehension method does not give enough control
        #self.assertTrue(len(captured_input.blogs)>3)
        #post_ids = [id for idx, id in enumerate(captured_input.blogs.iterkeys()) if idx < 3]

        post_ids = []

        if len(captured_input.blogs) < 1:
            self.fail('No data returned in ten seconds by the blog scrappers!')

        for post_id, blog in captured_input.blogs.iteritems(): # Use items not iter items - I copy of fixed length

            log.info('Captured Input: %s' % post_id)
            if len(blog.get('comments',[])) > 2:
                post_ids.append(post_id)

            if len(post_ids) >3:
                break

        else:
            self.fail('Not enough comments returned by the blog scrappers in 30 seconds')



        #------------------------------------------------------------------------------------------------------
        # Create subscriber to listen to the replays
        #------------------------------------------------------------------------------------------------------


        captured_replays = {}

        for idx, post_id in enumerate(post_ids):
            # Create the stateful listener to hold the captured data for comparison with replay

            replay_id, stream_id = self._create_replay(post_id)


            query = StreamQuery(stream_ids=[stream_id])

            captured_replay = BlogListener()

            self._create_subscriber(subscription_name='replay_capture_queue_%d' % idx, subscription_query=query , blog_listener = captured_replay)

            self.dr_cli.start_replay(replay_id)

            captured_replays[post_id] = captured_replay




        # wait five seconds for some data to come in...
        log.warn('Sleeping for 5 seconds to wait for some output')
        time.sleep(5)

        matched_comments={}
        for post_id, captured_replay in captured_replays.iteritems():

            # There should be only one blog in here!
            self.assertEqual(len(captured_replay.blogs),1)

            replayed_blog = captured_replay.blogs[post_id]

            input_blog = captured_input.blogs[post_id]

            self.assertEqual(replayed_blog['post'].content, input_blog['post'].content)

            # can't deterministically assert that the number of comments is the same...
            matched_comments[post_id] = 0

            for updated, comment in replayed_blog.get('comments',{}).iteritems():
                self.assertIn(updated, input_blog['comments'])
                matched_comments[post_id] += 1


        # Assert that we got some comments back!
        self.assertTrue(sum(matched_comments.values()) > 0)

        log.info('Matched comments on the following blogs: %s' % matched_comments)




    def _create_subscriber(self, subscription_name, subscription_query, blog_listener):
        #------------------------------------------------------------------------------------------------------
        # Create subscriber to listen to the messages published to the ingestion
        #------------------------------------------------------------------------------------------------------

        # Make a subscription to the input stream to ingestion
        subscription_id = self.pubsub_cli.create_subscription(query = subscription_query, exchange_name=subscription_name ,name = subscription_name)


        # It is not required or even generally a good idea to use the subscription resource name as the queue name, but it makes things simple here
        # Normally the container creates and starts subscribers for you when a transform process is spawned
        subscriber = self.subscriber_registrar.create_subscriber(exchange_name=subscription_name, callback=blog_listener.blog_store)
        subscriber.start()

        blog_listener.subscriber = subscriber

        self.pubsub_cli.activate_subscription(subscription_id)

        return subscription_id

    def _launch_blog_scraper(self):
        #-------------------------------------------------------------------------------------------------------
        # Launching blog scraper
        #-------------------------------------------------------------------------------------------------------

        blogs = [
            'voodoofunk',
            'google-code-featured',
            'googleblog',
            'deerhuntertheband',
            'googlewebmastercentral',
            'strobist',
            'chrome',
            'hellohighprices',
            'saintsandspinners',
            'warnakubrunei']


        blogs = [
            'saintsandspinners',
            'strobist',
            'voodoofunk'
            ]


        log.debug('before spawning blog scraper')

        for blog in blogs:
            config = {'process':{'type':'stream_process','blog':blog}}
            self.cc.spawn_process(name=blog,
                module='ion.services.dm.ingestion.example.blog_scraper',
                cls='FeedStreamer',
                config=config)

    def _create_replay(self, post_id):
        """
        Define the replay
        """
        dataset_id = self.dsm_cli.create_dataset(
            stream_id=post_id,
            datastore_name='dm_datastore',
            view_name='posts/posts_join_comments'
        )


        return self.dr_cli.define_replay(dataset_id)




