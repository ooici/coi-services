from mock import Mock
from pyon.container.cc import IContainerAgent
from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container
from pyon.util.unit_test import PyonTestCase
from pyon.public import log
from ion.services.dm.distribution.pubsub_management_service import PubsubManagementService
from pyon.core.bootstrap import IonObject

class FakeProcess(object):
    name = ''

    def get_context(self):
        pass

'''
To run test_pubsub, run the following commands in pycc

cc.start_rel_from_url('res/deploy/pubsub.yml')
from ion.services.dm.distribution.test.test_pubsub import PubSubTest
instance = PubSubTest()
instance.test_runStream(cc)
'''
class PubSubTest(PyonTestCase):

    def setUp(self):
        # Start container
        self.container = Container()
        self.container.start()

        # Establish endpoint with container
        container_client = ProcessRPCClient(node=self.container.node,
                                            name=self.container.name,
                                            iface=IContainerAgent,
                                            process=FakeProcess())
        container_client.start_rel_from_url('/res/deploy/pubsub.yml')

    def tearDown(self):
        self.container.quit()

    def test_noop(self):
        pass

    def runTest(self):
        pass

    def test_runStream(self, container):
        # Now create client to pubsub service
        #client = ProcessRPCClient(node=container.node,
        #                          name="pubsub",
        #                          iface=IPubsubManagementService,
        #                          process=FakeProcess())
        self.mock_ionobj = self._create_IonObject_mock('ion.services.dm.distribution.pubsub_management_service.IonObject')
        self._create_service_mock('pubsub_management')

        self.pubsub_service = PubsubManagementService()
        self.pubsub_service.clients = self.clients

        self.createPubSubStream()
        self.readPubSubStream()
        #self.updatePubSubStream()
        #self.deletePubSubStream()
        self.createSubscription()
        #self.readSubscription()
        #self.updateSubscription()
        #self.deleteSubscription()
        #self.registerProducer()
        #self.unregisterProducer()

    def createPubSubStream(self):
        self.resource_registry.create.return_value = ('id_2', 'I do not care')
        producers = ['producer1', 'producer2', 'producer3']
        stream_resource_dict = {"mimetype": "", "name": "SampleStream", "description": "Sample Stream In PubSub", "producers": producers}
        self.streamID = self.pubsub_service.create_stream(stream_resource_dict)
        #self.mock_find_resources.assert_called_once_with('Stream', None, self.streamID, True)
        
    def readPubSubStream(self):
        stream_obj = self.pubsub_service.read_stream(self.streamID)

    def updatePubSubStream(self):
        stream_obj = self.pubsub_service.read_stream(self.streamID)
        stream_obj.name = "SampleStream2"
        stream_obj.description = "Updated Sample Stream"
        self.pubsub_service.update_stream(stream_obj)

    def deletePubSubStream(self):
        self.pubsub_service.delete_stream(self.streamID)

    def createSubscription(self):
        single_stream_query_dict = {"stream_id": self.streamID}
        subscription_resource_dict = {"name": "SampleSubscription", "description": "Sample Subscription In PubSub", "query": single_stream_query_dict}
        self.subscriptionID = self.pubsub_service.create_subscription(subscription_resource_dict)

    def readSubscription(self):
        subscription_obj = self.pubsub_service.read_subscription(self.subscriptionID)

    def updateSubscription(self):
        subscription_obj = self.pubsub_service.read_subscription(self.subscriptionID)
        subscription_obj.name = "SampleSubscription2"
        subscription_obj.description = "Updated Sample Subscription"
        self.pubsub_service.update_subscription(subscription_obj)
        subscription_obj = self.pubsub_service.read_subscription(self.subscriptionID)

    def deleteSubscription(self):
        self.pubsub_service.delete_subscription(self.subscriptionID)

    def registerProducer(self):
        self.pubsub_service.register_producer("Test Producer", self.streamID)

    def unregisterProducer(self):
        self.pubsub_service.unregister_producer("Test Producer", self.streamID)