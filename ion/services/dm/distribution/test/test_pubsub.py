from pyon.container.cc import IContainerAgent
from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container
from pyon.test.pyontest import PyonTestCase
from interface.services.dm.ipubsub_management_service \
    import IPubsubManagementService
from interface.services.coi.iresource_registry_service \
    import IResourceRegistryService
from pyon.public import log


class FakeProcess(object):
    name = ''

    def get_context(self):
        pass


class Test_PubSub(PyonTestCase):

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

    def runStream(self, container):
        # Now create client to pubsub service
        client = ProcessRPCClient(node=container.node,
                                  name="pubsub",
                                  iface=IPubsubManagementService,
                                  process=FakeProcess())
        #resourceRegistryClient = ProcessRPCClient(node=container.node,
        #                                          name="resourceregistry",
        #                                          iface=IResourceRegistryService,
        #                                          process=FakeProcess())

        self.createPubSubStream(client)
        self.readPubSubStream(client)
        self.updatePubSubStream(client)
        #self.deletePubSubStream(client)
        self.createSubscription(client)
        self.readSubscription(client)
        self.updateSubscription(client)
        #self.deleteSubscription(client)
        self.registerProducer(client)
        self.unregisterProducer(client)

    def createPubSubStream(self, client):
        #stream_resource_dict = {"mimetype": "", "encoding": "", "archive" : archive, "url" : "", "name" : "SampleStream", "description" : "Sample Stream In PubSub"}
        producers = ['a', 'b', 'c']
        stream_resource_dict = {"mimetype": "", "name": "SampleStream", "description": "Sample Stream In PubSub", "producers": producers}
        self.streamID = client.create_stream(stream_resource_dict)
        
    def readPubSubStream(self, client):
        stream_obj = client.read_stream(self.streamID)

    def updatePubSubStream(self, client):
        stream_obj = client.read_stream(self.streamID)
        stream_obj.name = "SampleStream2"
        stream_obj.description = "Updated Sample Stream"
        client.update_stream(stream_obj)

    def deletePubSubStream(self, client):
        client.delete_stream(self.streamID)

    def createSubscription(self, client):
        subscription_resource_dict = {"name": "SampleSubscription", "description": "Sample Subscription In PubSub"}
        self.subscriptionID = client.create_subscription(subscription_resource_dict)

    def readSubscription(self, client):
        subscription_obj = client.read_subscription(self.subscriptionID)

    def updateSubscription(self, client):
        subscription_obj = client.read_subscription(self.subscriptionID)
        subscription_obj.name = "SampleSubscription2"
        subscription_obj.description = "Updated Sample Subscription"
        client.update_subscription(subscription_obj)
        subscription_obj = client.read_subscription(self.subscriptionID)

    def deleteSubscription(self, client):
        client.delete_subscription(self.subscriptionID)

    def registerProducer(self, client):
        client.register_producer("Test Producer", self.streamID)

    def unregisterProducer(self, client):
        client.unregister_producer("Test Producer", self.streamID)