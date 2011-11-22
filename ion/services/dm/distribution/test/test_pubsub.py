from pyon.container.cc import IContainerAgent
from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container
from pyon.test.pyontest import PyonTestCase
from interface.services.dm.ipubsub_management_service import IPubsubManagementService
from pyon.core.bootstrap import IonObject
from pyon.util.log import log

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
        container_client = ProcessRPCClient(node=self.container.node, name=self.container.name, iface=IContainerAgent, process=FakeProcess())
        container_client.start_rel_from_url('/res/deploy/pubsub.yml')

    def tearDown(self):
        self.container.quit()

    def test_noop(self):
        pass

    def runTest(self):
        pass

    def runStream(self, container):
        # Now create client to pubsub service
        client = ProcessRPCClient(node=container.node, name="pubsub", iface=IPubsubManagementService, process=FakeProcess())

        #log.debug("****************************************************************************************")
        #log.debug("*****************************************CREATE*****************************************")
        #log.debug("****************************************************************************************")
        self.createPubSubStream(client, container)
        #log.debug("****************************************************************************************")
        #log.debug("*****************************************READ*******************************************")
        #log.debug("****************************************************************************************")
        self.readPubSubStream(client, container)
        #log.debug("****************************************************************************************")
        #log.debug("*****************************************UPDATE*****************************************")
        #log.debug("****************************************************************************************")
        self.updatePubSubStream(client, container)
        #log.debug("****************************************************************************************")
        #log.debug("*****************************************DELETE*****************************************")
        #log.debug("****************************************************************************************")
        self.deletePubSubStream(client, container)

    def createPubSubStream(self, client, container):
        archive = {}
        #stream_resource_dict = {"mimetype": "", "encoding": "", "archive" : archive, "url" : "", "name" : "SampleStream", "description" : "Sample Stream In PubSub"}
        stream_resource_dict = {"mimetype": "", "name" : "SampleStream", "description" : "Sample Stream In PubSub"}

        self.streamID = client.create_stream(stream_resource_dict)
        
    def readPubSubStream(self, client, container):
        stream_obj = client.read_stream(self.streamID)

    def updatePubSubStream(self, client, container):
        stream_obj = client.read_stream(self.streamID)
        stream_obj.name = "SampleStream2"
        stream_obj.description = "Updated Sample Stream"
        client.update_stream(stream_obj)
        stream_obj = client.read_stream(self.streamID)

    def deletePubSubStream(self, client, container):
        client.delete_stream(self.streamID)