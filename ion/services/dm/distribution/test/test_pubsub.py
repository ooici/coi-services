from pyon.container.cc import IContainerAgent
from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container
from pyon.test.pyontest import PyonTestCase
from interface.services.dm.ipubsub_management_service import IPubsubManagementService
from pyon.core.bootstrap import IonObject


from pyon.public import log


class FakeProcess(object):
    name = ''

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

    def test_runTests(self, container):
        # Now create client to pubsub service
        client = ProcessRPCClient(node=container.node, name="pubsub", iface=IPubsubManagementService, process=FakeProcess())

        self.test_createPubSub(client, container)
        self.test_readPubSub(client, container)
        self.test_deletePubSub(client, container)

    def test_createPubSub(self, client, container):
        archive = {}
        stream_resource_dict = {"mimetype": "", "encoding": "", "archive" : archive, "url" : ""}

        self.streamID,self.streamRev = client.create_stream(stream_resource_dict)

    def test_readPubSub(self, client, container):
        stream_obj = client.read_stream(self.streamID)

    def test_deletePubSub(self, client, container):
        client.delete_stream(self.streamID)

    """
    def test_bank(self):
        
        # Send some requests
        print 'Creating savings account'
        savingsAcctNum = self.client.new_account('kurt', 'Savings')
        print "New savings account number: " + str(savingsAcctNum)
        print "Starting savings balance %s" % str(self.client.get_balances(savingsAcctNum))
        self.client.deposit(savingsAcctNum, 99999999)
        print "Savings balance after deposit %s" % str(self.client.get_balances(savingsAcctNum))
        self.client.withdraw(savingsAcctNum, 1000)
        print "Savings balance after withdrawl %s" % str(self.client.get_balances(savingsAcctNum))

        print "Buying 1000 savings bonds"
        self.client.buy_bonds(savingsAcctNum, 1000)
        print "Savings balance after bond purchase %s" % str(self.client.get_balances(savingsAcctNum))

        checkingAcctNum = self.client.new_account('kurt', 'Checking')
        print "New checking account number: " + str(checkingAcctNum)
        print "Starting checking balance %s" % str(self.client.get_balances(checkingAcctNum))
        self.client.deposit(checkingAcctNum, 99999999)
        print "Confirming checking balance after deposit %s" % str(self.client.get_balances(checkingAcctNum))
        self.client.withdraw(checkingAcctNum, 1000)
        print "Confirming checking balance after withdrawl %s" % str(self.client.get_balances(checkingAcctNum))

        acctList = self.client.list_accounts('kurt')
        self.assertTrue(len(acctList) == 2)

    """