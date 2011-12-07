from pyon.container.cc import IContainerAgent
from pyon.net.endpoint import ProcessRPCClient
from pyon.public import Container
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from interface.services.examples.bank.ibank_service import IBankService

from nose.plugins.attrib import attr

class FakeProcess(LocalContextMixin):
    name = ''

@attr('INT')
class Test_Bank(IonIntegrationTestCase):

    def test_bank(self):
        # Start container
        self._start_container()

        # Establish endpoint with container
        container_client = ProcessRPCClient(node=self.container.node, name=self.container.name, iface=IContainerAgent, process=FakeProcess())
        container_client.start_rel_from_url('res/deploy/examples/bank_complete.yml')

        # Now create client to bank service
        client = ProcessRPCClient(node=self.container.node, name="bank", iface=IBankService, process=FakeProcess())

        # Send some requests
        print 'Creating savings account'
        savingsAcctNum = client.new_account('kurt', 'Savings')
        print "New savings account number: " + str(savingsAcctNum)
        print "Starting savings balance %s" % str(client.get_balances(savingsAcctNum))
        client.deposit(savingsAcctNum, 99999999)
        print "Savings balance after deposit %s" % str(client.get_balances(savingsAcctNum))
        client.withdraw(savingsAcctNum, 1000)
        print "Savings balance after withdrawl %s" % str(client.get_balances(savingsAcctNum))

        print "Buying 1000 savings bonds"
        client.buy_bonds(savingsAcctNum, 1000)
        print "Savings balance after bond purchase %s" % str(client.get_balances(savingsAcctNum))

        checkingAcctNum = client.new_account('kurt', 'Checking')
        print "New checking account number: " + str(checkingAcctNum)
        print "Starting checking balance %s" % str(client.get_balances(checkingAcctNum))
        client.deposit(checkingAcctNum, 99999999)
        print "Confirming checking balance after deposit %s" % str(client.get_balances(checkingAcctNum))
        client.withdraw(checkingAcctNum, 1000)
        print "Confirming checking balance after withdrawl %s" % str(client.get_balances(checkingAcctNum))

        acctList = client.list_accounts('kurt')
        self.assertTrue(len(acctList) == 2)
