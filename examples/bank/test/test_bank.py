from interface.services.icontainer_agent import ContainerAgentClient
from pyon.core.exception import BadRequest
from pyon.core.interceptor.validate import ValidateInterceptor
from pyon.net.endpoint import RPCClient
from pyon.public import Container
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from interface.services.examples.bank.ibank_service import BankServiceClient
import unittest, os

from nose.plugins.attrib import attr

@attr('LOCOINT', 'INT', group='example')
class Test_Bank(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/examples/bank_complete.yml')

        # Now create client to bank service
        self.client = BankServiceClient(node=self.container.node)

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_bank(self):

        # Send some requests
        print 'Creating savings account'
        savingsAcctNum = self.client.new_account('kurt', "555-555-5555",  ["123 Main Street", "Any Town, USA", "00000"], 'Savings')
        print "New savings account number: " + str(savingsAcctNum)
        print "Starting savings balance %s" % str(self.client.get_balances(savingsAcctNum))
        self.client.deposit(savingsAcctNum, 9999)
        print "Savings balance after deposit %s" % str(self.client.get_balances(savingsAcctNum))
        self.client.withdraw(savingsAcctNum, 1000)
        print "Savings balance after withdrawl %s" % str(self.client.get_balances(savingsAcctNum))

        print "Buying 1000 savings bonds"
        self.client.buy_bonds(savingsAcctNum, 1000)
        print "Savings balance after bond purchase %s" % str(self.client.get_balances(savingsAcctNum))

        checkingAcctNum = self.client.new_account('kurt', "555-555-5555",  ["123 Main Street", "Any Town, USA", "00000"], 'Checking')
        print "New checking account number: " + str(checkingAcctNum)
        print "Starting checking balance %s" % str(self.client.get_balances(checkingAcctNum))
        self.client.deposit(checkingAcctNum, 9999)
        print "Confirming checking balance after deposit %s" % str(self.client.get_balances(checkingAcctNum))
        self.client.withdraw(checkingAcctNum, 1000)
        print "Confirming checking balance after withdrawl %s" % str(self.client.get_balances(checkingAcctNum))

        acctList = self.client.list_accounts('kurt')
        self.assertTrue(len(acctList) == 2)

    @unittest.skip
    def test_bank_op_decorators(self):
        # Test decorator validation on account creation
        with self.assertRaises(BadRequest):
            # Fail to pass required attribute
            self.client.new_account(name=None)

        with self.assertRaises(BadRequest):
            # Fail to pass required attribute
            self.client.new_account(name="Fail", us_phone_number=None)

        with self.assertRaises(BadRequest):
            # Fail to pass required attribute
            self.client.new_account(name="Fail", us_phone_number="555-555-5555", address=None)

        with self.assertRaises(BadRequest):
            # Fail to pass required attribute
            self.client.new_account(name="Fail", us_phone_number="555-555-5555", address=["123 Main Street", "Any Town, USA", "00000"], account_type=None)

        with self.assertRaises(BadRequest):
            # Pass string attribute with wrong value format
            self.client.new_account(name="Fail", us_phone_number="5555555555", address=["123 Main Street", "Any Town, USA", "00000"], account_type="Savings")

        with self.assertRaises(BadRequest):
            # Pass collection attribute with wrong content count
            self.client.new_account(name="Fail", us_phone_number="555-555-5555", address=["123 Main Street", "Any Town, USA, 00000"], account_type="Savings")

        with self.assertRaises(BadRequest):
            # Pass collection attribute with wrong content type
            self.client.new_account(name="Fail", us_phone_number="555-555-5555", address=["123 Main Street", "Any Town, USA", 00000], account_type="Savings")

        # Test numeric range checking
        savingsAcctNum = self.client.new_account('kurt', "555-555-5555",  ["123 Main Street", "Any Town, USA", "00000"], 'Savings')

        with self.assertRaises(BadRequest):
            self.client.deposit(savingsAcctNum, 99999999)
