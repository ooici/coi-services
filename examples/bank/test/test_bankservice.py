#!/usr/bin/env python

import unittest
from mock import Mock, patch
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, NotFound
from examples.bank.bank_service import BankService

@attr('unit')
class TestBankService(unittest.TestCase):

    def setUp(self):
        """Examples of patch with clean up"""
        patcher = patch('pyon.core.object.IonObjectBase')
        self.addCleanup(patcher.stop)
        
        self.mock_ionobj = patcher.start()
        self.bank_service = BankService()
        mock_clients = Mock()
        self.bank_service.clients = mock_clients
        self.mock_resource_registry = mock_clients.resource_registry

    def test_new_acct_existing_customer(self):
        pass

    def test_new_acct_new_customer(self):
        pass

    def test_deposit_not_found(self):
        mock_read = self.mock_resource_registry.read
        mock_read.return_value = None
        self.assertRaises(NotFound, self.bank_service.deposit, account_id=4)
        mock_read.assert_called_once_with(4)

    def test_deposit_okay(self):
        mock_read = self.mock_resource_registry.read
        mock_read.return_value = Mock()
        mock_read.return_value.cash_balance = 10
        mock_update = self.mock_resource_registry.update
        
        self.bank_service.deposit(account_id=4, amount=5)
       
        mock_read.assert_called_once_with(4)
        self.assertEqual(mock_read.return_value.cash_balance, 15)
        mock_update.assert_called_once_with(mock_read.return_value) 

    def test_withdraw_not_found(self):
        pass

    def test_withdraw_insufficient_funds(self):
        mock_read = self.mock_resource_registry.read
        mock_read.return_value = Mock()
        mock_read.return_value.cash_balance = 3
        mock_update = self.mock_resource_registry.update

        self.assertRaises(BadRequest, self.bank_service.withdraw, account_id=4, amount=5)
        # No change in balance
        self.assertEqual(mock_read.return_value.cash_balance, 3)
        mock_read.assert_called_once_with(4)
        # mock update not called
        self.assertEqual(mock_update.call_count, 0)
        

    def test_withdraw_okay(self):
        pass

    def test_get_balances_not_found(self):
        pass

    def test_get_balances_ok(self):
        pass

    def test_buy_bonds_not_found(self):
        pass

    def test_buy_bonds_insufficient_funds(self):
        pass

    def test_buy_bonds_ok(self):
        pass

    def test_sell_bonds_not_found(self):
        pass

    def test_sell_bonds_insufficient_funds(self):
        pass

    def test_sell_bonds_ok(self):
        pass

    def test_list_accounts_no_customers(self):
        pass

    def test_list_accounts_ok(self):
        pass
