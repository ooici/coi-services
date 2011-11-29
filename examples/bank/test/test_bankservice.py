#!/usr/bin/env python

'''
@file examples/bank/test/test_bankservice.py
@author Jamie Chen
@test examples.bank.bank_service Unit test suite to cover all bank service code
'''

import unittest
from mock import Mock, sentinel
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, NotFound
from examples.bank.bank_service import BankService
from pyon.datastore.datastore import DataStore
from pyon.util.unit_test import pop_last_call, PyonTestCase
from interface.services.coi.iresource_registry_service import BaseResourceRegistryService
from interface.services.examples.bank.itrade_service import BaseTradeService

@attr('unit')
class TestBankService(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_object_mock('examples.bank.bank_service.IonObject')
        self._create_service_mock('bank'),

        self.bank_service = BankService()
        self.bank_service.clients = self.clients

        # Rename to save some typing
        self.mock_create = self.resource_registry.create
        self.mock_read = self.resource_registry.read
        self.mock_update = self.resource_registry.update
        self.mock_find_by_name = self.resource_registry.find_by_name
        self.mock_find_by_type = self.resource_registry.find_by_type
        self.mock_exercise = self.trade.exercise

    def test_new_acct_existing_customer(self):
        self.mock_find_by_name.return_value = (['id_5'], 'I do not care')
        self.mock_create.return_value = ('id_2', 'I do not care')

        account_id = self.bank_service.new_account('John')

        self.mock_find_by_name.assert_called_once_with('John',
                'BankCustomer', True)
        self.mock_ionobj.assert_called_once_with('BankAccount',
                account_type='Checking', owner='id_5')
        self.mock_create.assert_called_once_with(self.mock_ionobj.return_value)
        self.assertEqual(account_id, 'id_2')

    def test_new_acct_new_customer(self):
        self.mock_find_by_name.return_value = ([], 'I do not care')
        # Use mock side effect to simulate two different return results
        results = [('acct_id_2', 'I do not care'),
                ('cust_id_1', 'I do not care')]
        def side_effect(*args, **kwargs):
            return results.pop()
        self.mock_create.side_effect = side_effect

        account_id = self.bank_service.new_account('John')

        self.mock_find_by_name.assert_called_once_with('John',
                'BankCustomer', True)
        # assert last call first, pop the stack, assert the previous one
        self.assertEqual(self.mock_ionobj.call_count, 2)
        self.mock_ionobj.assert_called_with('BankAccount',
                account_type='Checking', owner='cust_id_1')
        pop_last_call(self.mock_ionobj)
        self.mock_ionobj.assert_called_once_with('BankCustomer', name='John')
        # assert last call first, pop the stack, assert the previous one
        self.assertEqual(self.mock_create.call_count, 2)
        self.mock_create.assert_called_with(self.mock_ionobj.return_value)
        pop_last_call(self.mock_create)
        self.mock_create.assert_called_once_with(self.mock_ionobj.return_value)
        self.assertEqual(account_id, 'acct_id_2')

    def test_deposit_not_found(self):
        self.mock_read.return_value = None

        # exercise code
        with self.assertRaises(NotFound) as cm:
            self.bank_service.deposit('id_5')

        ex = cm.exception
        self.assertEqual(ex.message, 'Account id_5 does not exist')
        self.mock_read.assert_called_once_with('id_5', '')

    def test_deposit_okay(self):
        self.mock_read.return_value.cash_balance = 10

        # exercise code in test
        status = self.bank_service.deposit(account_id='id_5', amount=5)

        self.assertEqual(status, 'Balance after cash deposit: 15')
        self.mock_read.assert_called_once_with('id_5', '')
        self.assertEqual(self.mock_read.return_value.cash_balance, 10 + 5)
        self.mock_update.assert_called_once_with(self.mock_read.return_value)

    def test_withdraw_not_found(self):
        self.mock_read.return_value = None

        # exercise code
        with self.assertRaises(NotFound) as cm:
            self.bank_service.withdraw('id_5')

        ex = cm.exception
        self.assertEqual(ex.message, 'Account id_5 does not exist')
        self.mock_read.assert_called_once_with('id_5', '')

    def test_withdraw_insufficient_funds(self):
        self.mock_read.return_value.cash_balance = 3

        # exercise code in test
        with self.assertRaises(BadRequest) as cm:
            self.bank_service.withdraw('id_5', 5)

        # verify exception message
        ex = cm.exception
        self.assertEqual(ex.message, 'Insufficient funds')
        # No change in balance
        self.mock_read.assert_called_once_with('id_5', '')
        self.assertEqual(self.mock_read.return_value.cash_balance, 3)
        # mock update not called
        self.assertEqual(self.mock_update.call_count, 0)

    def test_withdraw_okay(self):
        self.mock_read.return_value.cash_balance = 10

        # exercise code in test
        status = self.bank_service.withdraw(account_id='id_5', amount=4)

        self.assertEqual(status, 'Balance after cash withdraw: 6')
        self.mock_read.assert_called_once_with('id_5', '')
        self.assertEqual(self.mock_read.return_value.cash_balance, 10 - 4)
        self.mock_update.assert_called_once_with(self.mock_read.return_value)

    def test_get_balances_not_found(self):
        self.mock_read.return_value = None

        # exercise code
        with self.assertRaises(NotFound) as cm:
            self.bank_service.get_balances('id_5')

        ex = cm.exception
        self.assertEqual(ex.message, 'Account id_5 does not exist')
        self.mock_read.assert_called_once_with('id_5', '')

    def test_get_balances_ok(self):
        self.mock_read.return_value.cash_balance = 10
        self.mock_read.return_value.bond_balance = 5

        # exercise code in test
        cash_balance, bond_balance = self.bank_service.get_balances('id_5')

        self.assertEqual(cash_balance, 10)
        self.assertEqual(bond_balance, 5)
        self.mock_read.assert_called_once_with('id_5', '')

    def test_buy_bonds_not_found(self):
        self.mock_read.return_value = None

        # exercise code
        with self.assertRaises(NotFound) as cm:
            self.bank_service.buy_bonds('id_5', '')

        ex = cm.exception
        self.assertEqual(ex.message, 'Account id_5 does not exist')
        self.mock_read.assert_called_once_with('id_5', '')

    def test_buy_bonds_insufficient_funds(self):
        self.mock_read.return_value.cash_balance = 3

        # exercise code in test
        with self.assertRaises(BadRequest) as cm:
            self.bank_service.buy_bonds('id_5', 5)

        # verify exception message
        ex = cm.exception
        self.assertEqual(ex.message, 'Insufficient funds')
        self.mock_read.assert_called_once_with('id_5', '')
        # No exercise happened
        self.assertEqual(self.mock_exercise.call_count, 0)

    def test_buy_bonds_complete(self):
        mock_account_obj = Mock()
        self.mock_read.return_value = mock_account_obj
        mock_account_obj.cash_balance = 10
        mock_account_obj.owner = 'David'
        mock_account_obj.bond_balance = 30
        self.mock_exercise.return_value.proceeds = 20
        self.mock_exercise.return_value.status = 'complete'

        # exercise code in test
        status = self.bank_service.buy_bonds('id_5', 4)

        self.mock_read.assert_called_once_with('id_5', '')
        self.mock_ionobj.assert_called_once_with('Order', type='buy',
                on_behalf='David', cash_amount=4)
        self.mock_exercise.assert_called_once_with(self.mock_ionobj.return_value)
        self.mock_update.assert_called_once_with(mock_account_obj)
        self.assertEqual(status, 'Balances after bond purchase: cash %f, bonds: %s' %  (10 - 4, 30 + 20))

    def test_buy_bonds_pending(self):
        mock_account_obj = Mock()
        self.mock_read.return_value = mock_account_obj
        mock_account_obj.cash_balance = 10
        mock_account_obj.owner = 'David'
        mock_account_obj.bond_balance = 30
        self.mock_exercise.return_value.proceeds = 20
        self.mock_exercise.return_value.status = 'pending'
        # exercise code in test
        status = self.bank_service.buy_bonds('id_5', 4)

        self.mock_read.assert_called_once_with('id_5', '')
        self.mock_ionobj.assert_called_once_with('Order', type='buy',
                on_behalf='David', cash_amount=4)
        self.mock_exercise.assert_called_once_with(self.mock_ionobj.return_value)
        # update should not be called
        self.assertEqual(self.mock_update.call_count, 0)
        self.assertEqual(status, 'Bond purchase status is: pending')

    def test_sell_bonds_not_found(self):
        self.mock_read.return_value = None

        # exercise code
        with self.assertRaises(NotFound) as cm:
            self.bank_service.sell_bonds('id_5')

        ex = cm.exception
        self.assertEqual(ex.message, 'Account id_5 does not exist')
        self.mock_read.assert_called_once_with('id_5', '')

    def test_sell_bonds_insufficient_funds(self):
        self.mock_read.return_value.bond_balance = 3

        # exercise code in test
        with self.assertRaises(BadRequest) as cm:
            self.bank_service.sell_bonds('id_5', 5)
        # verify exception message

        ex = cm.exception
        self.assertEqual(ex.message, 'Insufficient bonds')
        self.mock_read.assert_called_once_with('id_5', '')
        # No exercise happened
        self.assertEqual(self.mock_exercise.call_count, 0)

    def test_sell_bonds_complete(self):
        mock_account_obj = Mock()
        self.mock_read.return_value = mock_account_obj
        mock_account_obj.cash_balance = 10
        mock_account_obj.owner = 'David'
        mock_account_obj.bond_balance = 30
        self.mock_exercise.return_value.proceeds = 20
        self.mock_exercise.return_value.status = 'complete'

        # exercise code in test
        status = self.bank_service.sell_bonds('id_5', 4)

        self.mock_read.assert_called_once_with('id_5', '')
        self.mock_ionobj.assert_called_once_with('Order', type='sell',
                on_behalf='David', bond_amount=4)
        self.mock_exercise.assert_called_once_with(self.mock_ionobj.return_value)
        self.mock_update.assert_called_once_with(mock_account_obj)
        self.assertEqual(status, 'Balances after bond sales: cash %f, bonds: %s' % (10 + 20, 30 - 4))

    def test_sell_bonds_pending(self):
        mock_account_obj = Mock()
        self.mock_read.return_value = mock_account_obj
        mock_account_obj.cash_balance = 10
        mock_account_obj.owner = 'David'
        mock_account_obj.bond_balance = 30
        self.mock_exercise.return_value.proceeds = 20
        self.mock_exercise.return_value.status = 'pending'

        # exercise code in test
        status = self.bank_service.sell_bonds('id_5', 4)

        self.mock_read.assert_called_once_with('id_5', '')
        self.mock_ionobj.assert_called_once_with('Order', type='sell',
                on_behalf='David', bond_amount=4)
        self.mock_exercise.assert_called_once_with(self.mock_ionobj.return_value)
        # update should not be called
        self.assertEqual(self.mock_update.call_count, 0)
        self.assertEqual(status, 'Bond sales status is: pending')

    def test_list_accounts_no_customers(self):
        self.mock_find_by_name.return_value = ([], 'I do not care')

        accounts = self.bank_service.list_accounts('Roger')

        self.assertEqual(accounts, [])
        self.mock_find_by_name.assert_called_once_with('Roger',
                'BankCustomer', False)

    def test_list_accounts_ok(self):
        # Use mock side effect to simulate exception thrown
        mock_customer_obj = Mock()
        mock_customer_obj._id = 'id_244'
        self.mock_find_by_name.return_value = ([mock_customer_obj], 'I do not care')
        mock_account1 = Mock()
        mock_account1.owner = 'not_good'
        mock_account2 = Mock()
        mock_account2.owner = 'id_244'
        mock_accounts = [mock_account1, mock_account2]
        self.mock_find_by_type.return_value = (mock_accounts, 'I do not care')

        accounts = self.bank_service.list_accounts('Roger')

        self.mock_find_by_name.assert_called_once_with('Roger', 'BankCustomer',
                False)
        self.mock_find_by_type.assert_called_once_with('BankAccount', '',
                False)
        self.assertEqual(accounts, [mock_account2])
