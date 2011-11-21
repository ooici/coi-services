#!/usr/bin/env python

import unittest
from mock import Mock, patch, sentinel, mocksignature
from nose.plugins.attrib import attr

from pyon.core.exception import BadRequest, NotFound
from examples.bank.bank_service import BankService
from pyon.datastore.datastore import DataStore
from pyon.util.test_utils import pop_last_call
from interface.services.coi.iresource_registry_service import BaseResourceRegistryService
from interface.services.examples.bank.itrade_service import BaseTradeService

@attr('unit')
class TestBankService(unittest.TestCase):

    def create_patch(self, name):
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)
        return thing

    def setUp(self):
        self.mock_ionobj = self.create_patch('examples.bank.bank_service.IonObject')
        self.bank_service = BankService()
        mock_clients = Mock()
        self.bank_service.clients = mock_clients
        mock_resource_registry = mock_clients.resource_registry

        # Force mock create to use signature, an exception will be thrown
        # if you are calling a method with wrong number of args from the
        # interface spec. For example, you are calling an outdated method
        # against a service spec.
        self.mock_create = mocksignature(BaseResourceRegistryService.create,
                mock=Mock(name='mock_create'),
                skipfirst=True)
        self.mock_find = mocksignature(BaseResourceRegistryService.find,
                mock=Mock(name='mock_find'),
                skipfirst=True)
        self.mock_read = mocksignature(BaseResourceRegistryService.read,
                mock=Mock(name='mock_read'),
                skipfirst=True)
        self.mock_update = mocksignature(BaseResourceRegistryService.update,
                mock=Mock(name='mock_update'),
                skipfirst=True)
        mock_resource_registry.create = self.mock_create
        mock_resource_registry.find = self.mock_find
        mock_resource_registry.read = self.mock_read
        mock_resource_registry.update = self.mock_update

        mock_trade = mock_clients.trade
        self.mock_exercise = mocksignature(BaseTradeService.exercise,
                mock=Mock(name='mock_exercise'),
                skipfirst=True)
        mock_trade.exercise = self.mock_exercise

    def test_new_acct_existing_customer(self):

        mock_result = Mock()
        mock_result._id = 'id_5'
        self.mock_find.return_value = [mock_result]
        self.mock_create.return_value = ['id_2']
        account_id = self.bank_service.new_account('John')
        self.mock_find.assert_called_once_with([('type_', DataStore.EQUAL,
            'BankCustomer'), DataStore.AND, ('name', DataStore.EQUAL,
                'John')])
        account_info = {'account_type' : 'Checking',
                'owner' : 'id_5'}
        self.mock_ionobj.assert_called_once_with('BankAccount', account_info)
        self.mock_create.assert_called_once_with(self.mock_ionobj.return_value)
        self.assertEqual(account_id, 'id_2')

    def test_new_acct_new_customer(self):
        # Use mock side effect to simulate exception thrown
        self.mock_find.side_effect = NotFound('not there guys')
        # Use mock side effect to simulate two different return results
        results = [['acct_id_2'], ['cust_id_1']]
        def side_effect(*args, **kwargs):
            return results.pop()
        self.mock_create.side_effect = side_effect
        account_id = self.bank_service.new_account('John')
        self.mock_find.assert_called_once_with([('type_', DataStore.EQUAL,
            'BankCustomer'), DataStore.AND, ('name', DataStore.EQUAL,
                'John')])
        customer_info = {'name' : 'John'}
        account_info = {'account_type' : 'Checking',
                'owner' : 'cust_id_1'}
        self.mock_ionobj.assert_called_with('BankAccount', account_info)
        pop_last_call(self.mock_ionobj)
        # asserting there's no more call before that...hence
        # called_once_with
        self.mock_ionobj.assert_called_once_with('BankCustomer', customer_info)
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
        assert self.mock_read.return_value.cash_balance == 10 + 5
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
            self.bank_service.withdraw('id_5')
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
        order_info = {'type' : 'buy',
                'on_behalf' : 'David',
                'cash_amount' : 4}
        self.mock_ionobj.assert_called_once_with('Order', order_info)
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
        order_info = {'type' : 'buy',
                'on_behalf' : 'David',
                'cash_amount' : 4}
        self.mock_ionobj.assert_called_once_with('Order', order_info)
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
        order_info = {'type' : 'sell',
                'on_behalf' : 'David',
                'bond_amount' : 4}
        self.mock_ionobj.assert_called_once_with('Order', order_info)
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
        order_info = {'type' : 'sell',
                'on_behalf' : 'David',
                'bond_amount' : 4}
        self.mock_ionobj.assert_called_once_with('Order', order_info)
        self.mock_exercise.assert_called_once_with(self.mock_ionobj.return_value)
        # update should not be called
        self.assertEqual(self.mock_update.call_count, 0)
        self.assertEqual(status, 'Bond sales status is: pending')

    def test_list_accounts_no_customers(self):
        self.mock_find.side_effect = Exception()
        accounts = self.bank_service.list_accounts('Roger')
        self.assertEqual(accounts, [])
        self.mock_find.assert_called_once_with([("type_", DataStore.EQUAL,
            "BankCustomer"), DataStore.AND, ("name", DataStore.EQUAL,
                'Roger')])

    def test_list_accounts_ok(self):
        # Use mock side effect to simulate exception thrown
        mock_customer_obj = Mock()
        mock_customer_obj._id = 'id_244'
        # Use mock side effect to simulate two different return results
        # also an examples of using sentinel
        results = [sentinel.accounts, [mock_customer_obj]]
        def side_effect(*args, **kwargs):
            return results.pop()
        self.mock_find.side_effect = side_effect

        accounts = self.bank_service.list_accounts('Roger')
        self.mock_find.assert_called_with([("type_",
            DataStore.EQUAL, "BankAccount"), DataStore.AND, ("owner",
                DataStore.EQUAL, 'id_244')])
        pop_last_call(self.mock_find)
        # asserting there's no more call before that...hence
        # called_once_with
        self.mock_find.assert_called_once_with([("type_", DataStore.EQUAL,
            "BankCustomer"), DataStore.AND, ("name", DataStore.EQUAL,
                'Roger')])
        self.assertEqual(accounts, sentinel.accounts)
