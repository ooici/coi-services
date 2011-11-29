#!/usr/bin/env python

'''
@package examples.bank.bank_service Implementation of IBankService inteface
@file examples/bank/bank_service.py
@author Thomas R. Lennan
@brief Example service that provides basic banking functionality.
This service tracks customers and their accounts (checking or saving)
'''

from pyon.core.exception import BadRequest, NotFound
from pyon.public import IonObject, AT
from pyon.util.log import log

from interface.services.examples.bank.ibank_service import BaseBankService

__author__ = 'Thomas R. Lennan'

class BankService(BaseBankService):
    '''A demo class, it's really just for demonstration.  This class uses
    resource registry client and a trade client.'''

    def new_account(self, name='', account_type='Checking'):
        '''Create a new bank account.

        @note If customer does not exist, create a customer account first before
        creating a bank account.

        @param name Customer name
        @param account_type Checking or Savings
        @retval account_id Newly created bank account id
        '''

        find_res, _ = self.clients.resource_registry.find_by_name(name, "BankCustomer", True)
        if len(find_res) == 0:
            # Create customer info entry
            customer_obj = IonObject("BankCustomer", name=name)
            customer_id, _ = self.clients.resource_registry.create(customer_obj)
        else:
            customer_id = find_res[0]

        # Create account entry
        account_obj = IonObject("BankAccount", account_type=account_type)
        account_id, _ = self.clients.resource_registry.create(account_obj)

        # Create association
        self.clients.resource_registry.create_association(customer_id, AT.hasAccount, account_id)

        return account_id

    def deposit(self, account_id='', amount=0.0):
        '''Deposits cash into an account

        @param account_id bank account id
        @param amount Cash deposit amount
        @retval status A string specifying balance after cash deposit
        @throws NotFound when account id doesn't exist
        '''

        account_obj = self.clients.resource_registry.read(account_id)
        if not account_obj:
            raise NotFound("Account %s does not exist" % account_id)

        account_obj.cash_balance += amount
        self.clients.resource_registry.update(account_obj)
        return "Balance after cash deposit: %s" % (account_obj.cash_balance)

    def withdraw(self, account_id='', amount=0.0):
        account_obj = self.clients.resource_registry.read(account_id)
        if not account_obj:
            raise NotFound("Account %s does not exist" % account_id)
        if account_obj.cash_balance < amount:
            raise BadRequest("Insufficient funds")

        account_obj.cash_balance -= amount
        self.clients.resource_registry.update(account_obj)
        return "Balance after cash withdraw: %s" % (account_obj.cash_balance)

    def get_balances(self, account_id=''):
        account_obj = self.clients.resource_registry.read(account_id)
        if not account_obj:
            raise NotFound("Account %s does not exist" % account_id)

        return (account_obj.cash_balance, account_obj.bond_balance)

    def buy_bonds(self, account_id='', cash_amount=0.0):
        """
        Purchase the specified amount of bonds.  Check is first made
        that the cash account has sufficient funds.
        """
        account_obj = self.clients.resource_registry.read(account_id)
        if not account_obj:
            raise NotFound("Account %s does not exist" % account_id)
        if account_obj.cash_balance < cash_amount:
            raise BadRequest("Insufficient funds")

        owner_obj = self.clients.resource_registry.find_subjects(account_obj, AT.hasAccount, "BankCustomer", False)[0][0]

        # Create order object and call trade service
        order_obj = IonObject("Order", type="buy", on_behalf=owner_obj.name, cash_amount=cash_amount)

        # Make the call to trade service
        confirmation_obj = self.clients.trade.exercise(order_obj)

        if confirmation_obj.status == "complete":
            account_obj.cash_balance -= cash_amount
            account_obj.bond_balance += confirmation_obj.proceeds
            self.clients.resource_registry.update(account_obj)
            return "Balances after bond purchase: cash %f, bonds: %s" % (account_obj.cash_balance,account_obj.bond_balance)
        return "Bond purchase status is: %s" % confirmation_obj.status

    def sell_bonds(self, account_id='', quantity=0):
        """
        Sell the specified amount of bonds.  Check is first made
        that the account has sufficient bonds.
        """
        account_obj = self.clients.resource_registry.read(account_id)
        if not account_obj:
            raise NotFound("Account %s does not exist" % account_id)
        if account_obj.bond_balance < quantity:
            raise BadRequest("Insufficient bonds")

        owner_obj = self.clients.resource_registry.find_subjects(account_obj, AT.hasAccount, "BankCustomer", False)[0][0]

        # Create order object and call trade service
        order_obj = IonObject("Order", type="sell", on_behalf=owner_obj.name, bond_amount=quantity)

        confirmation_obj = self.clients.trade.exercise(order_obj)

        if confirmation_obj.status == "complete":
            account_obj.cash_balance += confirmation_obj.proceeds
            account_obj.bond_balance -= quantity
            self.clients.resource_registry.update(account_obj)
            return "Balances after bond sales: cash %f, bonds: %s" % (account_obj.cash_balance,account_obj.bond_balance)
        return "Bond sales status is: %s" % confirmation_obj.status

    def list_accounts(self, name=''):
        """
        Find all accounts (optionally of type) owned by user
        """
        customer_list, _ = self.clients.resource_registry.find_by_name(name, "BankCustomer")
        if len(customer_list) == 0:
            log.error("No customers found")
            return []
        customer_obj = customer_list[0]

        accounts, _ = self.clients.resource_registry.find_objects(customer_obj, AT.hasAccount, "BankAccount", False)
        return accounts
