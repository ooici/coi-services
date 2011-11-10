"""
Example service that provides basic banking functionality.
This service tracks customers and their accounts (checking or saving)
"""

from pyon.public import Container
from pyon.core.bootstrap import IonObject
from pyon.core.exception import BadRequest, NotFound
from pyon.datastore.datastore import DataStore
from pyon.net.endpoint import RPCClient
from pyon.util.log import log

from interface.services.examples.bank.ibank_service import BaseBankService

class BankService(BaseBankService):

    def new_account(self, name='', account_type='Checking'):
        find_res = []
        try:
            find_res = self.clients.resource_registry.find([("type_", DataStore.EQUAL, "BankCustomer"), DataStore.AND, ("name", DataStore.EQUAL, name)])
            customer_info = find_res[0]
            customer_id = customer_info._id
        except NotFound:
            # New customer
            pass
        if len(find_res) == 0:
            # Create customer info entry
            customer_info = {}
            customer_info["name"] = name
            customer_obj = IonObject("BankCustomer", customer_info)
            customer_create_tuple = self.clients.resource_registry.create(customer_obj)
            customer_id = customer_create_tuple[0]

        # Create account entry
        account_info = {}
        account_info["account_type"] = account_type
        account_info["owner"] = customer_id
        account_obj = IonObject("BankAccount", account_info)
        account_create_tuple = self.clients.resource_registry.create(account_obj)
        account_id = account_create_tuple[0]

        return account_id

    def deposit(self, account_id=-1, amount=0.0):
        account_obj = self.clients.resource_registry.read(account_id)
        if account_obj is None:
            raise NotFound("Account %d does not exist" % account_id)
        account_obj.cash_balance += amount
        self.clients.resource_registry.update(account_obj)
        return "Balance after cash deposit: %s" % (str(account_obj.cash_balance))

    def withdraw(self, account_id=-1, amount=0.0):
        account_obj = self.clients.resource_registry.read(account_id)
        if account_obj is None:
            raise NotFound("Account %d does not exist" % account_id)
        if account_obj.cash_balance < amount:
            raise BadRequest("Insufficient funds")
        account_obj.cash_balance -= amount
        self.clients.resource_registry.update(account_obj)
        return "Balance after cash withdrawl: %s" % (str(account_obj.cash_balance))

    def get_balances(self, account_id=-1):
        account_obj = self.clients.resource_registry.read(account_id)
        if account_obj is None:
            raise NotFound("Account %d does not exist" % account_id)
        return account_obj.cash_balance, account_obj.bond_balance

    def buy_bonds(self, account_id='', cash_amount=0.0):
        """
        Purchase the specified amount of bonds.  Check is first made
        that the cash account has sufficient funds.
        """
        account_obj = self.clients.resource_registry.read(account_id)
        if account_obj is None:
            raise NotFound("Account %d does not exist" % account_id)
        if account_obj.cash_balance < cash_amount:
            raise BadRequest("Insufficient funds")

        # Create order object and call trade service
        order_info = {}
        order_info["type"] = "buy"
        order_info["on_behalf"] = account_obj.owner
        order_info["cash_amount"] = cash_amount
        order_obj = IonObject("Order", order_info)

        confirmation_obj = self.clients.trade.exercise(order_obj)

        if confirmation_obj.status == "complete":
            account_obj.cash_balance -= cash_amount
            account_obj.bond_balance += confirmation_obj.proceeds
            self.clients.resource_registry.update(account_obj)
            return "Balances after bond purchase: cash %f    bonds: %s" % (account_obj.cash_balance,account_obj.bond_balance)
        return "Bond purchase status is: %s" % confirmation_obj.status

    def sell_bonds(self, account_id='', quantity=0):
        """
        Sell the specified amount of bonds.  Check is first made
        that the account has sufficient bonds.
        """
        account_obj = self.clients.resource_registry.read(account_id)
        if account_obj is None:
            raise NotFound("Account %d does not exist" % account_id)
        if account_obj.bond_balance < quantity:
            raise BadRequest("Insufficient bonds")

        # Create order object and call trade service
        order_info = {}
        order_info["type"] = "sell"
        order_info["on_behalf"] = account_obj.owner
        order_info["bond_amount"] = quantity
        order_obj = IonObject("Order", order_info)

        confirmation_obj = self.clients.trade.exercise(order_obj)

        if confirmation_obj.status == "complete":
            account_obj.cash_balance += confirmation_obj.proceeds
            account_obj.bond_balance -= quantity
            self.clients.resource_registry.update(account_obj)
            return "Balances after bond sales: cash %f    bonds: %s" % (account_obj.cash_balance,account_obj.bond_balance)
        return "Bond sales status is: %s" % confirmation_obj.status

    def list_accounts(self, name=''):
        """
        Find all accounts (optionally of type) owned by user
        """
        try:
            customer_list = self.clients.resource_registry.find([("type_", DataStore.EQUAL, "BankCustomer"), DataStore.AND, ("name", DataStore.EQUAL, name)])
        except:
            log.error("No customers found!")
            return []
        customer_obj = customer_list[0]
        accounts = self.clients.resource_registry.find([("type_", DataStore.EQUAL, "BankAccount"), DataStore.AND, ("owner", DataStore.EQUAL, customer_obj._id)])
        return accounts
