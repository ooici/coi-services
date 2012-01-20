"""
Example service that simulates exercising bond trades,
which for now is nothing more than persisting an order
object in a trade log and responding to the caller in
the affirmative.
"""

from pyon.public import IonObject

from interface.services.examples.bank.itrade_service import BaseTradeService

class TradeService(BaseTradeService):

    def exercise(self, order=None):
        # Made up market price of bond
        bond_price = 1.56

        order_create_tuple = self.clients.resource_registry.create(order)

        # Create confirmation response object
        proceeds = order.cash_amount / bond_price if order.type == 'buy' else order.bond_amount * bond_price
        confirmation_obj = IonObject("Confirmation", tracking_number=order_create_tuple[0], status="complete", proceeds=proceeds)

        return confirmation_obj
