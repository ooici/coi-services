#! /usr/bin/env python

import unittest
from mock import Mock, patch
from nose.plugins.attrib import attr

from examples.bank.trade_service import TradeService

class _ANY(object):
    def __eq__(self, other):
        return True

@attr('unit')
@patch('pyon.core.object.IonObjectBase')
class TestTradeService(unittest.TestCase):
    """
    Shows examples of patch class decorator, mock data, and mock calls
    """

    def setUp(self):
        self.trade_service = TradeService()
        mock_clients = Mock()
        self.trade_service.clients = mock_clients
        self.mock_resource_registry = mock_clients.resource_registry

    @unittest.skip("no time to do this")
    def test_exercise_sell(self, mock_ionobj):
        pass

    def test_exercise_buy(self, mock_ionobj):
        # set up order
        order = Mock()
        order.type = 'buy'
        order.cash_amount = 156
        # Mock create return value
        mock_create = self.mock_resource_registry.create
        mock_create.return_value = ['111']
        # test our function with our data
        confirmation_obj = self.trade_service.exercise(order)
        # How is the test result
        # assert resource_registry.create did get called with correct
        # arguments
        mock_create.assert_called_once_with(order)
        # assert ion object was created with the correct dict
        asserted_dict = {
                'tracking_number' : '111',
                'status' : 'complete',
                'proceeds' : 100.0}
        mock_ionobj.assert_called_once_with(_ANY(),
                asserted_dict)
