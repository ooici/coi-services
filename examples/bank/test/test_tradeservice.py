#! /usr/bin/env python

import unittest
from mock import Mock, patch

from examples.bank.trade_service import TradeService
from nose.plugins.attrib import attr

class _ANY(object):
    def __eq__(self, other):
        return True

@attr('unit')
class TestTradeService(unittest.TestCase):
    
    def setUp(self):
        patcher = patch('pyon.core.object.IonObjectBase')
        self.addCleanup(patcher.stop)
        self.mock_ionobj = patcher.start()

    @unittest.skip("no time to do this")
    def test_exercise_sell(self):
        pass

    def test_exercise_buy(self):
        # set up order
        order = Mock()
        order.type = 'buy'
        order.cash_amount = 156
        trade_service = TradeService()
        # Mock clients
        mock_clients = Mock()
        trade_service.clients = mock_clients
        mock_create = mock_clients.resource_registry.create
        mock_create.return_value = ['111']
        # test our function with our data
        confirmation_obj = trade_service.exercise(order)
        # How is the test result
        # assert resource_registry.create did get called with correct
        # arguments
        mock_create.assert_called_once_with(order)
        # assert returned object was correct
        asserted_dict = {'tracking_number' : '111',
                    'status' : 'complete',
                    'proceeds' : 100.0}
        self.mock_ionobj.assert_called_once_with(_ANY(),
                asserted_dict)

if __name__ == '__main__':
    unittest.main()
