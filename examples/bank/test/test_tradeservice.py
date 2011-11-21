#! /usr/bin/env python

import unittest
from mock import Mock, patch, mocksignature
from nose.plugins.attrib import attr

from examples.bank.trade_service import TradeService
from interface.services.coi.iresource_registry_service import BaseResourceRegistryService
@attr('unit')
class TestTradeService(unittest.TestCase):

    def setUp(self):
        self.trade_service = TradeService()
        self.trade_service.clients = Mock(name='mock_clients')
        # Force mock create to use signature, an exception will be thrown
        # if you are calling a method with wrong number of args from the
        # interface spec. For example, you are calling an outdated method
        # against a service spec.
        self.mock_create = mocksignature(BaseResourceRegistryService.create,
                mock=Mock(name='mock_create'),
                skipfirst=True)
        self.trade_service.clients.resource_registry.create = self.mock_create

    @patch('examples.bank.trade_service.IonObject')
    def test_exercise_buy(self, mock_ionobj):
        # set up order
        order = Mock()
        order.type = 'buy'
        order.cash_amount = 156
        self.mock_create.return_value = ['111']
        # test our function with our data
        confirmation_obj = self.trade_service.exercise(order)

        # How is the test result

        # assert resource_registry.create did get called with correct
        # arguments
        assert confirmation_obj is mock_ionobj.return_value
        self.mock_create.assert_called_once_with(order)
        # assert mock ion object is called
        confirmation_dict = {
                'tracking_number' : '111',
                'status' : 'complete',
                'proceeds' : 156 / 1.56}
        mock_ionobj.assert_called_once_with('Confirmation',
                 confirmation_dict)

    @patch('examples.bank.trade_service.IonObject')
    def test_exercise_sell(self, mock_ionobj):
        order = Mock()
        order.type = 'sell'
        order.bond_amount = 156
        self.mock_create.return_value = ['123']
        confirmation_obj = self.trade_service.exercise(order)
        assert confirmation_obj is mock_ionobj.return_value
        self.mock_create.assert_called_once_with(order)
        confirmation_dict = {
                'tracking_number' : '123',
                'status' : 'complete',
                'proceeds' : 156 * 1.56}
        mock_ionobj.assert_called_once_with('Confirmation',
                confirmation_dict)
