#! /usr/bin/env python

'''
@file examples/bank/test/test_tradeservice.py
@author Jamie Chen
@test examples.bank.trade_service Unit test suite to cover all trade service code
'''

import unittest
from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr

from examples.bank.trade_service import TradeService
from interface.services.coi.iresource_registry_service import BaseResourceRegistryService

@attr('UNIT')
class TestTradeService(PyonTestCase):

    def setUp(self):
        self.mock_ionobj = self._create_IonObject_mock('examples.bank.trade_service.IonObject')
        self._create_service_mock('trade')

        self.trade_service = TradeService()
        self.trade_service.clients = self.clients

        # Rename to save some typing
        self.mock_create = self.resource_registry.create

    def test_exercise_buy(self):
        # set up order
        order = Mock()
        order.type = 'buy'
        order.cash_amount = 156
        self.mock_create.return_value = ['111']

        # TEST: Execute the service operation call
        confirmation_obj = self.trade_service.exercise(order)

        # How is the test result
        # assert resource_registry.create did get called with correct
        # arguments
        assert confirmation_obj is self.mock_ionobj.return_value
        self.mock_create.assert_called_once_with(order)
        # assert mock ion object is called
        self.mock_ionobj.assert_called_once_with('Confirmation',
                status='complete', tracking_number='111', proceeds=156 /
                1.56)

    def test_exercise_sell(self):
        order = Mock()
        order.type = 'sell'
        order.bond_amount = 156
        self.mock_create.return_value = ['123']

        confirmation_obj = self.trade_service.exercise(order)

        self.mock_create.assert_called_once_with(order)
        self.mock_ionobj.assert_called_once_with('Confirmation',
                status='complete', tracking_number='123',
                proceeds= 156 * 1.56)
        assert confirmation_obj is self.mock_ionobj.return_value
