#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject
from pyon.util.log import log

from interface.services.coi.iexchange_management_service import BaseExchangeManagementService

class ExchangeManagementService(BaseExchangeManagementService):

    RT_EXCHANGE_SPACE = "ExchangeSpace"
    RT_EXCHANGE_NAME = "ExchangeName"
    RT_EXCHANGE_POINT = "ExchangePoint"

    def create_exchange_space(self, name='', org_id=''):
        xs = IonObject(self.RT_EXCHANGE_SPACE, dict(name=name))
        res,rev = self.clients.resource_registry.create(xs)

        aid = self.clients.resource_registry.create_association(org_id, "HAS-A", res)

        return res

    def update_exchange_space(self, exchange_space_id={}):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def delete_exchange_space(self, exchange_space_id={}):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def find_exchange_spaces(self, name='*'):
        # Return Value
        # ------------
        # exchange_space_list: []
        # 
        pass

    def create_exchange_name(self, name='', exchange_space={}):
        # Return Value
        # ------------
        # exchange_name_id: {}
        # 
        pass

    def update_exchange_name(self, exchange_name_id={}):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def delete_exchange_name(self, exchange_name_id={}):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def find_exchange_names(self, name='*'):
        # Return Value
        # ------------
        # exchange_name_list: []
        # 
        pass

    def create_exchange_point(self, name=''):
        # Return Value
        # ------------
        # {exchange_point_id: ''}
        # 
        pass

    def update_exchange_point(self, exchange_point_id=''):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def delete_exchange_point(self, exchange_point_id=''):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def find_exchange_points(self, name='*'):
        # Return Value
        # ------------
        # exchange_point_list: []
        # 
        pass

    def find_brokers(self, name='*'):
        # Return Value
        # ------------
        # broker_list: []
        # 
        pass

    def get_broker(self):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def configure_broker(self):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def delete_broker(self):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def join_broker(self):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def leave_broker(self):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass


