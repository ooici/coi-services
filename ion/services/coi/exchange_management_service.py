#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log
from pyon.ion.public import RT, AT

from interface.services.coi.iexchange_management_service import BaseExchangeManagementService

class ExchangeManagementService(BaseExchangeManagementService):



    def create_exchange_space(self, name='', org_id=''):
        xs = IonObject(RT.ExchangeSpace, dict(name=name))
        res,rev = self.clients.resource_registry.create(xs)
        aid = self.clients.resource_registry.create_association(org_id, AT.HAS_A, res)

        # Now do the work
        if name == "ioncore":
            pass
        
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
        xn = IonObject(RT.ExchangeName, dict(name=name))
        res,rev = self.clients.resource_registry.create(xn)

        aid = self.clients.resource_registry.create_association(exchange_space, AT.HAS_A, res)

        return res

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
        xp = IonObject(RT.ExchangePoint, dict(name=name))
        res,rev = self.clients.resource_registry.create(xp)

        #aid = self.clients.resource_registry.create_association(exchange_space, AT.HAS_A, res)

        return res


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


