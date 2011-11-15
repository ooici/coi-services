#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log
from pyon.ion.public import RT, AT

from interface.services.coi.iexchange_management_service import BaseExchangeManagementService

class ExchangeManagementService(BaseExchangeManagementService):

    def create_exchange_space(self, xs=None, org_id=''):
        log.debug("create_exchange_space" + xs.name)
        assert not hasattr(xs, "_id"), "ID already set"
        xs_id,rev = self.clients.resource_registry.create(xs)

        aid = self.clients.resource_registry.create_association(org_id, AT.HAS_A, xs_id)

        # Now do the work
        if xs.name == "ioncore":
            # Bottom turtle initialization
            pass
        else:
            # All other XS initialization
            pass
        
        return xs_id

    def update_exchange_space(self, xs=None):
        log.debug("update_exchange_space" + xs.name)
        assert hasattr(xs, "_id"), "ID not set"
        xs_id,rev = self.clients.resource_registry.update(xs)
        return True

    def read_exchange_space(self, exchange_space_id=""):
        xs = self.clients.resource_registry.read(exchange_space_id)
        return xs

    def delete_exchange_space(self, exchange_space_id=""):
        raise NotImplementedError()

    def find_exchange_spaces(self, name='*'):
        raise NotImplementedError()

    def create_exchange_name(self, name='', exchange_space={}):
        xn = IonObject(RT.ExchangeName, dict(name=name))
        res,rev = self.clients.resource_registry.create(xn)

        aid = self.clients.resource_registry.create_association(exchange_space, AT.HAS_A, res)

        return res

    def update_exchange_name(self, exchange_name_id=""):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def delete_exchange_name(self, exchange_name_id=""):
        raise NotImplementedError()

    def find_exchange_names(self, name='*'):
        raise NotImplementedError()

    def create_exchange_point(self, name=''):
        xp = IonObject(RT.ExchangePoint, dict(name=name))
        res,rev = self.clients.resource_registry.create(xp)

        #aid = self.clients.resource_registry.create_association(exchange_space, AT.HAS_A, res)
        return res

    def update_exchange_point(self, exchange_point_id=''):
        raise NotImplementedError()

    def delete_exchange_point(self, exchange_point_id=''):
        raise NotImplementedError()

    def find_exchange_points(self, name='*'):
        raise NotImplementedError()
