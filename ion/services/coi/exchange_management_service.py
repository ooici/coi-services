#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log
from pyon.ion.public import RT, AT

from interface.services.coi.iexchange_management_service import BaseExchangeManagementService

class ExchangeManagementService(BaseExchangeManagementService):

    def create_exchange_space(self, xs=None, org_id=''):
        log.debug("create_exchange_space(%s, org_id=%s)" % (xs, org_id))
        assert xs and org_id, "Arguments not set"
        xs_id,rev = self.clients.resource_registry.create(xs)

        aid = self.clients.resource_registry.create_association(org_id, AT.hasExchangeSpace, xs_id)

        # Now do the work
        if xs.name == "ioncore":
            # Bottom turtle initialization
            pass
        else:
            # All other XS initialization
            pass
        
        return xs_id

    def update_exchange_space(self, xs=None):
        log.debug("update_exchange_space(%s)" % xs)
        xs_id,rev = self.clients.resource_registry.update(xs)
        return True

    def read_exchange_space(self, exchange_space_id=""):
        xs = self.clients.resource_registry.read(exchange_space_id)
        return xs

    def delete_exchange_space(self, exchange_space_id=""):
        raise NotImplementedError()

    def find_exchange_spaces(self, name='*'):
        raise NotImplementedError()


    def declare_exchange_name(exchange_name=None, exchange_space_id=''):
        res,rev = self.clients.resource_registry.create(exchange_name)

        aid = self.clients.resource_registry.create_association(exchange_space, AT.hasExchangeName, res)

        return res

    def undeclare_exchange_name(canonical_name='', exchange_space_id=''):
        raise NotImplementedError()

    def find_exchange_names(self, name='*'):
        raise NotImplementedError()

    def create_exchange_point(self, name=''):
        xp = IonObject(RT.ExchangePoint, dict(name=name))
        res,rev = self.clients.resource_registry.create(xp)

        #aid = self.clients.resource_registry.create_association(exchange_space, AT.hasExchangePoint, res)
        return res

    def update_exchange_point(self, exchange_point_id=''):
        raise NotImplementedError()

    def delete_exchange_point(self, exchange_point_id=''):
        raise NotImplementedError()

    def find_exchange_points(self, name='*'):
        raise NotImplementedError()
