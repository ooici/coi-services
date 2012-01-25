#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log, RT, PRED

from interface.services.coi.iexchange_management_service import BaseExchangeManagementService
from pyon.core.exception import Conflict, Inconsistent, NotFound
from pyon.util.log import log

class ExchangeManagementService(BaseExchangeManagementService):

    """
    The Exchange Management Service is the service that manages the Exchange and its associated resources, such as Exchange Spaces, Names, Points and Brokers.

    """
    def create_exchange_space(self, exchange_space=None, org_id=''):
        """Creates an Exchange Space distributed resource from the parameter exchangespace object.

        @param exchange_space    ExchangeSpace
        @param org_id    str
        @retval exchange_space_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        log.debug("create_exchange_space(%s, org_id=%s)" % (exchange_space, org_id))
        self.assert_condition(exchange_space and org_id, "Arguments not set")
        exchange_space_id,rev = self.clients.resource_registry.create(exchange_space)

        aid = self.clients.resource_registry.create_association(org_id, PRED.hasExchangeSpace, exchange_space_id)

        # Now do the work
        if exchange_space.name == "ioncore":
            # Bottom turtle initialization
            pass
        else:
            # All other XS initialization
            pass
        
        return exchange_space_id

    def update_exchange_space(self, exchange_space=None):
        """Updates an existing Exchange Space resource with data passed in as a parameter.

        @param exchange_space    ExchangeSpace
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(exchange_space)

    def read_exchange_space(self, exchange_space_id=''):
        """Returns an Exchange Space resource for the provided exchange space id.

        @param exchange_space_id    str
        @retval exchange_space    ExchangeSpace
        @throws NotFound    object with specified id does not exist
        """
        exchange_space = self.clients.resource_registry.read(exchange_space_id)
        if not exchange_space:
            raise NotFound("Exchange Space %s does not exist" % exchange_space_id)
        return exchange_space


    def delete_exchange_space(self, exchange_space_id=''):
        """Deletes an existing exchange space resource for the provided id.

        @param exchange_space_id    str
        @throws NotFound    object with specified id does not exist
        """
        exchange_space = self.clients.resource_registry.read(exchange_space_id)
        if not exchange_space:
            raise NotFound("Exchange Space %s does not exist" % exchange_space_id)
        self.clients.resource_registry.delete(exchange_space)

    def find_exchange_spaces(self, filters=None):
        """Returns a list of Exchange Space resources for the given Resource Filter.

        @param filters    ResourceFilter
        @retval exchange_space_list    []
        """
        raise NotImplementedError()


    def declare_exchange_name(self, exchange_name=None, exchange_space_id=''):
        """Create an Exchange Name resource resource

        @param exchange_name    ExchangeName
        @param exchange_space_id    str
        @retval canonical_name    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        exchange_name_id,rev = self.clients.resource_registry.create(exchange_name)

        aid = self.clients.resource_registry.create_association(exchange_space, PRED.hasExchangeName, exchange_name_id)

        return exchange_name_id  #QUestion - is this the correct canonical name?

    def undeclare_exchange_name(self, canonical_name='', exchange_space_id=''):
        """Remove an exhange nane resource

        @param canonical_name    str
        @param exchange_space_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        raise NotImplementedError()

    def find_exchange_names(self, filters=None):
        """Returns a list of exchange name resources for the given resource filter.

        @param filters    ResourceFilter
        @retval exchange_name_list    []
        """
        raise NotImplementedError()

    def create_exchange_point(self, exchange_point=None, exchange_space_id=''):
        """Create an exchange point resource within the exchange space provided by the id.

        @param exchange_point    ExchangePoint
        @param exchange_space_id    str
        @retval exchange_point_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        exchange_point_id, _ver = self.clients.resource_registry.create(exchange_point)

        #aid = self.clients.resource_registry.create_association(exchange_space_id, PRED.hasExchangePoint, exchange_point_id)

        return exchange_point_id


    def update_exchange_point(self, exchange_point=None):
        """Update an existing exchange point resource.

        @param exchange_point    ExchangePoint
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(exchange_point)


    def read_exchange_point(self, exchange_point_id=''):
        """Return an existing exchange point resource.

        @param exchange_point_id    str
        @retval exchange_point    ExchangePoint
        @throws NotFound    object with specified id does not exist
        """
        exchange_point = self.clients.resource_registry.read(exchange_point_id)
        if not exchange_point:
            raise NotFound("Exchange Point %s does not exist" % exchange_point_id)
        return exchange_point

    def delete_exchange_point(self, exchange_point_id=''):
        """Delete an existing exchange point resource.

        @param exchange_point_id    str
        @throws NotFound    object with specified id does not exist
        """
        exchange_point = self.clients.resource_registry.read(exchange_point_id)
        if not exchange_point:
            raise NotFound("Exchange Point %s does not exist" % exchange_point_id)
        self.clients.resource_registry.delete(exchange_point)

    def find_exchange_points(self, filters=None):
        """Returns a list of exchange point resources for the provided resource filter.

        @param filters    ResourceFilter
        @retval exchange_point_list    []
        """
        raise NotImplementedError()


    def create_exchange_broker(self, exchange_broker=None):
        """Creates an exchange broker resource

        @param exchange_broker    ExchangeBroker
        @retval exchange_broker_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        exchange_point_id, _ver = self.clients.resource_registry.create(exchange_point)
        return exchange_point_id

    def update_exchange_broker(self, exchange_broker=None):
        """Updates an existing exchange broker resource.

        @param exchange_broker    ExchangeBroker
        @throws BadRequest    if object does not have _id or _rev attribute
        @throws NotFound    object with specified id does not exist
        @throws Conflict    object not based on latest persisted object version
        """
        self.clients.resource_registry.update(exchange_broker)

    def read_exchange_broker(self, exchange_broker_id=''):
        """Returns an existing exchange broker resource.

        @param exchange_broker_id    str
        @retval exchange_broker    ExchangeBroker
        @throws NotFound    object with specified id does not exist
        """
        exchange_broker = self.clients.resource_registry.read(exchange_broker_id)
        if not exchange_broker:
            raise NotFound("Exchange Broker %s does not exist" % exchange_broker_id)
        return exchange_broker

    def delete_exchange_broker(self, exchange_broker_id=''):
        """Deletes an existing exchange broker resource.

        @param exchange_broker_id    str
        @throws NotFound    object with specified id does not exist
        """
        exchange_broker = self.clients.resource_registry.read(exchange_broker_id)
        if not exchange_broker:
            raise NotFound("Exchange Broker %s does not exist" % exchange_broker_id)
        self.clients.resource_registry.delete(exchange_broker)

    def find_exchange_broker(self, filters=None):
        """Returns a list of exchange broker resources for the provided resource filter.

        @param filters    ResourceFilter
        @retval exchange_broker_list    []
        """
        raise NotImplementedError()
