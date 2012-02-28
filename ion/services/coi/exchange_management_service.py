#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log, RT, PRED

from interface.services.coi.iexchange_management_service import BaseExchangeManagementService
from pyon.core.exception import Conflict, Inconsistent, NotFound, BadRequest
from pyon.util.log import log
#from pyon.ion.exchange import ExchangeSpace, ExchangeName, ExchangePoint
from pyon.ion import exchange

class ExchangeManagementService(BaseExchangeManagementService):

    """
    The Exchange Management Service is the service that manages the Exchange and its associated resources, such as Exchange Spaces, Names, Points and Brokers.

    """
    def create_exchange_space(self, exchange_space=None, org_id=''):
        """Creates an Exchange Space distributed resource from the parameter exchange_space object.

        @param exchange_space    ExchangeSpace
        @param org_id    str
        @retval exchange_space_id    str
        @throws BadRequest    if object passed has _id or _rev attribute
        """
        log.debug("create_exchange_space(%s, org_id=%s)" % (exchange_space, org_id))
        self.assert_condition(exchange_space and org_id, "Arguments not set")

        #First make sure that Org with the org_id exists, otherwise bail
        org = self.clients.resource_registry.read(org_id)
        if not org:
            raise NotFound("Org %s does not exist" % org_id)

        exchange_space_id,rev = self.clients.resource_registry.create(exchange_space)

        aid = self.clients.resource_registry.create_association(org_id, PRED.hasExchangeSpace, exchange_space_id)

        # Now do the work

#        if exchange_space.name == "ioncore":
#            # Bottom turtle initialization
#            # @TODO: what's different here
#            self.container.ex_manager.create_xs(exchange_space.name)
#        else:
        self.container.ex_manager.create_xs(exchange_space.name, use_ems=False)
        
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

        # remove association between itself and org
        _, assocs = self.clients.resource_registry.find_subjects(RT.Org, PRED.hasExchangeSpace, exchange_space_id, id_only=True)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)

        # delete assocs to XNs
        _, assocs = self.clients.resource_registry.find_objects(exchange_space_id, PRED.hasExchangeName, RT.ExchangeName, id_only=True)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)

        # delete assocs to XPs
        _, assocs = self.clients.resource_registry.find_objects(exchange_space_id, PRED.hasExchangePoint, RT.ExchangePoint, id_only=True)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)

        # delete XS now
        self.clients.resource_registry.delete(exchange_space_id)

        # call container API to delete
        xs = exchange.ExchangeSpace(self.container.ex_manager, exchange_space.name)
        self.container.ex_manager.delete_xs(xs, use_ems=False)

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
        # get xntype and translate @TODO should we just consolidate these to be the same?
        typemap = { 'XN_SERVICE':'service', 'XN_PROCESS':'process', 'XN_QUEUE':'queue' }
        if not exchange_name.xn_type in typemap:
            raise BadRequest("Unknown exchange name type: %s" % exchange_name.xn_type)

        xntype = typemap[exchange_name.xn_type]

        exchange_space          = self.read_exchange_space(exchange_space_id)
        exchange_name_id,rev    = self.clients.resource_registry.create(exchange_name)

        aid = self.clients.resource_registry.create_association(exchange_space_id, PRED.hasExchangeName, exchange_name_id)

        # call container API
        xs = exchange.ExchangeSpace(self.container.ex_manager, exchange_space.name)
        self.container.ex_manager._create_xn(xntype, exchange_name.name, xs, use_ems=False)

        return exchange_name_id  #QUestion - is this the correct canonical name?

    def undeclare_exchange_name(self, canonical_name='', exchange_space_id=''):
        """Remove an exhange nane resource

        @param canonical_name    str
        @param exchange_space_id    str
        @retval success    bool
        @throws NotFound    object with specified id does not exist
        """
        # @TODO: currently we are using the exchange_name's id as the canonical name
        # and exchange_space_id is unused?
        exchange_name = self.clients.resource_registry.read(canonical_name)
        if not exchange_name:
            raise NotFound("Exchange Name with id %s does not exist" % canonical_name)

        exchange_name_id = exchange_name._id        # yes, this should be same, but let's make it look cleaner

        # get associated XS first
        exchange_space_list, assoc_list = self.clients.resource_registry.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, exchange_point_id)
        if not len(exchange_space_list) == 1:
            raise NotFound("Associated Exchange Space to Exchange Point %s does not exist" % exchange_point_id)

        exchange_space = exchange_space_list[0]

        # remove association between itself and XS
        _, assocs = self.clients.resource_registry.find_subjects(RT.ExchangeSpace, PRED.hasExchangeName, exchange_name_id, id_only=True)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)

        # remove XN
        self.clients.resource_registry.delete(exchange_name_id)

        # call container API
        xs = exchange.ExchangeSpace(self.container.ex_manager, exchange_space.name)
        xn = exchange.ExchangeName(self.container.ex_manager, exchange_point.name, xs)              # type doesn't matter here
        self.container.ex_manager.delete_xn(xn, use_ems=False)

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
        exchange_space          = self.read_exchange_space(exchange_space_id)
        exchange_point_id, _ver = self.clients.resource_registry.create(exchange_point)

        aid = self.clients.resource_registry.create_association(exchange_space_id, PRED.hasExchangePoint, exchange_point_id)

        # call container API
        xs = exchange.ExchangeSpace(self.container.ex_manager, exchange_space.name)
        self.container.ex_manager.create_xp(exchange_point.name, xs, xptype=exchange_point.topology_type, use_ems=False)

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

        # get associated XS first
        exchange_space_list, assoc_list = self.clients.resource_registry.find_subjects(RT.ExchangeSpace, PRED.hasExchangePoint, exchange_point_id)
        if not len(exchange_space_list) == 1:
            raise NotFound("Associated Exchange Space to Exchange Point %s does not exist" % exchange_point_id)

        exchange_space = exchange_space_list[0]

        # delete association to XS
        for assoc in assoc_list:
            self.clients.resource_registry.delete_association(assoc._id)

        # delete from RR
        self.clients.resource_registry.delete(exchange_point_id)

        # call container API
        xs = exchange.ExchangeSpace(self.container.ex_manager, exchange_space.name)
        xp = exchange.ExchangePoint(self.container.ex_manager, exchange_point.name, xs, 'ttree')
        self.container.ex_manager.delete_xp(xp, use_ems=False)

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
