#!/usr/bin/env python

'''
@package ion.services.dm.distribution.pubsub_management_service Implementation of IPubsubManagementService interface
@file ion/services/dm/distribution/pubsub_management_service.py
@author Tim Giguere
@brief PubSub Management service to keep track of Streams, Publishers, Subscriptions,
and the relationships between them
@TODO implement the stream definition
'''

from interface.services.dm.ipubsub_management_service import\
    BasePubsubManagementService
from pyon.core.exception import NotFound, BadRequest
from pyon.public import RT, PRED, log
from pyon.public import CFG
from interface.objects import Stream, StreamQuery, ExchangeQuery, StreamRoute, StreamDefinition
from interface.objects import Subscription, SubscriptionTypeEnum

# Can't make a couchdb data store here...
### so for now - the pubsub service will just publish the first message on the stream that is creates with the definition


class PubsubManagementService(BasePubsubManagementService):
    '''Implementation of IPubsubManagementService. This class uses resource registry client
        to create streams and subscriptions.
    '''



    def __init__(self, *args, **kwargs):
        BasePubsubManagementService.__init__(self,*args,**kwargs)

        xs_dot_xp = CFG.core_xps.science_data
        try:
            self.XS, xp_base = xs_dot_xp.split('.')
            self.XP = xp_base #'.'.join([bootstrap.get_sys_name(), xp_base])
        except ValueError:
            raise StandardError('Invalid CFG for core_xps.science_data: "%s"; must have "xs.xp" structure' % xs_dot_xp)

    def create_stream_definition(self, container=None, name='', description=''):
        """
        @brief Create a new stream definition which may be used to publish on one or more streams
        @param container is a stream definition container object

        @param container    StreamDefinitionContainer
        @retval stream_definition_id    str
        """
        stream_definition = StreamDefinition(container=container, name=name, description=description)
        stream_def_id, rev = self.clients.resource_registry.create(stream_definition)

        return stream_def_id

    def find_stream_definition(self, stream_id='', id_only=True):
        """@brief Retrieves a stream definition from an existing stream_id
        @param stream_id Stream ID
        @param id_only True if you only want the stream definition id
        @return stream definition object

        @param stream_id    str
        @param id_only    bool
        @retval stream_definition    str
        @throws NotFound    if there is no association
        """

        retval = self.clients.resource_registry.find_objects(subject=stream_id, predicate=PRED.hasStreamDefinition, id_only=id_only)
        if len(retval) != 2:
            raise NotFound('Desired stream definition not found.')
        if len(retval[0]) < 1:
            raise NotFound('Desired stream definition not found.')
        return retval[0][0]

    def update_stream_definition(self, stream_definition=None):
        """Update an existing stream definition

        @param stream_definition    StreamDefinition
        @retval success    bool
        """
        id, rev = self.clients.resource_registry.update(stream_definition)
        #@todo will this throw a not found in the client if the op failed?


    def read_stream_definition(self, stream_definition_id=''):
        """Get an existing stream definition.

        @param stream_definition_id    str
        @retval stream_definition    StreamDefinition
        @throws NotFound    if stream_definition_id doesn't exist
        """
        stream_definition = self.clients.resource_registry.read(stream_definition_id)
        if stream_definition is None:
            raise NotFound("StreamDefinition %s does not exist" % stream_definition_id)
        return stream_definition

    def delete_stream_definition(self, stream_definition_id=''):
        """Delete an existing stream definition.

        @param stream_definition_id    str
        @throws NotFound    if stream_definition_id doesn't exist
        """
        stream_definition = self.clients.resource_registry.read(stream_definition_id)
        if stream_definition is None:
            raise NotFound("StreamDefinition %s does not exist" % stream_definition_id)

        self.clients.resource_registry.delete(stream_definition_id)

    def create_stream(self,encoding='', original=True, stream_definition_id='', name='', description='', url=''):
        '''@brief Creates a new stream. The id string returned is the ID of the new stream in the resource registry.
        @param encoding the encoding for data on this stream
        @param original is the data on this stream from a source or a transform
        @param stream_definition a predefined stream definition type for this stream
        @param name (optional) the name of the stream
        @param description (optional) the description of the stream
        @param url (optional) the url where data from this stream can be found (Not implemented)

        @retval stream_id    str
        '''
        log.debug("Creating stream object")

        stream_obj = Stream(name=name, description=description)
        stream_obj.original = original
        stream_obj.encoding = encoding

        stream_obj.url = url
        stream_id, rev = self.clients.resource_registry.create(stream_obj)

        if stream_definition_id != '':
            self.clients.resource_registry.create_association(stream_id, PRED.hasStreamDefinition, stream_definition_id)

        return stream_id

    def update_stream(self, stream=None):
        '''
        Update an existing stream.

        @param stream The stream object with updated properties.
        @retval success Boolean to indicate successful update.
        @todo Add logic to validate optional attributes. Is this interface correct?
        @todo Determine if operation was successful for return value
        '''
        log.debug("Updating stream object: %s" % stream.name)
        id, rev = self.clients.resource_registry.update(stream)
        #@todo will this throw a NotFound in the client if there was a problem?

    def read_stream(self, stream_id=''):
        '''
        Get an existing stream object.

        @param stream_id The id of the stream.
        @retval stream The stream object.
        @throws NotFound when stream doesn't exist.
        '''
        log.debug("Reading stream object id: %s", stream_id)
        stream_obj = self.clients.resource_registry.read(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %s does not exist" % stream_id)
        return stream_obj

    def delete_stream(self, stream_id=''):
        '''
        Delete an existing stream.

        @param stream_id The id of the stream.
        @retval success Boolean to indicate successful deletion.
        @throws NotFound when stream doesn't exist.
        @todo Determine if operation was successful for return value.
        '''
        log.debug("Deleting stream id: %s", stream_id)
        stream_obj = self.clients.resource_registry.read(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %s does not exist" % stream_id)

        self.clients.resource_registry.delete(stream_id)

    def find_streams(self, filter=None):
        '''
        Find a stream in the resource_registry based on the filters provided.

        @param filter ResourceFilter object containing filter values.
        @retval stream_list The list of streams that match the filter.
        '''
        result = []
        objects = self.clients.resource_registry.find_resources(RT.Stream, None, None, False)
        for obj in objects:
            match = True
            for key in filter.keys():
                if (getattr(obj, key) != filter[key]):
                    match = False
            if (match):
                result.append(obj)
        return result

    def find_streams_by_producer(self, producer_id=''):
        '''
        Find all streams that contain a particular producer.

        @param producer_id The id of the producer.
        @retval stream_list The list of streams that contain the producer.
        '''
        def containsProducer(obj):
            if producer_id in obj.producers:
                return True
            else:
                return False

        objects = self.clients.resource_registry.find_resources(RT.Stream, None, None, False)
        result = filter(containsProducer, objects)
        return result

    def get_stream_route_for_stream(self, stream_id='', exchange_point=''):
        '''
        @brief Returns StreamRoute object for a given stream and exchange point
        @param stream_id identifier for the stream
        @param exchange_point name of the exchange point
        @retval StreamRoute object
        '''
        if not exchange_point:
            exchange_point = 'science_data'
        # Verifies the stream
        self.read_stream(stream_id)
        route_obj = StreamRoute(exchange_point=exchange_point, routing_key='%s.data' % stream_id)
        return route_obj


    def find_streams_by_consumer(self, consumer_id=''):
        '''
        Not implemented here.
        '''
        raise NotImplementedError("find_streams_by_consumer not implemented.")

    def create_subscription(self, query=None, exchange_name='', name='', description='', exchange_point=''):
        '''
        @brief Create a new subscription. The id string returned is the ID of the new subscription
        in the resource registry.
        @param query is a subscription query object (Stream Query, Exchange Query, etc...)
        @param exchange_name is the name (queue) where messages will be delivered for this subscription
        @param name (optional) is the name of the subscription
        @param description (optional) is the description of the subscription

        @param query    Unknown
        @param exchange_name    str
        @param name    str
        @param description    str
        @retval subscription_id    str
        @throws BadRequestError    Throws when the subscription query object type is not found
        '''
        log.debug("Creating subscription object")
        subscription = Subscription(name, description=description)
        subscription.exchange_name = exchange_name
        subscription.exchange_point = exchange_point or 'science_data'
        subscription.query = query
        if isinstance(query, StreamQuery):
            subscription.subscription_type = SubscriptionTypeEnum.STREAM_QUERY
        elif isinstance(query, ExchangeQuery):
            subscription.subscription_type = SubscriptionTypeEnum.EXCHANGE_QUERY
        else:
            raise BadRequest("Query type does not exist")

        subscription_id, _ = self.clients.resource_registry.create(subscription)

        #we need the stream_id to create the association between the
        #subscription and stream.
        if subscription.subscription_type == SubscriptionTypeEnum.STREAM_QUERY:
            for stream_id in subscription.query.stream_ids:
                self.clients.resource_registry.create_association(subscription_id, PRED.hasStream, stream_id)

        return subscription_id

    def update_subscription(self, subscription_id='', query=None):
        '''Update an existing subscription.
        @param subscription_id Identification for the subscription
        @param query The new query
        @throws NotFound if the resource doesn't exist
        @retval True on success
        '''

        subscription = self.clients.resource_registry.read(subscription_id)
        subscription_type = subscription.subscription_type
        log.debug("Updating subscription object: %s", subscription.name)


        if subscription_type == SubscriptionTypeEnum.EXCHANGE_QUERY:
            raise BadRequest('Attempted to change query type on a subscription resource.')


        book = dict()

        stream_ids, assocs = self.clients.resource_registry.find_objects(
            subject=subscription_id,
            predicate=PRED.hasStream,
            id_only=True
        )
        # Create a dictionary with  { stream_id : association } entries.
        for stream_id, assoc in zip(stream_ids, assocs):
            book[stream_id] = assoc


        if subscription.subscription_type == SubscriptionTypeEnum.STREAM_QUERY and isinstance(query,StreamQuery):
            current_streams = set(stream_ids)
            updated_streams = set(query.stream_ids)
            removed_streams = current_streams.difference(updated_streams)
            added_streams = updated_streams.difference(current_streams)

            for stream_id in removed_streams:
                self.clients.resource_registry.delete_association(book[stream_id])
                if subscription.is_active:
                    self._unbind_subscription(subscription.exchange_point,subscription.exchange_name, '%s.data' % stream_id)

            for stream_id in added_streams:
                self.clients.resource_registry.create_association(
                    subject=subscription_id,
                    predicate=PRED.hasStream,
                    object=stream_id
                )
                if subscription.is_active:
                    self._bind_subscription(subscription.exchange_point,subscription.exchange_name, '%s.data' % stream_id)

            subscription.query.stream_ids = current_streams
            id, rev = self.clients.resource_registry.update(subscription)
            return True

        else:
            log.info('Updating an inactive subscription!')


        return False

    def read_subscription(self, subscription_id=''):
        '''
        Get an existing subscription object.

        @param subscription_id The id of the subscription.
        @retval subscription The subscription object.
        @throws NotFound when subscription doesn't exist.
        '''
        log.debug("Reading subscription object id: %s", subscription_id)
        subscription_obj = self.clients.resource_registry.read(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %s does not exist" % subscription_id)
        return subscription_obj

    def delete_subscription(self, subscription_id=''):
        '''
        Delete an existing subscription.

        @param subscription_id The id of the subscription.
        @retval success Boolean to indicate successful deletion.
        @throws NotFound when subscription doesn't exist.
        @todo Determine if operation was successful for return value
        '''
        log.debug("Deleting subscription id: %s", subscription_id)
        subscription_obj = self.clients.resource_registry.read(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %s does not exist" % subscription_id)

        assocs = self.clients.resource_registry.find_associations(subscription_id, PRED.hasStream)
        if assocs is None:
            raise NotFound('Subscription to Stream association for subscription id %s does not exist' % subscription_id)
        for assoc in assocs:
            self.clients.resource_registry.delete_association(assoc._id)        # Find and break association with Streams

        # Delete the Subscription
        self.clients.resource_registry.delete(subscription_id)
        return True

    def activate_subscription(self, subscription_id=''):
        '''
        Bind a subscription using a channel layer.

        @param subscription_id The id of the subscription.
        @retval success Boolean to indicate successful activation.
        @throws NotFound when subscription doesn't exist.
        '''
        log.debug("Activating subscription")
        subscription_obj = self.clients.resource_registry.read(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %s does not exist" % subscription_id)

        if subscription_obj.is_active:
            raise BadRequest('Subscription is already active!')

        ids, _ = self.clients.resource_registry.find_objects(subscription_id, PRED.hasStream, RT.Stream, id_only=True)

        if subscription_obj.subscription_type == SubscriptionTypeEnum.STREAM_QUERY:
            for stream_id in ids:
                self._bind_subscription(subscription_obj.exchange_point, subscription_obj.exchange_name, stream_id + '.data')
        elif subscription_obj.subscription_type == SubscriptionTypeEnum.EXCHANGE_QUERY:
            self._bind_subscription(subscription_obj.exchange_point, subscription_obj.exchange_name, '*.data')

        subscription_obj.is_active = True

        self.clients.resource_registry.update(object=subscription_obj)

        return True

    def deactivate_subscription(self, subscription_id=''):
        '''
        Unbinds a subscription using a channel layer.

        @param subscription_id The id of the subscription.
        @retval success Boolean to indicate successful deactivation.
        @throws NotFound when subscription doesn't exist.
        '''
        log.debug("Deactivating subscription")
        subscription_obj = self.clients.resource_registry.read(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %s does not exist" % subscription_id)

        if not subscription_obj.is_active:
            raise BadRequest('Subscription is not active!')

        ids, _ = self.clients.resource_registry.find_objects(subscription_id, PRED.hasStream, RT.Stream, id_only=True)

        if subscription_obj.subscription_type == SubscriptionTypeEnum.STREAM_QUERY:
            for stream_id in ids:
                self._unbind_subscription(subscription_obj.exchange_point, subscription_obj.exchange_name, stream_id + '.data')

        elif subscription_obj.subscription_type == SubscriptionTypeEnum.EXCHANGE_QUERY:
            self._unbind_subscription(subscription_obj.exchange_point, subscription_obj.exchange_name, '*.data')

        subscription_obj.is_active = False

        self.clients.resource_registry.update(object=subscription_obj)

        return True

    def register_consumer(self, exchange_name=''):
        '''
        Not implmented here.
        '''
        raise NotImplementedError("register_consumer not implemented.")

    def unregister_consumer(self, exchange_name=''):
        '''
        Not implemented here
        '''
        raise NotImplementedError("unregister_consumer not implemented.")

    def find_consumers_by_stream(self, stream_id=''):
        '''
        Not implemented here.
        '''
        raise NotImplementedError("find_consumers_by_stream not implemented.")

    def register_producer(self, exchange_name='', stream_id=''):
        '''
        Register a producer with a stream.

        @param exchange_name The producer exchange name to register.
        @param stream_id The id of the stream.
        @retval credentials Credentials for a publisher to use.
        @throws NotFound when stream doesn't exist.
        '''
        log.debug("Registering producer with stream")
        stream_obj = self.clients.resource_registry.read(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %s does not exist" % stream_id)

        stream_obj.producers.append(exchange_name)
        self.update_stream(stream_obj)
        stream_route_obj = StreamRoute(routing_key=stream_id + '.data')
        return stream_route_obj

    def unregister_producer(self, exchange_name='', stream_id=''):
        '''
        Unregister a producer with a stream.

        @param exchange_name The producer exchange name to unregister.
        @param stream_id The id of the stream.
        @retval success Boolean to indicate successful unregistration.
        @throws NotFound when stream doesn't exist.
        @throws ValueError if producer is not registered with the stream.
        '''
        log.debug("Unregistering producer with stream_id %s " % stream_id)
        stream_obj = self.clients.resource_registry.read(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %s does not exist" % stream_id)

        if (exchange_name in stream_obj.producers):
            stream_obj.producers.remove(exchange_name)
            self.update_stream(stream_obj)
            return True
        else:
            raise ValueError('Producer %s not found in stream %s' % (exchange_name, stream_id))

    def find_producers_by_stream(self, stream_id=''):
        '''
        Returns a list of registered producers for a stream.

        @param stream_id The id of the stream.
        @retval producer_list List of producers for the stream.
        @throws NotFound when stream doesn't exist.
        '''
        log.debug("Finding producers by stream")
        stream_obj = self.clients.resource_registry.read(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %s does not exist" % stream_id)

        return stream_obj.producers

    def _bind_subscription(self, exchange_point, exchange_name, routing_key):

        # create an XN
        xn = self.container.ex_manager.create_xn_queue(exchange_name)

        # create an XP
        xp = self.container.ex_manager.create_xp(exchange_point)

        # bind it on the XP
        xn.bind(routing_key, xp)


    def _unbind_subscription(self, exchange_point, exchange_name, routing_key):

        # create an XN
        xn = self.container.ex_manager.create_xn_queue(exchange_name)

        # create an XP
        xp = self.container.ex_manager.create_xp(exchange_point)

        # unbind it on the XP
        xn.unbind(routing_key, xp)

