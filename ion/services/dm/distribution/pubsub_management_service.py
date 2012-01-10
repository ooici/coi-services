#!/usr/bin/env python

'''
@package ion.services.dm.distribution.pubsub_management_service Implementation of IPubsubManagementService interface
@file ion/services/dm/distribution/pubsub_management_service.py
@author Tim Giguere
@brief PubSub Management service to keep track of Streams, Publishers, Subscriptions,
and the relationships between them
'''

from interface.services.dm.ipubsub_management_service import \
    BasePubsubManagementService
from pyon.core.exception import NotFound
from pyon.public import RT, AT, log, IonObject
from pyon.net.channel import SubscriberChannel
from pyon.public import CFG


class PubsubManagementService(BasePubsubManagementService):
    '''Implementation of IPubsubManagementService. This class uses resource registry client
        to create streams and subscriptions.
    '''

    XP = 'science.data' # CFG.exchanges.ioncore.exchange_points.science_data.name

    def create_stream(self, stream=None):
        '''Creates a new stream. The id string returned is the ID of the new stream
               in the resource registry.

        @param stream New stream properties.
        @retval id New stream id.
        '''
        log.debug("Creating stream object")
        stream_id, rev = self.clients.resource_registry.create(stream)

        return stream_id

    def update_stream(self, stream={}):
        '''
        Update an existing stream.

        @param stream The stream object with updated properties.
        @retval success Boolean to indicate successful update.
        @todo Add logic to validate optional attributes. Is this interface correct?
        @todo Determine if operation was successful for return value
        '''
        log.debug("Updating stream object: %s" % stream.name)
        id, rev = self.clients.resource_registry.update(stream)
        return True

    def read_stream(self, stream_id=''):
        '''
        Get an existing stream object.

        @param stream_id The id of the stream.
        @retval stream The stream object.
        @throws NotFound when stream doesn't exist.
        '''
        # Return Value
        # ------------
        # stream: {}
        #
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
        stream_obj = self.read_stream(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %d does not exist" % stream_id)

        self.clients.resource_registry.delete(stream_obj)
        return True

    def find_streams(self, filter={}):
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
    
    def find_streams_by_consumer(self, consumer_id=''):
        '''
        Not implemented here.
        '''
        raise NotImplementedError("find_streams_by_consumer not implemented.")

    def create_subscription(self, subscription={}):
        '''
        Create a new subscription. The id string returned is the ID of the new subscription
               in the resource registry.

        @param subscription New subscription properties.
        @retval id The id of the the new subscription.
        '''
        log.debug("Creating subscription object")
        subscription_id, rev = self.clients.resource_registry.create(subscription)

        #we need the stream_id to create the association between the
        #subscription and stream.
        self.clients.resource_registry.create_association(subscription_id, AT.hasStream, subscription.query['stream_id'])
        return subscription_id

    def update_subscription(self, subscription={}):
        '''
        Update an existing subscription.

        @param subscription The subscription object with updated properties.
        @retval success Boolean to indicate successful update.
        '''
        log.debug("Updating subscription object: %s", subscription.name)
        id, rev = self.clients.resource_registry.update(subscription)
        return True

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
        subscription_obj = self.read_subscription(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %s does not exist" % subscription_id)

        # Find and break association with UserIdentity
        subjects, assocs = self.clients.resource_registry.find_subjects(subscription_id, AT.hasStream, subscription_obj.query['stream_id'])
        if not assocs:
            raise NotFound("Subscription to Stream association for subscription id %s does not exist" % subscription_id)
        association_id = assocs[0]._id
        self.clients.resource_registry.delete_association(association_id)
        # Delete the Subscription
        self.clients.resource_registry.delete(subscription_obj)
        return True

    def activate_subscription(self, subscription_id=''):
        '''
        Bind a subscription using a channel layer.

        @param subscription_id The id of the subscription.
        @retval success Boolean to indicate successful activation.
        @throws NotFound when subscription doesn't exist.
        '''
        log.debug("Activating subscription")
        subscription_obj = self.read_subscription(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %s does not exist" % subscription_id)

        ids, assocs = self.clients.resource_registry.find_objects(subscription_id, AT.hasStream, RT.Stream, id_only=True)

        for stream_id in ids:
            print stream_id
            self._bind_subscription(self.XP, subscription_obj.exchange_name, stream_id + '.data')
        return True

    def deactivate_subscription(self, subscription_id=''):
        '''
        Unbinds a subscription using a channel layer.

        @param subscription_id The id of the subscription.
        @retval success Boolean to indicate successful deactivation.
        @throws NotFound when subscription doesn't exist.
        '''
        log.debug("Deactivating subscription")
        subscription_obj = self.read_subscription(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %d does not exist" % subscription_id)

        self._unbind_subscription(self.XP, subscription_obj.exchange_name)

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
        stream_obj = self.read_stream(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %s does not exist" % stream_id)

        stream_obj.producers.append(exchange_name)
        self.update_stream(stream_obj)
        stream_route_obj = IonObject("StreamRoute", routing_key=stream_id + '.data')
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
        log.debug("Unregistering producer with stream")
        stream_obj = self.read_stream(stream_id)
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
        stream_obj = self.read_stream(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %s does not exist" % stream_id)

        return stream_obj.producers

    def _bind_subscription(self, exchange_point, exchange_name, routing_key):
        channel = SubscriptionChannel()
        channel.setup_listener((exchange_point, exchange_name), binding=routing_key)
        channel.start_consume()

    def _unbind_subscription(self, exchange_point, exchange_name):
        channel = SubscriptionChannel()
        channel._recv_name = (exchange_point, exchange_name)
        channel.stop_consume()
        channel.destroy_binding()

    class SubscriptionChannel(SubscriberChannel):

        def _declare_queue(self):
            pass
