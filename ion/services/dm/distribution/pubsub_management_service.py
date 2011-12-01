#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'

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
from pyon.core.bootstrap import IonObject, AT
from pyon.datastore.datastore import DataStore
from pyon.public import log


class PubsubManagementService(BasePubsubManagementService):
    '''Implementation of IPubsubManagementService. This class uses resource registry client
        to create streams and subscriptions.
    '''

    def create_stream(self, stream=None):
        '''Create a new stream.

        @param stream New stream properties.
        @retval id New stream id.
        '''
#        log.debug("create_stream" + stream.name)
#        assert not hasattr(stream, "_id"), "ID already set"
#        # Register the stream; create and store the resource and associations
#        stream_id,rev = self.clients.resource_registry.create(stream)
#        #aid = self.clients.resource_registry.create_association(...)
#
#        # More biz logic here....
#
#        # Return the stream id
#        return stream_id
        log.debug("Creating stream object: %s" % stream.name)
        stream_obj = IonObject("Stream", stream)
        stream_id, rev = self.clients.resource_registry.create(stream_obj)

        return stream_id

    def update_stream(self, stream={}):
        '''
        Update an existing stream.

        @param stream The stream object with updated properties.
        @retval success Boolean to indicate successful update.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Updating stream object: %s" % stream.name)
        return self.clients.resource_registry.update(stream)

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
        log.debug("Reading stream object id: %d" % stream_id)
        stream_obj = self.clients.resource_registry.read(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %d does not exist" % stream_id)
        return stream_obj

    def delete_stream(self, stream_id=''):
        '''
        Delete an existing stream.

        @param stream_id The id of the stream.
        @retval success Boolean to indicate successful deletion.
        @throws NotFound when stream doesn't exist.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Deleting stream id: %d" % stream_id)
        stream_obj = self.read_stream(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %d does not exist" % stream_id)

        return self.clients.resource_registry.delete(stream_obj)

    def find_streams(self, filter={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # stream_list: []
        #
        pass

    def find_streams_by_producer(self, producer_id=''):
        '''
        Find all streams that contain a particular producer

        @param producer_id The id of the producer
        @retval stream_list The list of streams that contain the producer
        '''
        # Return Value
        # ------------
        # stream_list: []
        #
        pass
    
    def find_streams_by_consumer(self, consumer_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # stream_list: []
        #
        pass

    def create_subscription(self, subscription={}):
        '''
        Create a new subscription.

        @param subscription New subscription properties.
        @retval id The id of the the new subscription.
        '''
        # Return Value
        # ------------
        # {subscription_id: ''}
        #
        log.debug("Creating subscription object: %s" % subscription.name)
        subscription_obj = IonObject("Subscription", subscription)
        id, rev = self.clients.resource_registry.create(subscription_obj)

        return id

    def update_subscription(self, subscription={}):
        '''
        Update an existing subscription.

        @param subscription The subscription object with updated properties.
        @retval success Boolean to indicate successful update.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Updating subscription object: %s" % subscription.name)
        return self.clients.resource_registry.update(subscription)

    def read_subscription(self, subscription_id=''):
        '''
        Get an existing subscription object.

        @param subscription_id The id of the subscription.
        @retval subscription The subscription object.
        @throws NotFound when subscription doesn't exist.
        '''
        # Return Value
        # ------------
        # subscription: {}
        #
        log.debug("Reading subscription object id: %d" % subscription_id)
        subscription_obj = self.clients.resource_registry.read(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %d does not exist" % subscription_id)
        return subscription_obj

    def delete_subscription(self, subscription_id=''):
        '''
        Delete an existing subscription.

        @param subscription_id The id of the subscription.
        @retval success Boolean to indicate successful deletion.
        @throws NotFound when subscription doesn't exist.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Deleting subscription id: %d" % subscription_id)
        subscription_obj = self.read_subscription(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %d does not exist" % subscription_id)

        return self.clients.resource_registry.delete(subscription_obj)

    def activate_subscription(self, subscription_id=''):
        '''
        Activate a subscription.

        @param subscription_id The id of the subscription.
        @retval success Boolean to indicate successful activation.
        @throws NotFound when subscription doesn't exist.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Activating subscription")
        subscription_obj = self.read_subscription(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %d does not exist" % subscription_id)

    def deactivate_subscription(self, subscription_id=''):
        '''
        Deactivate a subscription.

        @param subscription_id The id of the subscription.
        @retval success Boolean to indicate successful deactivation.
        @throws NotFound when subscription doesn't exist.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Deactivating subscription")
        subscription_obj = self.read_subscription(subscription_id)
        if subscription_obj is None:
            raise NotFound("Subscription %d does not exist" % subscription_id)

    def register_consumer(self, exchange_name=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def unregister_consumer(self, exchange_name=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_consumers_by_stream(self, stream_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # consumer_list: []
        #
        pass

    def register_producer(self, exchange_name='', stream_id=''):
        '''
        Register a producer with a stream.

        @param exchange_name The producer exchange name to register.
        @param stream_id The id of the stream.
        @retval credentials Credentials for a publisher to use.
        @throws NotFound when stream doesn't exist.
        '''
        # logic to create credentials for a publisher to use
        # to place data onto stream.
        # return mock credentials
        log.debug("Registering producer with stream")
        stream_obj = self.read_stream(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %d does not exist" % stream_id)

        stream_obj.producers.append(exchange_name)
        return "credentials"

    def unregister_producer(self, exchange_name='', stream_id=''):
        '''
        Unregister a producer with a stream.

        @param exchange_name The producer exchange name to unregister.
        @param stream_id The id of the stream.
        @retval success Boolean to indicate successful unregistration.
        @throws NotFound when stream doesn't exist.
        '''
        # Return Value
        # ------------
        # {success: true}
        #
        log.debug("Unregistering producer with stream")
        stream_obj = self.read_stream(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %d does not exist" % stream_id)

        stream_obj.producers.remove(exchange_name)
        return True

    def find_producers_by_stream(self, stream_id=''):
        '''
        Return the list of producers for a stream.

        @param stream_id The id of the stream.
        @retval producer_list List of producers for the stream.
        @throws NotFound when stream doesn't exist.
        '''
        # Return Value
        # ------------
        # producer_list: []
        #
        log.debug("Finding producers by stream")
        stream_obj = self.read_stream(stream_id)
        if stream_obj is None:
            raise NotFound("Stream %d does not exist" % stream_id)

        return stream_obj.producers
        
