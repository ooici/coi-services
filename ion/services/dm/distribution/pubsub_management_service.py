#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.ipubsub_management_service import BasePubsubManagementService

class PubsubManagementService(BasePubsubManagementService):


    def create_stream(self, stream={}):
        """
        method docstring
        """
        log.debug("create_stream" + stream.name)
        assert not hasattr(stream, "_id"), "ID already set"
        # Register the stream; create and store the resource and associations
        stream_id,rev = self.clients.resource_registry.create(stream)
        #aid = self.clients.resource_registry.create_association(...)

        # More biz logic here....

        # Return the stream id
        return stream_id

    def update_stream(self, stream={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_stream(self, stream_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # stream: {}
        #
        pass

    def delete_stream(self, stream_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

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
        """
        method docstring
        """
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
        """
        method docstring
        """
        # Return Value
        # ------------
        # {subscription_id: ''}
        #
        pass

    def update_subscription(self, subscription={}):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def read_subscription(self, subscription_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # subscription: {}
        #
        pass

    def delete_subscription(self, subscription_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def activate_subscription(self, subscription_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def deactivate_subscription(self, subscription_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

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
        """
        method docstring
        """
        # logic to create credentials for a publisher to use to place data onto stream.
        # return mock credentials
        return "credentials"

    def unregister_producer(self, exchange_name='', stream_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # {success: true}
        #
        pass

    def find_producers_by_stream(self, stream_id=''):
        """
        method docstring
        """
        # Return Value
        # ------------
        # producer_list: []
        #
        pass
  