#!/usr/bin/env python

__author__ = 'Maurice Manning'
__license__ = 'Apache 2.0'


from interface.services.dm.ipubsub_management_service import BasePubsubManagementService

class PubsubManagementService(BasePubsubManagementService):


    def define_stream(self, stream={}):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def define_subscription(self, query={}, queue={}):
        # Return Value
        # ------------
        # {subscriptionId: ''}
        #
        pass

    def activate_subscription(self, subscriptionId=''):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def register_consumer(self, consumerid=''):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def register_publisher(self, processid='', streamid=''):
        # Return Value
        # ------------
        # {status: true}
        #
        pass

    def find_streams(self, filter={}):
        # Return Value
        # ------------
        # result: []
        #
        pass

    def find_stream_producers(self, streamid=''):
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

    def find_producers_stream(self, producerid=''):
        # Return Value
        # ------------
        # streams: []
        #
        pass

    def find_stream_consumers(self, streamid=''):
        # Return Value
        # ------------
        # consumers: []
        #
        pass

    def find_consumers_stream(self, consumerid=''):
        # Return Value
        # ------------
        # streams: []
        #
        pass
  