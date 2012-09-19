#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/ctd_stream_publisher.py
@description A simple example process which publishes prototype ctd data

'''


from pyon.ion.process import StandaloneProcess
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from pyon.public import log, PRED
from pyon.ion.stream import StreamSubscriber

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from interface.objects import Granule
import os


class SimpleDispatcher(StandaloneProcess):


    def on_start(self):

        rr_cli = ResourceRegistryServiceProcessClient(process=self, node=self.container.node)
        pubsub_cli = PubsubManagementServiceProcessClient(process=self, node=self.container.node)

        # Get the stream(s)
        data_product_id = self.CFG.get_safe('dispatcher.data_product_id','')

        stream_ids,_ = rr_cli.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)

        log.info('Got Stream Ids: "%s"', stream_ids)
        assert stream_ids, 'No streams found for this data product!'


        exchange_name = 'dispatcher_%s' % str(os.getpid())
        subscription_id = pubsub_cli.create_subscription(
                name='SampleSubscription', 
                exchange_name=exchange_name,
                stream_ids=stream_ids,
                description='Sample Subscription Description'
                )



        def message_received(message, stream_route, stream_id):
            if isinstance(message,Granule):
                log.info(message.__dict__)



        subscriber = StreamSubscriber(process=self, exchange_name=exchange_name, callback=message_received)
        subscriber.start()

        pubsub_cli.activate_subscription(subscription_id)


