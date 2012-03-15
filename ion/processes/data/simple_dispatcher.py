#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/ctd_stream_publisher.py
@description A simple example process which publishes prototype ctd data

To Run:
bin/pycc --rel res/deploy/r2dm.yml dispatcher.data_product_id='abc123'
cc.spawn_process(name="dispatcher_process", module="ion.processes.data.simple_dispatcher", cls="SimpleDispatcher")
'''


from gevent.greenlet import Greenlet
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from pyon.public import log, StreamSubscriberRegistrar, PRED

from interface.objects import StreamQuery

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
import os


class SimpleDispatcher(StandaloneProcess):


    def on_start(self):

        rr_cli = ResourceRegistryServiceProcessClient(process=self, node=self.container.node)
        pubsub_cli = PubsubManagementServiceProcessClient(process=self, node=self.container.node)

        # Get the stream(s)
        data_product_id = self.CFG.get_safe('dispatcher.data_product_id','')

        log.warn('CFG: %s' % str(self.CFG))

        stream_ids,_ = rr_cli.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)

        assert stream_ids, 'No streams found for this data product!'

        query = StreamQuery(stream_ids)

        exchange_name = 'dispatcher_%s' % str(os.getpid())

        self.ctd_subscription_id = pubsub_cli.create_subscription(
            query = query,
            exchange_name = exchange_name,
            name = "SampleSubscription",
            description = "Sample Subscription Description")


        stream_subscriber = StreamSubscriberRegistrar(process=self, node=self.container.node)

        def message_received(m, h):
            log.info('Received a message from the stream')


        subscriber = stream_subscriber.create_subscriber(exchange_name=exchange_name, callback=message_received)
        subscriber.start()