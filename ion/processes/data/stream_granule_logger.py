#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/stream_granule_logger.py
@description A simple example process which publishes prototype ctd data


$ bin/pycc --rel res/deploy/r2dm.yml

### To Create a data stream and get some data on the stream copy this and use %paste
"""
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
pmsc = PubsubManagementServiceClient(node=cc.node)
stream_id = pmsc.create_stream(name='pfoo')
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.example_data_producer',cls='ExampleDataProducer',config={'process':{'stream_id':stream_id}})

pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.stream_granule_logger',cls='StreamGranuleLogger',config={'process':{'stream_id':stream_id}})

'''


from gevent.greenlet import Greenlet
from pyon.ion.stream import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from pyon.public import log, StreamSubscriberRegistrar, PRED
from pyon.util.containers import get_datetime
from interface.objects import StreamQuery
from pyon.ion.granule.record_dictionary import RecordDictionaryTool

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient


class StreamGranuleLogger(StandaloneProcess):

    def on_start(self):

        pubsub_cli = PubsubManagementServiceProcessClient(process=self, node=self.container.node)

        # Get the stream(s)
        stream_id = self.CFG.get_safe('process.stream_id','')


        query = StreamQuery(stream_ids=[stream_id,])

        exchange_name = 'dispatcher_%s' % self.id

        subscription_id = pubsub_cli.create_subscription(
            query = query,
            exchange_name = exchange_name,
            name = "SampleSubscription",
            description = "Sample Subscription Description")


        stream_subscriber = StreamSubscriberRegistrar(process=self, container=self.container)

        def message_received(granule, h):

            rdt = RecordDictionaryTool.load_from_granule(granule)

            log.warn('Logging Record Dictionary received in logger subscription  \n%s', rdt.pretty_print())

        subscriber = stream_subscriber.create_subscriber(exchange_name=exchange_name, callback=message_received)
        self._process.add_endpoint(subscriber)

        pubsub_cli.activate_subscription(subscription_id)
