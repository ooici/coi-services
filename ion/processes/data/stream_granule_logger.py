#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file
@description A simple example process which publishes prototype ctd data


$ bin/pycc --rel res/deploy/r2dm.yml

### To Create a data product and get some data on the stream copy this and use %paste
"""
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
pmsc = PubsubManagementServiceClient(node=cc.node)
stream_id = pmsc.create_stream(name='pfoo')
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.example_ctd_data_producer',cls='ExampleCTDDataProducer',config={'process':{'stream_id':stream_id}})

pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.stream_granule_logger',cls='StreamGranuleLogger',config={'process':{'stream_id':stream_id}})

'''


from gevent.greenlet import Greenlet
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from pyon.public import log, StreamSubscriberRegistrar, PRED
from pyon.util.containers import get_datetime
from interface.objects import StreamQuery
from pyon.ion.granule.record_dictionary import RecordDictionaryTool

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
import os


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


        stream_subscriber = StreamSubscriberRegistrar(process=self, node=self.container.node)

        def message_received(granule, h):

            rdt = RecordDictionaryTool.load_from_granule(granule)

            last_data = ''

            #@todo Use RecordDictionaryTool.pretty_print() once it is complete
#            for k,v in rdt.iteritems():
#                if isinstance(v, RecordDictionaryTool):
#
#                    last_data += 'RDT: "%s" contains:\n' % k.pop()
#
#                    for k2, v2 in v.iteritems():
#                        last_data += '    item: "%s" values: %s\n' % (k2.pop(), v2)
#                else:
#                    last_data += 'item: "%s" values: %s\n' % (k.pop(), v)

            log.warn('\nLast values in the message:\n%s' % rdt.pretty_print())

        subscriber = stream_subscriber.create_subscriber(exchange_name=exchange_name, callback=message_received)
        subscriber.start()

        pubsub_cli.activate_subscription(subscription_id)