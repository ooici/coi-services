#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/ctd_stream_publisher.py
@description A simple example process which publishes prototype ctd data


$ bin/pycc --rel res/deploy/r2sa.yml

### To Create a data product and get some data on the stream copy this and use %paste
"""
from interface.services.sa.idata_product_management_service import  DataProductManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition
from interface.objects import DataProduct
from pyon.public import PRED, log

definition = SBE37_CDM_stream_definition()

dpm_cli = DataProductManagementServiceClient(node=cc.node)
pubsub_cli =  PubsubManagementServiceClient(node=cc.node)
rr_cli = ResourceRegistryServiceClient(node=cc.node)

stream_def_id = pubsub_cli.create_stream_definition(container=definition)

dp = DataProduct(name='dp1')

data_product_id = dpm_cli.create_data_product(data_product=dp, stream_definition_id=stream_def_id)
stream_ids, garbage = rr_cli.find_objects(data_product_id, PRED.hasStream, id_only=True)
stream_id = stream_ids[0]

pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.ctd_stream_publisher',cls='SimpleCtdPublisher',config={'process':{'stream_id':stream_id}})

"""

Get the data product id from the variable and use it to start a separate container running the dispatcher


To run the dispatcher:
bin/pycc --rel res/deploy/examples/dispatcher.yml dispatcher.data_product_id=<data product id>


'''


from gevent.greenlet import Greenlet
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient
from pyon.public import log, StreamSubscriberRegistrar, PRED
from pyon.util.containers import get_datetime
from interface.objects import StreamQuery
from prototype.sci_data.stream_parser import PointSupplementStreamParser

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
import os


class SimpleDispatcher(StandaloneProcess):


    def on_start(self):

        rr_cli = ResourceRegistryServiceProcessClient(process=self, node=self.container.node)
        pubsub_cli = PubsubManagementServiceProcessClient(process=self, node=self.container.node)

        # Get the stream(s)
        data_product_id = self.CFG.get_safe('dispatcher.data_product_id','')

        stream_ids,_ = rr_cli.find_objects(subject=data_product_id, predicate=PRED.hasStream, id_only=True)

        log.warn('Got Stream Ids: "%s"', stream_ids)
        assert stream_ids, 'No streams found for this data product!'

        query = StreamQuery(stream_ids=stream_ids)

        exchange_name = 'dispatcher_%s' % str(os.getpid())

        subscription_id = pubsub_cli.create_subscription(
            query = query,
            exchange_name = exchange_name,
            name = "SampleSubscription",
            description = "Sample Subscription Description")


        stream_subscriber = StreamSubscriberRegistrar(process=self, node=self.container.node)

        
        stream_defs = {}

        def message_received(granule, h):

            stream_id = granule.stream_resource_id

            data_stream_id = granule.data_stream_id
            data_stream = granule.identifiables[data_stream_id]

            tstamp = get_datetime(data_stream.timestamp.value)

            records = granule.identifiables['record_count'].value
            

            log.warn('Received a message from stream %s with time stamp %s and %d records' % (stream_id, tstamp, records))


            if stream_id not in stream_defs:
                stream_defs[stream_id] = pubsub_cli.find_stream_definition(stream_id, id_only=False).container
            stream_def = stream_defs.get(stream_id)

            sp = PointSupplementStreamParser(stream_definition=stream_def, stream_granule=granule)

            last_data = {}
            for field in sp.list_field_names():
                last_data[field] = sp.get_values(field)[-1]

            log.warn('Last values in the message: %s' % str(last_data))



        subscriber = stream_subscriber.create_subscriber(exchange_name=exchange_name, callback=message_received)
        subscriber.start()

        pubsub_cli.activate_subscription(subscription_id)


