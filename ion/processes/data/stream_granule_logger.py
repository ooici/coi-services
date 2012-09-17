#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/stream_granule_logger.py
@description A simple example process which publishes prototype ctd data
'''


from pyon.ion.transforma import TransformDataProcess
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.util.log import log
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient


class StreamGranuleLogger(TransformDataProcess):

    def on_start(self):
        super(StreamGranuleLogger,self).on_start()

        pubsub_cli = PubsubManagementServiceProcessClient(process=self, node=self.container.node)

        # Get the stream(s)
        stream_id = self.CFG.get_safe('process.stream_id','')
        if stream_id:
            self.subscription_id = pubsub_cli.create_subscription('%s_sub' % self.id, stream_ids=[stream_id], exchange_name=self.queue_name)
            pubsub_cli.activate_subscription(self.subscription_id)

    def recv_packet(self, granule, stream_route, stream_id):

        rdt = RecordDictionaryTool.load_from_granule(granule)
        log.warn('Logging Record Dictionary received in logger subscription  \n%s', rdt.pretty_print())

    def on_quit(self):
        pubsub_cli = PubsubManagementServiceProcessClient(process=self, node=self.container.node)
        pubsub_cli.deactivate_subscription(self.subscription_id)
        pubsub_cli.delete_subscription(self.subscription_id)
        super(StreamGranuleLogger, self).on_quit()
