'''
@author Tim Giguere
@file ion/processes/data/transforms/example_data_receiver_a.py
@description Uses new transform classes to receive data from a stream
'''

'''
Example of how to use ExampleDataReceiver:

from pyon.util.containers import DotDict
config = DotDict()
config.process.queue_name = 'output_queue'  #Give the queue a name

#spawn the process. ExampleDataReceiver will create the queue
cc.spawn_process(name='receiver_test', module='ion.processes.data.example_data_receiver_a', cls='ExampleDataReceiver', config=config)

xn2 = cc.ex_manager.create_xn_queue('output_queue')     #create_xn_queue gets the queue with the given name or creates it if it doesn't exist
xp2 = cc.ex_manager.create_xp('output_xp')              #create_xp gets the xp with the given name or creates it
xn2.bind(stream_id + '.data', xp2)                      #bind the queue to the xp

'''

import re

from pyon.ion.transforma import TransformStreamListener
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from pyon.public import log


class ExampleDataReceiver(TransformStreamListener):

    def recv_packet(self, msg, headers):
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)
        self.receive_msg(msg, stream_id)

    def receive_msg(self, msg, stream_id):
        rdt = RecordDictionaryTool.load_from_granule(msg)
        log.info('Message Received: {0}'.format(rdt.pretty_print()))