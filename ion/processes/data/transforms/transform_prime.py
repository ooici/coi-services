#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell at ASAScience dot com>
@date Tue Feb 12 09:54:27 EST 2013
@file ion/processes/data/transforms/transform_prime.py
'''

from ion.core.process.transform import TransformDataProcess
from coverage_model import ParameterDictionary
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient


class TransformPrime(TransformDataProcess):
    binding=['output']
    '''
    Transforms which have an incoming stream and an outgoing stream.

    Parameters:
      process.stream_id      Outgoing stream identifier.
      process.exchange_point Route's exchange point.
      process.routing_key    Route's routing key.
      process.queue_name     Name of the queue to listen on.
      
    Either the stream_id or both the exchange_point and routing_key need to be provided.
    '''    
    def on_start(self):
        TransformDataProcess.on_start(self)
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)

    
    def _get_pdict(self, stream_id):
        stream_def = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        pdict = stream_def.parameter_dictionary
        return pdict

    def recv_packet(self, msg, stream_route, stream_id):
        incoming_pdict_dump = self._get_pdict(stream_id)
        outgoing_pdict_dump = self._get_pdict(self.CFG.process.stream_id)
       
        incoming_pdict = ParameterDictionary.load(incoming_pdict_dump)
        outgoing_pdict = ParameterDictionary.load(outgoing_pdict_dump)

        print "Incoming: "
        for key in incoming_pdict.keys():
            print '\t', key

        print "Outgoing: "
        for key in outgoing_pdict.keys():
            print '\t', key

