#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell at ASAScience dot com>
@date Tue Feb 12 09:54:27 EST 2013
@file ion/processes/data/transforms/transform_prime.py
'''

from ion.core.process.transform import TransformDataProcess
from coverage_model import ParameterDictionary
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from coverage_model import get_value_class
from coverage_model.parameter_types import ParameterFunctionType
from pyon.util.memoize import memoize_lru

class TransformPrime(TransformDataProcess):
    binding=['output']
    '''
    Transforms which have an incoming stream and an outgoing stream.

    Parameters:
      process.stream_id      Outgoing stream identifier.
      process.exchange_point Route's exchange point.
      process.routing_key    Route's routing key.
      process.queue_name     Name of the queue to listen on.
      process.routes         streams,actor for each route {(stream_input_id, stream_output_id):actor} 
    Either the stream_id or both the exchange_point and routing_key need to be provided.
    '''    
    def on_start(self):
        TransformDataProcess.on_start(self)
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)

    @memoize_lru(100)
    def read_stream_def(self,stream_id):
        return self.pubsub_management.read_stream_definition(stream_id=stream_id)

    
    def recv_packet(self, msg, stream_route, stream_id):
        process_routes = self.CFG.get_safe('process.routes', {})
        for streams,actor in process_routes.iteritems():
            stream_in_id, stream_out_id = streams
            if stream_id == stream_in_id:
                if actor is None:
                    rdt_out = self._execute_transform(msg, streams)
                else:
                    rdt_out = self._execute_actor(msg, actor, streams)
                self.publish(rdt_out.to_granule(), stream_out_id)

    def publish(self, msg, stream_out_id):
        publisher = getattr(self, stream_out_id)
        publisher.publish(msg)
   
    def _execute_actor(self, msg, actor, streams):
        stream_in_id,stream_out_id = streams
        stream_def_out = self._read_stream_def(stream_out_id)
        #do the stuff with the actor
        rdt_out = RecordDictionaryTool(stream_definition_id=stream_def_out._id)
        return rdt_out

    def _execute_transform(self, msg, streams):
        stream_in_id,stream_out_id = streams
        stream_def_in = self.read_stream_def(stream_in_id)
        stream_def_out = self.read_stream_def(stream_out_id)
        
        incoming_pdict_dump = stream_def_in.parameter_dictionary
        outgoing_pdict_dump = stream_def_out.parameter_dictionary
        
        incoming_pdict = ParameterDictionary.load(incoming_pdict_dump)
        outgoing_pdict = ParameterDictionary.load(outgoing_pdict_dump)
        
        merged_pdict = ParameterDictionary()
        for k,v in incoming_pdict.iteritems():
            ordinal, v = v
            if k not in merged_pdict:
                merged_pdict.add_context(v)
        for k,v in outgoing_pdict.iteritems():
            ordinal, v = v
            if k not in merged_pdict:
                merged_pdict.add_context(v)
        rdt_temp = RecordDictionaryTool(param_dictionary=merged_pdict)
        rdt_in = RecordDictionaryTool.load_from_granule(msg)
        for field in rdt_temp.fields:
            if not isinstance(rdt_temp._pdict.get_context(field).param_type, ParameterFunctionType):
                try:
                    rdt_temp[field] = rdt_in[field]
                except KeyError:
                    pass
        
        for field in rdt_temp.fields:
            if isinstance(rdt_temp._pdict.get_context(field).param_type, ParameterFunctionType):
                rdt_temp[field] = rdt_temp[field]

        
        rdt_out = RecordDictionaryTool(stream_definition_id=stream_def_out._id)

        for field in rdt_out.fields:
            rdt_out[field] = rdt_temp[field]
        
        return rdt_out 


