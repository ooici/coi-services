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
    
    def _get_stream_defs(self, stream_ids):
        stream_in_id, stream_out_id = stream_ids
        stream_def_in = self.pubsub_management.read_stream_definition(stream_id=stream_in_id)
        stream_def_out = self.pubsub_management.read_stream_definition(stream_id=stream_out_id)
        return (stream_def_in, stream_def_out)

    def recv_packet(self, msg, stream_route, stream_id):
        process_routes = self.CFG.get_safe('process.routes', {})
        for streams,actor in process_routes.iteritems():
            stream_in_id, stream_out_id = streams
            if stream_id == stream_in_id:
                if actor is None:
                    rdt_out = self.execute_transform(msg, streams)
                else:
                    pass
                    rdt_out = self.execute_actor(msg, actor, streams)
                self.publish(rdt_out.to_granule(), stream_out_id)

    def publish(self, msg, stream_out_id):
        publisher = getattr(self, stream_out_id)
        publisher.publish(msg)
   
    def execute_actor(self, msg, actor, streams):
        stream_def_in,stream_def_out = self._get_stream_defs(streams)
        #do the stuff with the actor
        rdt_out = RecordDictionaryTool(stream_definition_id=stream_def_out._id)
        return rdt_out

    def execute_transform(self, msg, streams):
        stream_def_in,stream_def_out = self._get_stream_defs(streams)
        incoming_pdict_dump = stream_def_in.parameter_dictionary
        outgoing_pdict_dump = stream_def_out.parameter_dictionary
        
        incoming_pdict = ParameterDictionary.load(incoming_pdict_dump)
        outgoing_pdict = ParameterDictionary.load(outgoing_pdict_dump)
        
        merged_pdict = dict([(k,v) for k,v in incoming_pdict.iteritems()] + [(k,v) for k,v in outgoing_pdict.iteritems()])
        rdt_in = RecordDictionaryTool.load_from_granule(msg)
        rdt_out = RecordDictionaryTool(stream_definition_id=stream_def_out._id)
        #modify the shape of the rdt out since we are using _rd then _shp will never get set
        rdt_out._shp = rdt_in._shp
        for key,pctup in merged_pdict.iteritems():
            n,pc = pctup
            #if function then a transform is applied to calculate values
            if isinstance(pc.param_type, ParameterFunctionType):
                #apply transform
                pv = get_value_class(pc.param_type, rdt_in.domain)
                #recursive function applies values
                def pval_callback(name, slice_):
                    result = None
                    #search for input data...first level input
                    if name in rdt_in._available_fields:
                        result = rdt_in[name]
                    else:
                        #not first level data so continue to evaluate
                        n,pc = merged_pdict[name]
                        pv = get_value_class(pc.param_type, rdt_in.domain)
                        pv._pval_callback = pval_callback
                        result = pv[:]
                    return result
                #set the evaluation callback so it can find values in the input stream
                pv._pval_callback = pval_callback
                #transform should be on the outgoing stream
                if key in rdt_out._available_fields:
                    rdt_out._rd[key] = pv[:]
            else:
                #field exists in both the in and the out stream so pass it along to the output stream
                if key in rdt_in._available_fields and key in rdt_out._available_fields:
                    #pass through
                    rdt_out._rd[key] = rdt_in._rd[key][:]
        return rdt_out 


