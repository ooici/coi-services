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
      
    Either the stream_id or both the exchange_point and routing_key need to be provided.
    '''    
    def on_start(self):
        TransformDataProcess.on_start(self)
        self.pubsub_management = PubsubManagementServiceProcessClient(process=self)
    
    def recv_packet(self, msg, stream_route, stream_id):
        
        #rdt = RecordDictionaryTool(stream_definition_id=stream_def_out.id)
        
        #publisher = getattr(self, self.CFG.process.stream_id)
        #publisher.publish(rdt_in.to_granule())
        #elif key in rdt._available_fields and isinstance(pdict, QuantityType):
        pass

    def execute_transform(self, msg, stream_id):
        import sys
        
        stream_def_in = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        incoming_pdict_dump = stream_def_in.parameter_dictionary
        
        stream_def_out = self.pubsub_management.read_stream_definition(stream_id=self.CFG.process.stream_id)
        outgoing_pdict_dump = stream_def_out.parameter_dictionary
        
        incoming_pdict = ParameterDictionary.load(incoming_pdict_dump)
        outgoing_pdict = ParameterDictionary.load(outgoing_pdict_dump)
        

        merged_pdict = dict([(k,v) for k,v in incoming_pdict.iteritems()] + [(k,v) for k,v in outgoing_pdict.iteritems()])
        rdt = RecordDictionaryTool.load_from_granule(msg)
        #print >> sys.stderr, "available_fields", rdt._available_fields
        #rdt_out = RecordDictionaryTool(stream_definition_id=stream_def_out._id)
        #print >> sys.stderr, "available_fields", rdt_out._available_fields
        for key,pctup in merged_pdict.iteritems():
            n,pc = pctup
            print >> sys.stderr, "param_type", pc.param_type
            if isinstance(pc.param_type, ParameterFunctionType):
                #apply transform
                pv = get_value_class(pc.param_type, rdt.domain)
                rdt._rd[key] = pv[:]
        return rdt 


