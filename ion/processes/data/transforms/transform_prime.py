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
from coverage_model import QuantityType, get_value_class
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
        stream_def_in = self.pubsub_management.read_stream_definition(stream_id=stream_id)
        incoming_pdict_dump = stream_def_in.parameter_dictionary
        
        stream_def_out = self.pubsub_management.read_stream_definition(stream_id=self.CFG.process.stream_id)
        outgoing_pdict_dump = stream_def_out.parameter_dictionary
        
        incoming_pdict = ParameterDictionary.load(incoming_pdict_dump)
        outgoing_pdict = ParameterDictionary.load(outgoing_pdict_dump)

        print "Incoming: "
        for key in incoming_pdict.keys():
            print '\t', key

        print "Outgoing: "
        for key in outgoing_pdict.keys():
            print '\t', key
        
        #merge incoming and outgoing parameter dictionaries
        #loop through the record dictionary output stream def
        #get the parameter context for foreach pdict associated with the pdict in the available fields
        #get the domain for the value class
        #get the Parameter value by using the parameter context

        merged_pdict = dict(incoming_pdict.items(), outgoing_pdict.items())
        for key in merged_pdict.keys():
            print '\t', key
        #rdt = RecordDictionaryTool(stream_definition_id=stream_def_out.id)
        rdt_in = RecordDictionaryTool.load_from_granule(msg)
        for key,pdict in merged_pdict.iteritems():
            if isinstance(pdict, ParameterFunctionType):
                #apply transform
                pcontext = pdict.get_context(key)
                pv = get_value_class(pcontext, rdt_in.domain)
                rdt_in._rd[key] = pv[:]
            
            
            #elif key in rdt._available_fields and isinstance(pdict, QuantityType):
                
    #def execute_transform(self, granule):
        
