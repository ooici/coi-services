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
from pyon.util.log import log
from pyon.core.exception import NotFound
from pyon.ion.event import EventSubscriber
from ion.util.stored_values import StoredValueManager
from pyon.public import OT

from gevent.event import Event
from gevent.queue import Queue
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
        self.stored_values = StoredValueManager(self.container)
        self.input_data_product_ids = self.CFG.get_safe('process.input_products', [])
        self.output_data_product_ids = self.CFG.get_safe('process.output_products', [])
        self.lookup_docs = self.CFG.get_safe('process.lookup_docs',[])
        self.new_lookups = Queue()
        self.lookup_monitor = EventSubscriber(event_type=OT.ExternalReferencesUpdatedEvent,callback=self._add_lookups, auto_delete=True)
        self.lookup_monitor.start()

    def on_quit(self):
        self.lookup_monitor.stop()
        TransformDataProcess.on_quit(self)

    def _add_lookups(self, event, *args, **kwargs):
        if event.origin in self.input_data_product_ids + self.output_data_product_ids:
            if isinstance(event.reference_keys, list):
                self.new_lookups.put(event.reference_keys)


    @memoize_lru(100)
    def read_stream_def(self,stream_id):
        return self.pubsub_management.read_stream_definition(stream_id=stream_id)

    
    def recv_packet(self, msg, stream_route, stream_id):
        process_routes = self.CFG.get_safe('process.routes', {})
        for stream_in_id,routes in process_routes.iteritems():
            if stream_id == stream_in_id:
                for stream_out_id, actor in routes.iteritems():
                    if actor is None:
                        rdt_out = self._execute_transform(msg, (stream_in_id, stream_out_id))
                        self.publish(rdt_out.to_granule(), stream_out_id)
                    else:
                        outgoing = self._execute_actor(msg, actor, (stream_in_id, stream_out_id))
                        self.publish(outgoing, stream_out_id)

    def publish(self, msg, stream_out_id):
        publisher = getattr(self, stream_out_id)
        publisher.publish(msg)

    def _load_actor(self, actor):
        '''
        Returns callable execute method if it exists, otherwise it raises a BadRequest
        '''
        try:
            module = __import__(actor['module'], fromlist=[''])
        except ImportError:
            log.exception('Actor could not be loaded')
            raise
        try:
            cls = getattr(module, actor['class'])
        except AttributeError:
            log.exception('Module %s does not have class %s', repr(module), actor['class'])
            raise
        try:
            execute = getattr(cls,'execute')
        except AttributeError:
            log.exception('Actor class does not contain execute method')
            raise
        return execute

   
    def _execute_actor(self, msg, actor, streams):
        stream_in_id,stream_out_id = streams
        stream_def_out = self.read_stream_def(stream_out_id)
        params = self.CFG.get_safe('process.params', {})
        config = self.CFG.get_safe('process')
        #do the stuff with the actor
        params['stream_def'] = stream_def_out._id
        executor = self._load_actor(actor)
        try:
            rdt_out = executor(msg, None, config, params, None)
        except:
            log.exception('Error running actor for %s', self.id)
            raise
        return rdt_out

    def _merge_pdicts(self, pdict1, pdict2):
        incoming_pdict = ParameterDictionary.load(pdict1)
        outgoing_pdict = ParameterDictionary.load(pdict2)
        
        merged_pdict = ParameterDictionary()
        for k,v in incoming_pdict.iteritems():
            ordinal, v = v
            if k not in merged_pdict:
                merged_pdict.add_context(v)
        for k,v in outgoing_pdict.iteritems():
            ordinal, v = v
            if k not in merged_pdict:
                merged_pdict.add_context(v)
        return merged_pdict

    def _merge_rdt(self, stream_def_in, stream_def_out):
        incoming_pdict_dump = stream_def_in.parameter_dictionary
        outgoing_pdict_dump = stream_def_out.parameter_dictionary

        merged_pdict = self._merge_pdicts(incoming_pdict_dump, outgoing_pdict_dump)
        rdt_temp = RecordDictionaryTool(param_dictionary=merged_pdict)
        return rdt_temp


    def _get_lookup_value(self, lookup_value):
        if not self.new_lookups.empty():
            new_values = self.new_lookups.get()
            self.lookup_docs = new_values + self.lookup_docs

        lookup_value_document_keys = self.lookup_docs
        for key in lookup_value_document_keys:
            try:
                document = self.stored_values.read_value(key)
                if lookup_value in document:
                    return document[lookup_value]
            except NotFound:
                log.warning('Specified lookup document does not exist')

        return None

    def _execute_transform(self, msg, streams):
        stream_in_id,stream_out_id = streams
        stream_def_in = self.read_stream_def(stream_in_id)
        stream_def_out = self.read_stream_def(stream_out_id)

        rdt_temp = self._merge_rdt(stream_def_in, stream_def_out)
        
        rdt_in = RecordDictionaryTool.load_from_granule(msg)
        for field in rdt_temp.fields:
            if not isinstance(rdt_temp._pdict.get_context(field).param_type, ParameterFunctionType):
                try:
                    rdt_temp[field] = rdt_in[field]
                except KeyError:
                    pass

        rdt_temp.fetch_lookup_values()

        for lookup_field in rdt_temp.lookup_values():
            s = lookup_field
            stored_value = self._get_lookup_value(rdt_temp.context(s).lookup_value)
            if stored_value is not None:
                rdt_temp[s] = stored_value
        
        for field in rdt_temp.fields:
            if isinstance(rdt_temp._pdict.get_context(field).param_type, ParameterFunctionType):
                rdt_temp[field] = rdt_temp[field]

        
        rdt_out = RecordDictionaryTool(stream_definition_id=stream_def_out._id)

        for field in rdt_out.fields:
            rdt_out[field] = rdt_temp[field]
        
        return rdt_out 


