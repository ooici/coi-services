#!/usr/bin/env python

"""
@package ion.agents.agent_stream_publisher
@file ion/agents/agent_stream_publisher.py
@author Edward Hunter
@brief Class for managing stream publishing details for opt-in use by agents
that stream data.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Basic Pyon imports
from pyon.public import log

# Standard imports.
import uuid

# 3rd party.
import gevent
import numpy
import base64

# Publilshing objects.
from pyon.ion.stream import StreamPublisher
from ion.services.dm.utility.granule_utils import RecordDictionaryTool
from interface.objects import StreamRoute

# Pyon Object Serialization
from pyon.core.bootstrap import get_obj_registry
from pyon.core.object import IonObjectDeserializer

from ion.agents.populate_rdt import populate_rdt

class AgentStreamPublisher(object):
    """
    """
    def __init__(self, agent):
        self._agent = agent
        self._stream_defs = {}
        self._publishers = {}
        self._stream_greenlets = {}
        self._stream_buffers = {}
        self._connection_ID = None
        self._connection_index = {}
        
        stream_info = self._agent.CFG.get('stream_config', None)
        if not stream_info:
            log.error('Instrument agent %s has no stream config.',
                      self._agent._proc_name)
            return
        
        log.info('Instrument agent %s stream config: %s', self._agent._proc_name,
                 str(stream_info))

        self._construct_streams(stream_info)
        self._construct_publishers(stream_info)
        agent.aparam_set_streams = self.aparam_set_streams
        
    def _construct_streams(self, stream_info):
        decoder = IonObjectDeserializer(obj_registry=get_obj_registry())
        for (stream_name, config) in stream_info.iteritems():
            try:
                if config.has_key('stream_def_dict'):
                    stream_def_dict = config['stream_def_dict']
                    stream_def_dict['type_'] = 'StreamDefinition'
                    stream_def_obj = decoder.deserialize(stream_def_dict)
                    self._stream_defs[stream_name] = stream_def_obj
                    rdt = RecordDictionaryTool(stream_definition=stream_def_obj)
                else:
                    stream_def = config['stream_definition_ref']
                    self._stream_defs[stream_name] = stream_def
                    rdt = RecordDictionaryTool(stream_definition_id=stream_def)    
                self._agent.aparam_streams[stream_name] = rdt.fields
                self._agent.aparam_pubrate[stream_name] = 0
            except Exception as e:
                errmsg = 'Instrument agent %s' % self._agent._proc_name
                errmsg += 'error constructing stream %s. ' % stream_name
                errmsg += str(e)
                log.error(errmsg)
                
        self._agent.aparam_set_pubrate = self.aparam_set_pubrate

    def _construct_publishers(self, stream_info):
        for (stream_name, stream_config) in stream_info.iteritems():
            try:                
                exchange_point = stream_config['exchange_point']
                routing_key = stream_config['routing_key']
                route = StreamRoute(exchange_point=exchange_point,
                                    routing_key=routing_key)
                stream_id = stream_config['stream_id']
                publisher = StreamPublisher(process=self._agent,
                                    stream_id=stream_id, stream_route=route)
                self._publishers[stream_name] = publisher
                self._stream_greenlets[stream_name] = None
                self._stream_buffers[stream_name] = []
        
            except Exception as e:
                errmsg = 'Instrument agent %s' % self._agent._proc_name
                errmsg += 'error constructing publisher for stream %s. ' % stream_name
                errmsg += str(e)
                log.error(errmsg)
    
    def reset_connection(self):
        # Reset the connection id and index.
        self._connection_ID = uuid.uuid4()
        self._connection_index = {key : 0 for key in self._agent.aparam_streams.keys()}
                 
    def on_sample(self, sample):
        
        try:
            stream_name = sample['stream_name']
            self._stream_buffers[stream_name].insert(0, sample)
            if not self._stream_greenlets[stream_name]:
                self._publish_stream_buffer(stream_name)

        except KeyError:
            log.warning('Instrument agent %s received sample with bad stream name %s.',
                      self._agent._proc_name, stream_name)

    def on_sample_mult(self, sample_list):
        """
        Enqueues a list of granules and publishes them
        """
        streams = set()
        for sample in sample_list:
            try:
                stream_name = sample['stream_name']
                self._stream_buffers[stream_name].insert(0, sample)
                streams.add(stream_name)
            except KeyError:
                log.warning('Instrument agent %s received sample with bad stream name %s.',
                          self._agent._proc_name, stream_name)

        for stream_name in streams:
            if not self._stream_greenlets[stream_name]:
                self._publish_stream_buffer(stream_name)


    def aparam_set_streams(self, params):
        return -1
    
    def aparam_set_pubrate(self, params):
        if not isinstance(params, dict):
            return -1
        
        for (stream_name, rate) in params.iteritems():
            if isinstance(stream_name, str) \
                and stream_name in self._agent.aparam_streams.keys() \
                and isinstance(rate, (int, float)) \
                and rate >= 0:
                self._agent.aparam_pubrate[stream_name] = rate
                if rate > 0:
                    if not self._stream_greenlets[stream_name]:
                        gl = gevent.spawn(self._pub_loop, stream_name)
                        self._stream_greenlets[stream_name] = gl
                elif self._stream_greenlets[stream_name]:
                    self._stream_greenlets[stream_name].kill()
                    self._stream_greenlets[stream_name] = None
                    self._publish_stream_buffer(stream_name)
    
    def _pub_loop(self, stream_name):
        """
        """
        while True:
            pubrate = self._agent.aparam_pubrate[stream_name]
            gevent.sleep(pubrate)
            self._publish_stream_buffer(stream_name)
    
    def _publish_stream_buffer(self, stream_name):
        """
        ['quality_flag', 'preferred_timestamp', 'port_timestamp', 'lon', 'raw', 'internal_timestamp', 'time', 'lat', 'driver_timestamp']
        ['quality_flag', 'preferred_timestamp', 'temp', 'density', 'port_timestamp', 'lon', 'salinity', 'pressure', 'internal_timestamp', 'time', 'lat', 'driver_timestamp', 'conductivit
        
        {"driver_timestamp": 3564867147.743795, "pkt_format_id": "JSON_Data", "pkt_version": 1, "preferred_timestamp": "driver_timestamp", "quality_flag": "ok", "stream_name": "raw",
        "values": [{"binary": true, "value": "MzIuMzkxOSw5MS4wOTUxMiwgNzg0Ljg1MywgICA2LjE5OTQsIDE1MDUuMTc5LCAxOSBEZWMgMjAxMiwgMDA6NTI6Mjc=", "value_id": "raw"}]}', 'time': 1355878347.744123}
        
        {"driver_timestamp": 3564867147.743795, "pkt_format_id": "JSON_Data", "pkt_version": 1, "preferred_timestamp": "driver_timestamp", "quality_flag": "ok", "stream_name": "parsed",
        "values": [{"value": 32.3919, "value_id": "temp"}, {"value": 91.09512, "value_id": "conductivity"}, {"value": 784.853, "value_id": "pressure"}]}', 'time': 1355878347.744127}
        
        {'quality_flag': [u'ok'], 'preferred_timestamp': [u'driver_timestamp'], 'port_timestamp': [None], 'lon': [None], 'raw': ['-4.9733,16.02390, 539.527,   34.2719, 1506.862, 19 Dec 2012, 01:03:07'],
        'internal_timestamp': [None], 'time': [3564867788.0627117], 'lat': [None], 'driver_timestamp': [3564867788.0627117]}
        
        {'quality_flag': [u'ok'], 'preferred_timestamp': [u'driver_timestamp'], 'temp': [-4.9733], 'density': [None], 'port_timestamp': [None], 'lon': [None], 'salinity': [None], 'pressure': [539.527],
        'internal_timestamp': [None], 'time': [3564867788.0627117], 'lat': [None], 'driver_timestamp': [3564867788.0627117], 'conductivity': [16.0239]}
        """

        try:
            buf_len = len(self._stream_buffers[stream_name])
            if buf_len == 0:
                return

            stream_def = self._stream_defs[stream_name]
            if isinstance(stream_def, str):
                rdt = RecordDictionaryTool(stream_definition_id=stream_def)
            else:
                rdt = RecordDictionaryTool(stream_definition=stream_def)
                
            publisher = self._publishers[stream_name]
                
            vals = []
            for x in xrange(buf_len):
                vals.append(self._stream_buffers[stream_name].pop())
    
            rdt = populate_rdt(rdt, vals)
            
            log.info('Outgoing granule: %s',
                     ['%s: %s'%(k,v) for k,v in rdt.iteritems()])
            log.info('Outgoing granule preferred timestamp: %s' % rdt['preferred_timestamp'][0])
            log.info('Outgoing granule destined for stream: %s', stream_name)
            g = rdt.to_granule(data_producer_id=self._agent.resource_id, connection_id=self._connection_ID.hex,
                    connection_index=str(self._connection_index[stream_name]))
            
            publisher.publish(g)
            log.info('Instrument agent %s published data granule on stream %s.',
                self._agent._proc_name, stream_name)
            log.info('Connection id: %s, connection index: %i.',
                     self._connection_ID.hex, self._connection_index[stream_name])
            self._connection_index[stream_name] += 1
        except:
            log.exception('Instrument agent %s could not publish data on stream %s.',
                self._agent._proc_name, stream_name)

            
