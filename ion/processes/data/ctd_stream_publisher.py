#!/usr/bin/env python

'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Fri Sep  7 12:05:34 EDT 2012
@file ion/processes/data/ctd_stream_publisher.py
@brief A process which produces a simple data stream
'''

from ion.core.process.transform import TransformStreamPublisher
from pyon.public import log
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceProcessClient

import random
import numpy
import gevent
import ntplib
import time

class SimpleCtdPublisher(TransformStreamPublisher):
    '''
    A very very simple CTD streaming producer
    The only parameter you need to provide is process.stream_id
    Example:
        pdict_id = pn.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', True)
        stream_def = pn.pubsub_management.create_stream_definition('transform output', parameter_dictionary_id=pdict_id)
        out_stream_id, route = pn.pubsub_management.create_stream('t_out', 'xp1', stream_definition_id=stream_def)

        from pyon.ion.stream import StandaloneStreamSubscriber

        def echo(m,r,s):
            from ion.services.dm.utility.granule import RecordDictionaryTool
            rdt = RecordDictionaryTool.load_from_granule(m)
            print rdt.pretty_print()
            
        sub = StandaloneStreamSubscriber('sub', echo)
        sub.start()
        sub_id = pn.pubsub_management.create_subscription('sub', stream_ids=[out_stream_id])
        pn.pubsub_management.activate_subscription(sub_id)
        cc.spawn_process('transform', 'ion.processes.data.ctd_stream_publisher','SimpleCtdPublisher', {'process':{'stream_id':out_stream_id}}, 'transform')
    '''
    def on_start(self):
        super(SimpleCtdPublisher,self).on_start()
        pubsub_cli = PubsubManagementServiceProcessClient(process=self)
        self.stream_id = self.CFG.get_safe('process.stream_id',{})
        self.interval  = self.CFG.get_safe('process.interval', 1.0)
        #self.last_time = self.CFG.get_safe('process.last_time', 0)

        self.stream_def = pubsub_cli.read_stream_definition(stream_id=self.stream_id)
        self.pdict      = self.stream_def.parameter_dictionary


        self.finished = gevent.event.Event()
        self.greenlet = gevent.spawn(self.publish_loop)
        self._stats['publish_count'] = 0
        log.info('SimpleCTDPublisher started, publishing to %s', self.publisher.stream_route.__dict__)

    def on_quit(self):
        if self.greenlet:
            self.finished.set()
            self.greenlet.join(10)
            self.greenlet.kill()
        super(SimpleCtdPublisher,self).on_quit() 

    def publish(self, msg, to_name=''):
        log.debug("self._stats: %s" % self._stats)
        self._stats['publish_count'] += 1
        if to_name:
            self.publisher.publish(msg,stream_id=to_name)
        else:
            log.info('Publishing on (%s,%s)', self.publisher.stream_route.exchange_point, self.publisher.stream_route.routing_key)
            self.publisher.publish(msg)

    def publish_loop(self):

        while not self.finished.is_set():

            #Length of packets should be uniform
            length = 10

            ctd_packet = self._get_new_ctd_packet(length)
            self.publish(ctd_packet)
            gevent.sleep(self.interval)

    def _get_new_ctd_packet(self, length):

        rdt = RecordDictionaryTool(stream_definition_id=self.stream_def._id)
        #Explicitly make these numpy arrays...
        c = numpy.array([random.uniform(0.0,75.0)  for i in xrange(length)]) 
        t = numpy.array([random.uniform(-1.7, 21.0) for i in xrange(length)]) 
        p = numpy.array([random.lognormvariate(1,2) for i in xrange(length)])
        lat = numpy.array([random.uniform(-90.0, 90.0) for i in xrange(length)]) 
        lon = numpy.array([random.uniform(0.0, 360.0) for i in xrange(length)]) 
        h = numpy.array([random.uniform(0.0, 360.0) for i in xrange(length)])

        start_time = ntplib.system_to_ntp_time(time.time()) - (length + 1)
        tvar = numpy.array([start_time + i for i in xrange(1,length+1)])


        rdt['time'] = tvar
        rdt['lat'] = lat
        rdt['lon'] = lon
        rdt['temp'] = t
        rdt['conductivity'] = c
        rdt['pressure'] = p

#        rdt['coordinates'] = rdt0
#        rdt['data'] = rdt1

        g = rdt.to_granule(data_producer_id=self.id)

        return g
