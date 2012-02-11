#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/ctd_stream_publisher.py
@description A simple example process which publishes prototype ctd data

To Run:
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.ctd_stream_publisher',cls='SimpleCtdPublisher')

'''
from gevent.greenlet import Greenlet
from pyon.core.exception import BadRequest
from pyon.datastore.datastore import DataStore, DatastoreManager
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from pyon.public import log

import time

import random

from prototype.sci_data.ctd_stream import ctd_stream_packet, ctd_stream_definition

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

class SimpleCtdPublisher(StandaloneProcess):
    process_type="standalone"
    def __init__(self, *args, **kwargs):
        super(StandaloneProcess, self).__init__(*args,**kwargs)
        #@todo Init stuff

    def on_start(self):
        '''
        Creates a publisher for each stream_id passed in as publish_streams
        Creates an attribute with the name matching the stream name which corresponds to the publisher
        ex: say we have publish_streams:{'output': my_output_stream_id }
          then the instance has an attribute output which corresponds to the publisher for the stream
          in my_output_stream_id
        '''

        # Get the stream(s)
        stream_id = self.CFG.get('process',{}).get('stream_id','')


        # Stream creation is done in SA, but to make the example go for demonstration create one here if it is not provided...
        if not stream_id:

            pubsub_cli = PubsubManagementServiceClient(node=self.container.node)
            stream_id = pubsub_cli.create_stream(stream_definition_type='simple_ctd',name='Example CTD Data', original=True, encoding='ION R2')

        self.stream_publisher_registrar = StreamPublisherRegistrar(process=self,node=self.container.node)


        ###
        ### This next bit is none of the publishers business, but we need it for now until it can be done by pubsub/ingestion
        ###
        # Get the name of the data store where records will go so we can create the stream definition and put it in there
        datastore_name = self.CFG.get('process',{}).get('datastore_name','dm_datastore')

        db = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.EXAMPLES, self.CFG)

        ctd_def = ctd_stream_definition(stream_id=stream_id)

        db.create(ctd_def)


        self.publisher = self.stream_publisher_registrar.create_publisher(stream_id=stream_id)


        self.last_time = 0


        g = Greenlet(self._trigger_func, stream_id)
        log.debug('Starting publisher thread for simpel ctd data.')
        g.start()


    def _trigger_func(self, stream_id):

        log.error('Printing Stream ID: %s' % stream_id)


        while True:

            length = random.randint(1,20)

            c = [random.uniform(0.0,75.0)  for i in xrange(length)]

            t = [random.uniform(-1.7, 21.0) for i in xrange(length)]

            p = [random.lognormvariate(1,2) for i in xrange(length)]

            lat = [random.uniform(-90.0, 90.0) for i in xrange(length)]

            lon = [random.uniform(0.0, 360.0) for i in xrange(length)]

            tvar = [self.last_time + i for i in xrange(1,length+1)]

            self.last_time = max(tvar)

            ctd_packet = ctd_stream_packet(stream_id=stream_id,
                c=c, t=t, p=p, lat=lat, lon=lon, time=tvar)

            log.warn('SimpleCtdPublisher sending %d values!' % length)
            self.publisher.publish(ctd_packet)

            time.sleep(2.0)