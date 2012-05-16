#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/ctd_stream_publisher.py
@description A simple example process which publishes prototype ctd data

To Run:
bin/pycc --rel res/deploy/r2dm.yml
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.ctd_stream_publisher',cls='SimpleCtdPublisher')

'''
from gevent.greenlet import Greenlet
from pyon.ion.stream import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from pyon.public import log
from ion.agents.eoi.data_acquisition_management_service_Placeholder import *

import time

import random

from prototype.sci_data.stream_defs import USGS_stream_definition
from prototype.sci_data.constructor_apis import PointSupplementConstructor

from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

class UsgsPublisher(StandaloneProcess):

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

        self.greenlet_queue = []

        self._usgs_def = USGS_stream_definition()

        # Stream creation is done in SA, but to make the example go for demonstration create one here if it is not provided...
        if not stream_id:

            pubsub_cli = PubsubManagementServiceClient(node=self.container.node)
            stream_id = pubsub_cli.create_stream(
                name='Example USGS Data',
                stream_definition=self._usgs_def,
                original=True,
                encoding='ION R2')

        self.stream_publisher_registrar = StreamPublisherRegistrar(process=self,node=self.container.node)
        # Needed to get the originator's stream_id
        self.stream_id = stream_id


        self.publisher = self.stream_publisher_registrar.create_publisher(stream_id=stream_id)


        self.last_time = 0


        g = Greenlet(self._trigger_func, stream_id)
        log.warn('Starting publisher thread for simple usgs data.')
        g.start()
        self.greenlet_queue.append(g)

    def on_quit(self):
        for greenlet in self.greenlet_queue:
            greenlet.kill()
        super(UsgsPublisher,self).on_quit()

    #overriding trigger function here to use PointSupplementConstructor
    def _trigger_func(self, stream_id):

        #point_def = usgs_stream_definition(stream_id=stream_id)
        point_constructor = PointSupplementConstructor(point_definition=self._usgs_def, stream_id=stream_id)

        damsP = DataAcquisitionManagementServicePlaceholder()
        dsh = damsP.get_data_handler(ds_id=USGS)
        dsh._block_size = 1

        lon = 0
        lon_iter = dsh.acquire_data(var_name='lon')
        for lon_vn, lon_slice_, lon_rng, lon_data in lon_iter:
            lon = lon_data[0]
        lat = 0
        lat_iter = dsh.acquire_data(var_name='lat')
        for lat_vn, lat_slice_, lat_rng, lat_data in lat_iter:
            lat = lat_data[0]

        location = (lon,lat)

        data_iter = dsh.acquire_data(var_name='time')

        index = 0
        for vn, slice_, rng, data in data_iter: #loop through each time step
            point_id = point_constructor.add_point(time=data[0], location=location)

            temp_iter = dsh.acquire_data(var_name=['water_temperature','water_height','streamflow'], slice_=index)
            for temp_vn, temp_slice_, temp_rng, temp_data in temp_iter:
                point_constructor.add_scalar_point_coverage(point_id=point_id, coverage_id=temp_vn, value=temp_data[0])

            ctd_packet = point_constructor.close_stream_granule()

            self.publisher.publish(ctd_packet)

            index += 1

#        time.sleep(2.0)
