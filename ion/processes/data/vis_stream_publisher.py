#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/vis_stream_publisher.py
@description A simple example process which publishes prototype ctd data for visualization

To Run:
bin/pycc --rel res/deploy/r2dm.yml
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.vis_stream_publisher',cls='SimpleCtdPublisher')

'''
from gevent.greenlet import Greenlet
from pyon.datastore.datastore import DataStore
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from pyon.public import log

import time

import random

from prototype.sci_data.stream_defs import ctd_stream_packet, ctd_stream_definition

from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher


class VisStreamPublisher(SimpleCtdPublisher):

    def __init__(self, *args, **kwargs):
        super(SimpleCtdPublisher, self).__init__(*args,**kwargs)

        #@todo anything you need to do before on start is called...



    def on_start(self):

        # Do stuff before on start - before the process tries to start publishing...

        super(SimpleCtdPublisher, self).on_start()


        # Generally can't do stuff here - the process is already trying to publish...


    def _trigger_func(self, stream_id):
        """
        Implement your own trigger func to load you netcdf data and publish it...
        """

        ctd_packet = ctd_stream_packet(stream_id=stream_id,
            c=None, t=None, p=None, lat=None, lon=None, time=None)

        log.warn('SimpleCtdPublisher sending %d values!' % length)
        self.publisher.publish(ctd_packet)

        time.sleep(2.0)