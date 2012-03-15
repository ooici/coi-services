#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/raw_stream_publisher.py
@description A simple example process which publishes prototype raw data

To Run:
bin/pycc --rel res/deploy/r2dm.yml
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.raw_stream_publisher',cls='RawCtdPublisher')

'''
from pyon.public import log
from prototype.sci_data.stream_defs import SBE37_RAW_stream_definition
from prototype.sci_data.constructor_apis import RawSupplementConstructor

from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
import gevent

class RawStreamPublisher(SimpleCtdPublisher):

    outgoing_stream_def = SBE37_RAW_stream_definition()

    def __init__(self, *args, **kwargs):
        super(RawStreamPublisher, self).__init__(*args,**kwargs)


    def on_start(self):

        # Do stuff before on start - before the process tries to start publishing...

        super(RawStreamPublisher, self).on_start()
        self.iterations = self.CFG.get_safe('process.iteration',20)


        # Generally can't do stuff here - the process is already trying to publish...


    def _trigger_func(self, stream_id):
        """
        Implement your own trigger func to load you netcdf data and publish it...
        """
        with open('/dev/urandom','r') as f:

            i=0
            infinite = False
            if self.iterations < 0:
                self.iterations = 1
                infinite = True
            while i < self.iterations:

                raw_constructor = RawSupplementConstructor(raw_definition= self.outgoing_stream_def, stream_id=stream_id)

                raw_constructor.set_samples(raw_samples=f.read(1000), num_of_samples=10)

                log.info('Publishing raw message!')
                self.publisher.publish(raw_constructor.close_stream_granule())

                gevent.sleep(1.0)
                if not infinite:
                    i+=1