#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Wed Aug 15 10:13:02 EDT 2012
@file ion/processes/data/transforms/mux.py
@brief Multiplexing and Demultiplexing Transforms
'''
from pyon.ion.transforma import TransformDataProcess, TransformStreamListener
from pyon.ion.stream import SimpleStreamPublisher
from pyon.util.log import log

class DemuxTransform(TransformDataProcess):
    def on_start(self): #pragma no cover
        TransformStreamListener.on_start(self)
        self.output_exchanges = self.CFG.get_safe('process.output_exchange_points')
        self.output_streams = self.CFG.get_safe('process.output_streams')

        if self.output_streams is None or not isinstance(self.output_streams, list):
            log.error('(%s) Failed to demux, I/O configuration is incorrect: (%s)', self.id, self.output_streams)
            return
        if len(self.output_exchanges) == 1:
            self.output_exchanges = [self.output_exchanges[0] for i in self.output_streams]
        elif len(self.output_exchanges) != len(self.output_streams):
            log.error('(%s) Failed to demux, exchange points and streams do not align: %s', self.output_exchanges, self.output_streams)
            return
        if self.output_exchanges is None:
            self.output_exchanges = ['science_data' for i in self.output_streams]

        self.publishers = [SimpleStreamPublisher.new_publisher(self.container,exchange, stream) for stream,exchange in zip(self.output_streams, self.output_exchanges)]


    def on_quit(self): #pragma no cover
        TransformStreamListener.on_quit(self)
        for publisher in self.publishers:
            publisher.close()

    def recv_packet(self, msg, headers):
        self.publish(msg,None)

    def publish(self, msg, to_name):
        for publisher in self.publishers:
            publisher.publish(msg)



