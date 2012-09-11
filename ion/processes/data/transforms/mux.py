#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Wed Aug 15 10:13:02 EDT 2012
@file ion/processes/data/transforms/mux.py
@brief Multiplexing and Demultiplexing Transforms
'''
from pyon.ion.transforma import TransformDataProcess, TransformStreamListener
from pyon.ion.stream import StreamPublisher
from pyon.util.log import log

class DemuxTransform(TransformDataProcess):
    def on_start(self): #pragma no cover
        TransformStreamListener.on_start(self)
        self.output_streams = self.CFG.get_safe('process.output_streams')

        if self.output_streams is None or not isinstance(self.output_streams, list):
            log.error('(%s) Failed to demux, I/O configuration is incorrect: (%s)', self.id, self.output_streams)
            return

        self.publishers = [StreamPublisher(process=self, stream_id=stream) for stream in self.output_streams]


    def on_quit(self): #pragma no cover
        TransformStreamListener.on_quit(self)
        for publisher in self.publishers:
            publisher.close()

    def recv_packet(self, msg, headers):
        self.publish(msg,None)

    def publish(self, msg, to_name):
        for publisher in self.publishers:
            publisher.publish(msg)



