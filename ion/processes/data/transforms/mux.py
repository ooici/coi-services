#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Wed Aug 15 10:13:02 EDT 2012
@file ion/processes/data/transforms/mux.py
@brief Multiplexing and Demultiplexing Transforms
'''
from pyon.ion.transforma import TransformStreamListener
from pyon.ion.stream import StreamPublisher
from pyon.util.log import log

class DemuxTransform(TransformStreamListener):
    def on_start(self): #pragma no cover
        log.info('Starting Demuxer')
        TransformStreamListener.on_start(self)
        log.info('----------')
        self.output_streams = self.CFG.get_safe('process.out_streams')

        if self.output_streams is None or not isinstance(self.output_streams, list):
            log.error('(%s) Failed to demux, I/O configuration is incorrect: (%s)', self.id, self.output_streams)
            return
        self.publishers = []
        for stream in self.output_streams:
            log.info("   -> %s", stream)
            self.publishers.append(StreamPublisher(process=self, stream_id=stream))

    def on_quit(self): #pragma no cover
        TransformStreamListener.on_quit(self)
        for publisher in self.publishers:
            publisher.close()

    def recv_packet(self, msg, stream_route, stream_id):
        log.info('Rebroadcasting message')

        self.publish(msg,None)

    def publish(self, msg, to_name):
        for publisher in self.publishers:
            publisher.publish(msg)



