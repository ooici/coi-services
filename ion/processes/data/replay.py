#!/usr/bin/env python

"""Process that replays data from the datastore"""

__author__ = 'Michael Meisinger'

from pyon.public import StreamProcess

class ReplayProcess(StreamProcess):
    def on_init(self, *args, **kwargs):
        # Determine what data to replay

        # Determine how to replay data

        # Determine target for replay
        self.target_xname = None

        # Open target publishing endpoint

    def on_start(self, *args, **kwargs):
        # Get chunks from science data store

        # Publish as messages
        pass

    def on_quit(self, *args, **kwargs):
        # We are getting interrupted
        pass

    def process(self, packet):
        # We don't receive any input nominally

        # Possibility: Subscribe to real time updates and keep track
        # so that continuation of replay into the future can be seamless
        pass
