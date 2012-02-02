#!/usr/bin/env python

'''
@author Luke Campbell <lcampbell@asascience.com>
@file pyon/ion/replayagent.py
@description Implementation for the Replay Agent
'''
from pyon.ion.endpoint import StreamPublisherRegistrar
from interface.services.dm.ireplay_agent import BaseReplayAgent
from pyon.public import log
from pyon.datastore.couchdb.couchdb_dm_datastore import CouchDB_DM_DataStore

class ReplayAgent(BaseReplayAgent):
    def __init__(self, *args, **kwargs):
        super(ReplayAgent, self).__init__(*args,**kwargs)
        #@todo Init stuff
        
    def on_start(self):
        '''
        Creates a publisher for each stream_id passed in as publish_streams
        Creates an attribute with the name matching the stream name which corresponds to the publisher
        ex: say we have publish_streams:{'output': my_output_stream_id }
          then the instance has an attribute output which corresponds to the publisher for the stream
          in my_output_stream_id
        '''
        self.stream_publisher_registrar = StreamPublisherRegistrar(process=self,node=self.container.node)

        # Get the stream(s)
        streams = self.CFG.get('process',{}).get('publish_streams',{})

        # Get the query
        self.query = self.CFG.get('process',{}).get('query',{})

        # Get the delivery_format
        self.delivery_format = self.CFG.get('process',{}).get('delivery_format',{})

        # Attach a publisher to each stream_name attribute
        self.stream_count = len(streams)
        for name,stream_id in streams.iteritems():
            pub = self.stream_publisher_registrar.create_publisher(stream_id=stream_id)
            setattr(self,name,pub)

    def execute_replay(self):
        ''' Performs the replay action
        Queries the data IAW the query argument and publishes the data on the output streams
        '''
        if hasattr(self,'output'):
            for result in self._query():
                self.output.publish(result)
        log.debug('(Replay Agent %s)', self.name)
        log.debug('  Published...')

    def _query(self,datastore_name='dm_datastore', view_name='posts/index', key=''):
        '''
        Performs the query action
        '''
        db = CouchDB_DM_DataStore(datastore_name=datastore_name)

        if db.datastore_exists(datastore_name):
            ret = db.query_view(view_name,key)
            return ret


        return []


