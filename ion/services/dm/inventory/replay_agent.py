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
        #@TODO does this belong here? Should it be in the container somewhere?
        self.stream_count = len(streams)
        for name,stream_id in streams.iteritems():
            pub = self.stream_publisher_registrar.create_publisher(stream_id=stream_id)
            log.warn('Setup publisher named: %s' % name)
            setattr(self,name,pub)

        if not hasattr(self,'output'):
            raise RuntimeError('The replay agent requires an output stream publisher named output. Invalid configuration!')


    def execute_replay(self):
        ''' Performs the replay action
        Queries the data IAW the query argument and publishes the data on the output streams
        '''

        log.debug('(Replay Agent %s)', self.name)
        if self.query:
            datastore_name = self.query.get('datastore_name','dm_datastore')
            view_name = self.query.get('view_name','posts/posts_by_id')
            opts = self.query.get('options',{})
        else:
            datastore_name = 'dm_datastore'
            view_name = 'posts/posts_by_id'
            opts = {}
        log.debug('Replay Query:\n\t%s\n\t%s\n\t%s', datastore_name, view_name, opts);
        results = self._query(datastore_name=datastore_name,view_name=view_name,opts=opts)
        for result in results:
            log.warn('Result: %s' % result)
            blog_msg = result['value']
            blog_msg.is_replay= True
            self.output.publish(blog_msg)
        #@todo: log when there are not results
        if results is None:
            log.warn('No results found in replay query!')
        else:
            log.debug('Published replay!')

    def _query(self,datastore_name='dm_datastore', view_name='posts/posts_by_id', opts={}):
        '''
        Performs the query action
        '''
        db = CouchDB_DM_DataStore(datastore_name=datastore_name)
        ret = []
        if db.datastore_exists(datastore_name):
            ret = db.query_view(view_name=view_name,datastore_name=datastore_name,opts=opts)

        db.close()

        return ret


