#!/usr/bin/env python

'''
@author Luke Campbell <lcampbell@asascience.com>
@file pyon/ion/replayagent.py
@description Implementation for the Replay Agent
'''
from gevent.greenlet import Greenlet
from gevent.coros import RLock
from pyon.core.exception import BadRequest
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.public import log
from pyon.datastore.couchdb.couchdb_datastore import CouchDB_DataStore
from interface.services.dm.ireplay_process import BaseReplayProcess
class ReplayProcess(BaseReplayProcess):
    process_type="standalone"
    def __init__(self, *args, **kwargs):
        super(ReplayProcess, self).__init__(*args,**kwargs)
        #@todo Init stuff
        # mutex for shared resources between threads
        self.lock = RLock()
        
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


    def _publish_query(self, results):
        '''
        Callback to publish the specified results
        '''
        #-----------------------
        # Iteration
        #-----------------------
        #  - Go through the results, if the user had include_docs=True in the options field
        #    then the full document is in result.doc; however if the query did not include_docs,
        #    then only the doc_id is provided in the result.value.
        #
        #  - What this allows us to do is limit the amount of traffic in information for large queries.
        #    If we only are making a query in a sequence of queries (such as map and reduce) then we don't
        #    care about the full document, yet, we only care about the doc id and will retrieve the document later.
        #  - Example:
        #      Imagine the blogging example, we want the latest blog by author George and all the comments for that blog
        #      The series of queries would go, post_by_updated -> posts_by_author -> posts_join_comments and then
        #      in the last query we'll set include_docs to true and parse the docs.
        #-----------------------

        #@todo: Add thread sync here because self.output is shared and deadlocks COULD occur

        for result in results:
            log.warn('Result: %s' % result)
            if 'doc' in result:
                log.debug('Result contains document.')
                blog_msg = result['doc']
                blog_msg.is_replay = True
            else:
                blog_msg = result['value'] # Document ID, not a document
            self.lock.acquire()
            self.output.publish(blog_msg)
            self.lock.release()

        #@todo: log when there are not results
        if results is None:
            log.warn('No results found in replay query!')
        else:
            log.debug('Published replay!')


    def execute_replay(self):
        log.debug('(Replay Agent %s)', self.name)

        # Handle the query
        if self.query:
            datastore_name = self.query.get('datastore_name','dm_datastore')
            post_id = self.query.get('post_id','7877516528284978243')
        else:
            raise BadRequest('(Replay Agent %s): Improper Query Received' % self.name)

        # Got the post ID, pull the post and the comments
        view_name = 'posts/posts_join_comments'
        opts = {
            'start_key':[post_id, 0],
            'end_key':[post_id,2],
            'include_docs': True
        }
        g = Greenlet(self._query,datastore_name=datastore_name, view_name=view_name, opts=opts,
            callback=lambda results: self._publish_query(results))
        g.start()

#    def execute_replay(self):
#        '''
#        Performs the replay action in a threaded manner
#        Queries the data IAW the query argument and publishes the data on the output streams
#        '''
#
#        log.debug('(Replay Agent %s)', self.name)
#        if self.query:
#            datastore_name = self.query.get('datastore_name','dm_datastore')
#            view_name = self.query.get('view_name','posts/posts_by_id')
#            opts = self.query.get('options',{'include_docs=True'})
#        else:
#            datastore_name = 'dm_datastore'
#            view_name = 'posts/posts_by_id'
#            opts = {'include_docs=True'}
#
#        log.debug('Replay Query:\n\t%s\n\t%s\n\t%s', datastore_name, view_name, opts)
#
#        #@todo: Evaluate the possibility of a separate stream per thread
#        #---------------------
#        # Threaded (greenlet)
#        #---------------------
#        # Execute_replay is now non_blocking
#        # execute_replay is thread safe so it can be called multiple times,
#        # there is a bottleneck at the stream, if the thread is publishing a large stream,
#        # the other threads have to wait until it is done.
#
#
#        g = Greenlet(self._query,datastore_name=datastore_name,view_name=view_name,opts=opts,
#            callback = lambda results: self._publish_query(results))
#
#        g.start()



    def _query(self,datastore_name='dm_datastore', view_name='posts/posts_by_id', opts={}, callback=None):
        '''
        Performs the query action
        '''
        log.debug('Couch Query:\n\t%s\n\t%s\n\t%s', datastore_name, view_name, opts)
        db = CouchDB_DataStore(datastore_name=datastore_name)
        ret = []
        if db.datastore_exists(datastore_name):
            ret = db.query_view(view_name=view_name,datastore_name=datastore_name,opts=opts)

        db.close()

        callback(ret)


