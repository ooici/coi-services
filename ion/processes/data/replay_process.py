#!/usr/bin/env python

'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/processes/data/replay_process.py
@description Implementation for the Replay Agent
'''
from gevent.greenlet import Greenlet
from gevent.coros import RLock
from interface.objects import BlogBase, StreamGranuleContainer, DataStream, Encoding, StreamDefinitionContainer
from pyon.datastore.datastore import DataStore
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.public import log
from interface.services.dm.ireplay_process import BaseReplayProcess
from pyon.util.containers import DotDict
from pyon.core.exception import IonException
from pyon.util.file_sys import FS, FileSystem

import hashlib

class ReplayProcessException(IonException):
    """
    Exception class for IngestionManagementService exceptions. This class inherits from IonException
    and implements the __str__() method.
    """
    def __str__(self):
        return str(self.get_status_code()) + str(self.get_error_message())


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


        # Get the query
        self.query = self.CFG.get_safe('process.query',{})

        # Get the delivery_format
        self.delivery_format = self.CFG.get_safe('process.delivery_format',{})
        self.datastore_name = self.CFG.get_safe('process.datastore_name','dm_datastore')

        self.view_name = self.CFG.get_safe('process.view_name','datasets/dataset_by_id')
        self.key_id = self.CFG.get_safe('process.key_id')
        # Get a stream_id for this process
        self.stream_id = self.CFG.get_safe('process.publish_streams.output',{})



        if not (self.stream_id and hasattr(self,'output')):
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
        log.warn('results: %s', results)

        for result in results:
            log.warn('REPLAY Result: %s' % result)



            assert('doc' in result)

            replay_obj_msg = result['doc']

            if isinstance(replay_obj_msg, BlogBase):
                replay_obj_msg.is_replay = True

                self.lock.acquire()
                self.output.publish(replay_obj_msg)
                self.lock.release()

            elif isinstance(replay_obj_msg, StreamDefinitionContainer):

                replay_obj_msg.stream_resource_id = self.stream_id


            elif isinstance(replay_obj_msg, StreamGranuleContainer):

                # Override the resource_stream_id so ingestion doesn't reingest, also this is a NEW stream (replay)
                replay_obj_msg.stream_resource_id = self.stream_id

                datastream = None
                sha1 = None

                for key, identifiable in replay_obj_msg.identifiables.iteritems():
                    if isinstance(identifiable, DataStream):
                        datastream = identifiable
                    elif isinstance(identifiable, Encoding):
                        sha1 = identifiable.sha1

                if sha1: # if there is an encoding

                    # Get the file from disk
                    filename = FileSystem.get_url(FS.TEMP, sha1, ".hdf5")

                    log.warn('Replay reading from filename: %s' % filename)

                    hdf_string = ''
                    try:
                        with open(filename, mode='rb') as f:
                            hdf_string = f.read()
                            f.close()

                            # Check the Sha1
                            retreived_hdfstring_sha1 = hashlib.sha1(hdf_string).hexdigest().upper()

                            if sha1 != retreived_hdfstring_sha1:
                                raise  ReplayProcessException('The sha1 mismatch between the sha1 in datastream and the sha1 of hdf_string in the saved file in hdf storage')

                    except IOError:
                        log.warn('No HDF file found!')
                        #@todo deal with this situation? How?
                        hdf_string = 'HDF File %s not found!' % filename

                    # set the datastream.value field!
                    datastream.values = hdf_string

                else:
                    log.warn('No encoding in the StreamGranuleContainer!')

                self.lock.acquire()
                self.output.publish(replay_obj_msg)
                self.lock.release()


            else:
                 log.warn('Unknown type retrieved in DOC!')



        #@todo: log when there are not results
        if results is None:
            log.warn('No results found in replay query!')
        else:
            log.debug('Published replay!')


    def execute_replay(self):
        log.debug('(Replay Agent %s)', self.name)

        # Handle the query
        datastore_name = self.datastore_name
        key_id = self.key_id


        # Got the post ID, pull the post and the comments
        view_name = self.view_name
        opts = {
            'start_key':[key_id, 0],
            'end_key':[key_id,2],
            'include_docs': True
        }
        g = Greenlet(self._query,datastore_name=datastore_name, view_name=view_name, opts=opts,
            callback=lambda results: self._publish_query(results))
        g.start()




    def _query(self,datastore_name='dm_datastore', view_name='posts/posts_by_id', opts={}, callback=None):
        '''
        Performs the query action
        '''
        log.debug('Couch Query:\n\t%s\n\t%s\n\t%s', datastore_name, view_name, opts)
        #@todo: Fix this datastore management profile with correct data profile in near future
        db = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.EXAMPLES, self.CFG)


        ret = db.query_view(view_name=view_name,opts=opts)

        callback(ret)


