#!/usr/bin/env python

'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/processes/data/replay_process.py
@description Implementation for the Replay Agent
'''
from prototype.hdf.hdf_codec import HDFEncoder
from pyon.util.containers import DotDict
from pyon.core.exception import IonException
from pyon.util.file_sys import FS, FileSystem
from gevent.greenlet import Greenlet
from gevent.coros import RLock
import time
import numpy as np
from interface.objects import BlogBase, StreamGranuleContainer, DataStream, Encoding, StreamDefinitionContainer
from prototype.hdf.hdf_array_iterator import acquire_data
from prototype.sci_data.constructor_apis import DefinitionTree, PointSupplementConstructor
from pyon.datastore.datastore import DataStore
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.public import log
from interface.services.dm.ireplay_process import BaseReplayProcess

import os

import hashlib


#@todo: Remove this debugging stuff
def llog(msg):
    with open(FileSystem.get_url(FS.TEMP,'debug'),'a') as f:
        f.write('%s ' % time.strftime('%Y-%m-%dT%H:%M:%S'))
        f.write(msg)
        f.write('\n')

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
        self.definition = self.delivery_format.get('container',None)
        self.fields = self.delivery_format.get('fields',None)
        self.record_count = self.delivery_format.get('record_count',None)

        self.view_name = self.CFG.get_safe('process.view_name','datasets/dataset_by_id')
        self.key_id = self.CFG.get_safe('process.key_id')
        # Get a stream_id for this process
        self.stream_id = self.CFG.get_safe('process.publish_streams.output',{})



        if not (self.stream_id and hasattr(self,'output')):
            raise RuntimeError('The replay agent requires an output stream publisher named output. Invalid configuration!')



    def _records(self, outgoing, n):
        '''
        Given a full packet return at most n packets per yield
        '''
        record_count = outgoing['records']
        file = outgoing['data']
        granule = outgoing['granule']
        element_count_id = DefinitionTree.get(self.defintion, '%s.element_count_id' % self.definition.data_stream_id)
        record_count = granule.identifiables[element_count_id]
        data_stream = granule.identifiables[self.definition.data_stream_id]

        for i in xrange(record_count / n):
            values = np.arange(0,n,dtype='float32')
            llog('chunk of values: %s' % str(values))
            codec = HDFEncoder()



        '''
        generator = acquire_data([file], self.fields, n)
        for segment in generator:
            # Get the hdf_string for segment
            # Replace the record count and bounds in the current granule
            record_count.value = len(segment)
            # Handle bounds
            data_stream.values = hdf_string
        '''


    def _parse_results(self, results):
        '''
        @return Returns a publish queue
        '''

        publish_queue = []
        outgoing_message = {
            'granule':None,
            'records':0,
            'data_string':''
        }
        for result in results:
            log.warn('REPLAY Result: %s' % result)

            # Here's what we'll do is we'll have a big queue
            # Inside this queue will go outgoing messages


            assert('doc' in result)

            replay_obj_msg = result['doc']

            if isinstance(replay_obj_msg, BlogBase):
                replay_obj_msg.is_replay = True
                self.lock.acquire()
                self.output.publish(replay_obj_msg)
                self.lock.release()
                continue

            elif isinstance(replay_obj_msg, StreamDefinitionContainer):
                replay_obj_msg.stream_resource_id = self.stream_id

            elif isinstance(replay_obj_msg, StreamGranuleContainer):
                packet = self._parse_granule(replay_obj_msg)
                if packet:
                    publish_queue.append(packet)

            else:
                log.warn('Unknown type retrieved in DOC!')
        return publish_queue

    def _parse_granule(self, replay_obj_msg):

        assert(isinstance(replay_obj_msg, StreamGranuleContainer))
        # The message is valid until proven otherwise.
        valid=True
        datastream = None
        sha1 = None

        # Override the resource_stream_id so ingestion doesn't reingest, also this is a NEW stream (replay)
        replay_obj_msg.stream_resource_id = self.stream_id

        # Get number of records
        record_count_id = DefinitionTree.get(self.definition,'%s.element_count_id' % self.definition.data_stream_id)
        record_count = replay_obj_msg.identifiables[record_count_id].value

        # Obviously it's not valid if there are no records.
        if not (record_count > 0):
            return None

        llog('----------------------------')
        llog('Records: %d' % record_count)

        # Get the datastream if there is one
        data_stream_id = self.definition.data_stream_id

        if data_stream_id in replay_obj_msg.identifiables:
            datastream = replay_obj_msg.identifiables[data_stream_id]
        else:
            datastream = self.definition.identifiables[data_stream_id]

        # Get the encoding if there is one
        encoding_id = datastream.encoding_id
        if not encoding_id in replay_obj_msg.identifiables:
            return None
        else:
            sha1 = replay_obj_msg.identifiables[encoding_id].sha1 or None

        if self.fields: # This matches what is in the definition
            # Check for these fields in the granule
            for field in self.fields:
                llog('Field: %s' % field)
                # See if the granule's got the field
                if field in replay_obj_msg.identifiables:
                    # The field is there, so get the updated info like values path from the granule
                    range_id = replay_obj_msg.identifiables[field].range_id
                    # The range may be in the granule or the definition, try the granule first (pri)
                else:
                    # Ok so maybe the range is in the granule but not the coverage
                    range_id = self.definition.identifiables[field].range_id
                    llog('range_id is %s' % range_id)

                if range_id in replay_obj_msg.identifiables:
                    # The range object is in the granule and has the correct values
                    range_obj = replay_obj_msg.identifiables[range_id]
                    values_path = range_obj.values_path or self.definition.identifiables[range_id].values_path
                    llog('Got values path: %s' % values_path)

                else:
                    # The granule doesn't have the data we're looking for, continue
                    # This message is no longer valid
                    llog('granule didnt have enough info %s not found' % range_id)
                    return None # msg is invalid
        else:
            # No fields were specified so let's see what the fields were
            fields = DefinitionTree.get(self.definition,'%s.element_type_id.data_record_id.field_ids' % self.definition.data_stream_id)
            llog('%s' % fields)
        if not sha1:
            return None # There is no encoding, no values and therefore not valid

        llog("Got sha1: %s" % sha1)
        # There is in fact an hdf file
        filepath = FileSystem.get_url(FS.TEMP,'%s.hdf5' % sha1)
        if not os.path.exists(filepath):
            llog('File doesnt exist: %s' % FileSystem.get_url(FS.TEMP,'%s.hdf5' % sha1))
            return None # Not valid

        # The file is there get the DATA!!!!
        # acquire_data




        llog('Pushing message')
        return {
            'granule':replay_obj_msg,
            'records':record_count,
            'data':sha1
        }


    def _merge(self, msgs):
        records = 0
        files = []
        for msg in msgs:
            records += msg['records']
            files.append(msg['data'])
        granule = PointSupplementConstructor(point_definition=self.definition, stream_id=self.stream_id)

        # acquire_data from every file
        # gen = acquire_data(files,self.fields,records)
        # data = gen.next()[4]

        #------ MOCKING --------
        codec = HDFEncoder()
        data = {}
        for field in self.fields:
            llog('making data for field %s' % field)
            data[field] = np.arange(0,records,dtype='float32')

            for i in xrange(len(data[field])):
                coverage_id = DefinitionTree.get(self.definition,'%s.range_id' % field)
                granule.add_scalar_point_coverage(point_id=i,coverage_id=coverage_id,value=data[field][i])

            codec.add_hdf_dataset(field,data[field])

        hdf_string = codec.encoder_close()
        sha1 = hashlib.sha1(hdf_string).hexdigest().upper()
        #-----------------------


        #-----------------------
        # At this point I have the things I need

        granule = granule.close_stream_granule()
        encoding_id = DefinitionTree.get(self.definition, '%s.encoding_id' % self.definition.data_stream_id)
        element_count_id = DefinitionTree.get(self.definition, '%s.element_count_id' % self.definition.data_stream_id)

        #@todo: fill this in
        # acquire_data([files], fields, records)
        # get me the hdf_encoding => hdf_string
        granule.identifiables[encoding_id].sha1 = sha1
        granule.identifiables[element_count_id].value = records

        # get bounds for each field etc.
        llog('merged granule: %s' % granule.identifiables.keys())

        return {
            'granule':granule,
            'records':records,
            'data':sha1,
            'hdf_string':hdf_string
        }






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


        log.warn('results: %s', results)

        publish_queue = self._parse_results(results)
        outgoing_message = {
            'granule':None,
            'records':0,
            'data_string':''
        }

        if self.record_count:
            llog('I need to chop up the messages')
            outgoing = self._merge(publish_queue)


        else: #Transmit it in one big shot
            outgoing = self._merge(publish_queue)
            packet = outgoing['granule']
            packet.identifiables[self.definition.data_stream_id].values = outgoing['hdf_string']
            llog('outgoing:')
            llog('\tGranule: %s' % outgoing['granule'].identifiables.keys())
            llog('\tRecords: %s' % outgoing['records'])
            llog('\tSha1: %s' % outgoing['data'])
            self.lock.acquire()
            self.output.publish(packet)
            self.lock.release()



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


