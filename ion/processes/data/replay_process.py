__author__ = 'luke'
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

    process_type = 'standalone'

    def __init__(self, *args, **kwargs):
        super(ReplayProcess,self).__init__(*args,**kwargs)

        self.lock = RLock()

    def on_start(self):

        self.query = self.CFG.get_safe('process.query',{})

        self.delivery_format = self.CFG.get_safe('process.delivery_format',{})
        self.datastore_name = self.CFG.get_safe('process.datastore_name','dm_datastore')

        self.definition = self.delivery_format.get('container')

        self.fields = self.delivery_format.get('fields',None)

        self.view_name = self.CFG.get_safe('process.view_name','datasets/dataset_by_id')
        self.key_id = self.CFG.get_safe('process.key_id')
        self.stream_id = self.CFG.get_safe('process.publish_streams.output')

        if not (self.stream_id and hasattr(self, 'output')):
            raise RuntimeError('The replay agent requires an output stream publisher named output. Invalid configuration!')

    def execute_replay(self):
        datastore_name = self.datastore_name
        key_id = self.key_id

        view_name = self.view_name

        opts = {
            'start_key':[key_id,0],
            'end_key':[key_id,2],
            'include_docs':True
        }

        g = Greenlet(self._query,datastore_name=datastore_name, view_name=view_name, opts=opts,
            callback=lambda results: self._publish_query(results))
        g.start()

    def _query(self,datastore_name='dm_datastore', view_name='posts/posts_by_id', opts={}, callback=None):
        db = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.EXAMPLES, self.CFG)

        ret = db.query_view(view_name=view_name,opts=opts)

        callback(ret)

    def _publish_query(self, results):

        if results is None:
            log.warn('No Results')
            return

        publish_queue = self._parse_results(results)

        dataset = self._merge(publish_queue)

        # regardless of what anyone wants at first the entire dataset is going
        packet = dataset['granule']
        llog('outgoing: ')
        llog('\tGranule: %s' % packet.identifiables.keys())
        llog('\tRecords: %s' % dataset['records'])
        llog('\tSHA1: %s' % dataset['sha1'])

        self.lock.acquire()
        self.output.publish(packet)
        self.lock.release()

    def _parse_results(self, results):

        publish_queue = []

        for result in results:
            assert('doc' in result)

            packet = result['doc']

            if isinstance(packet, BlogBase):
                packet.is_replay = True
                self.lock.acquire()
                self.output.publish(packet)
                self.lock.release()
                continue

            if isinstance(packet, StreamDefinitionContainer):
                continue # Ignore

            if isinstance(packet, StreamGranuleContainer):
                packet = self._parse_granule(packet)
                if packet:
                    publish_queue.append(packet)
                continue

            log.warn('Unknown packet type in replay.')

        return publish_queue

    def _records(self, dataset, n):
        '''
        Returns a packet of at most n records
        '''
        records = dataset['records']
        segments = records / n
        for i in xrange(segments+1):
            granule = PointSupplementConstructor(point_definition=self.definition, stream_id=self.stream_id)
            for field in self.fields:
                data = np.arange(i*segments,(i+1)*segments,dtype='float32')
                for i in data:
                    granule.add_scalar_point_coverage(point_id=i, coverage_id=field,value=data[i])
            granule = granule.close_stream_granule()
            yield granule
            

    def _parse_granule(self, granule):

        granule.stream_resource_id = self.stream_id



        element_count_id = DefinitionTree.get(self.definition,'%s.element_count_id' % self.definition.data_stream_id)
        encoding_id = DefinitionTree.get(self.definition,'%s.encoding_id' % self.definition.data_stream_id)

        record_count = granule.identifiables[element_count_id].value
        sha1 = granule.identifiables[encoding_id].sha1 or None

        # If there are no records then this is not a proper granule
        if not (record_count > 0):
            return None

        # No encoding, no packet
        if not encoding_id in granule.identifiables:
            return None

        if not sha1:
            return None


        filepath = FileSystem.get_url(FS.TEMP,'%s.hdf5' % sha1)

        if not os.path.exists(filepath):
            return None

        return {
            'granule':granule,
            'records':record_count,
            'sha1':sha1
        }



    def _merge(self, msgs):

        records = 0
        files = []
        for msg in msgs:
            records += msg['records']
            files.append(msg['sha1'])
        granule = PointSupplementConstructor(point_definition=self.definition, stream_id=self.stream_id)

        data = {}
        for field in self.fields:
            data[field] = np.arange(0,records,dtype='float32')

            for i in xrange(len(data[field])):
            #                point_id = granule.add_point(time=np.float32(i), location=(1,1,1))
                coverage_id = DefinitionTree.get(self.definition,'%s.range_id' % field)
                granule.add_scalar_point_coverage(point_id=i, coverage_id=coverage_id, value=data[field][i])


        granule = granule.close_stream_granule()
        encoding_id = DefinitionTree.get(self.definition,'%s.encoding_id' % self.definition.data_stream_id)
        sha1 = granule.identifiables[encoding_id].sha1

        element_count_id = DefinitionTree.get(self.definition,'%s.element_count_id' % self.definition.data_stream_id)
        granule.identifiables[element_count_id].value = records
        return {
            'granule':granule,
            'records':records,
            'sha1':sha1,
            }
