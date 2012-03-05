'''
@author Luke Campbell
@file ion/processes/data/replay_process.py
@description Replay Process handling set manipulations for the DataModel
'''
import hashlib
from prototype.sci_data.stream_parser import PointSupplementStreamParser
from pyon.public import log
from pyon.datastore.datastore import DataStore
from pyon.core.exception import IonException
from pyon.util.file_sys import FS, FileSystem
from prototype.hdf.hdf_array_iterator import acquire_data
from prototype.hdf.hdf_codec import HDFEncoder

from prototype.sci_data.constructor_apis import DefinitionTree, PointSupplementConstructor
from interface.objects import BlogBase, StreamGranuleContainer, StreamDefinitionContainer, CoordinateAxis, QuantityRangeElement, CountElement, RangeSet
from interface.services.dm.ireplay_process import BaseReplayProcess
from gevent.greenlet import Greenlet
from gevent.coros import RLock
import os
import time
import numpy as np


#@todo: Remove this debugging stuff
def llog(msg):
    with open(FileSystem.get_url(FS.CACHE,'debug'),'a') as f:
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
        '''
        Spawns a greenlet to take care of the query and work
        '''
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
        '''
        Makes the couch query and then callsback to publish
        '''
        db = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA, self.CFG)

        ret = db.query_view(view_name=view_name,opts=opts)

        callback(ret)

    def _publish_query(self, results):
        '''
        Merges the results into one dataset
        Manages the publishing rules
        '''

        if results is None:
            log.warn('No Results')
            return

        publish_queue = self._parse_results(results)
        for item in publish_queue:
            log.debug('Item in queue: %s' % type(item))
        granule = self._merge(publish_queue)

        if self.delivery_format.has_key('fields'):
            granule = self.subset(granule,self.delivery_format['fields'])

        element_count_id = DefinitionTree.get(self.definition, '%s.element_count_id' % self.definition.data_stream_id)
        encoding_id = DefinitionTree.get(self.definition,'%s.encoding_id' % self.definition.data_stream_id)
        record_count = granule.identifiables[element_count_id].value
        sha1 = granule.identifiables[encoding_id].sha1
        # regardless of what anyone wants at first the entire dataset is going

        llog('outgoing: ')
        llog('\tGranule: %s' % granule.identifiables.keys())
        llog('\tRecords: %s' % record_count)
        llog('\tSHA1: %s' % sha1)

        self.lock.acquire()
        self.output.publish(granule)
        self.lock.release()

    def _parse_results(self, results):
        '''
        Switch-case logic for what packet types replay can handle and how to handle
        '''
        log.debug('called _parse_results')
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
                log.debug('Got packet')
                if packet:
                    log.debug('Appending packet')
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
                    point_id = granule.add_point(time=i, location=(0,0,i))
                    granule.add_scalar_point_coverage(point_id=point_id, coverage_id=field,value=data[i])
            granule = granule.close_stream_granule()
            yield granule


    def _parse_granule(self, granule):
        '''
        Ensures the granule is valid and gets some metadata from the granule for building the dataset
        '''

        granule.stream_resource_id = self.stream_id

        element_count_id = DefinitionTree.get(self.definition,'%s.element_count_id' % self.definition.data_stream_id)
        encoding_id = DefinitionTree.get(self.definition,'%s.encoding_id' % self.definition.data_stream_id)

        record_count = granule.identifiables[element_count_id].value
        sha1 = granule.identifiables[encoding_id].sha1 or None

        # If there are no records then this is not a proper granule
        if not (record_count > 0):
            log.debug('Granule had no record count discarding.')
            return None

        # No encoding, no packet
        if not encoding_id in granule.identifiables:
            log.debug('Granule had no encoding discarding.')
            return None

        if not sha1:
            log.debug('Granule had no sha1')
            return None


        filepath = FileSystem.get_url(FS.CACHE,'%s.hdf5' % sha1)

        if not os.path.exists(filepath):
            log.debug('File with sha1 does not exist')
            return None

        return {
            'granule':granule,
            'records':record_count,
            'sha1':sha1
        }

    @staticmethod
    def merge_granule(definition, granule1, granule2):
        '''

        returns the union of these two granules

        granule1 := granule1 U granule2
        '''
        assert isinstance(definition,StreamDefinitionContainer), 'object is not a definition.'
        assert isinstance(granule1, StreamGranuleContainer), 'object is not a granule.'
        assert isinstance(granule2, StreamGranuleContainer), 'object is not a granule.'

        encoding_id = DefinitionTree.get(definition,'%s.encoding_id' % definition.data_stream_id)

        files = []
        if encoding_id in granule1.identifiables:
            if granule1.identifiables[encoding_id].sha1:
                files.append('%s.hdf5' % granule1.identifiables[encoding_id].sha1)
        if encoding_id in granule2.identifiables:
            if granule2.identifiables[encoding_id].sha1:
                files.append('%s.hdf5' % granule2.identifiables[encoding_id].sha1)

        element_count_id = DefinitionTree.get(definition, '%s.element_count_id' % definition.data_stream_id)
        record_count = 0
        if element_count_id in granule1.identifiables:
            record_count += granule1.identifiables[element_count_id].value
        if element_count_id in granule2.identifiables:
            record_count += granule2.identifiables[element_count_id].value

        if not element_count_id in granule1.identifiables:
            granule1.identifiables[element_count_id] = CountElement()
            granule1.identifiables[element_count_id].value = record_count
        else:
            granule1.identifiables[element_count_id].value = record_count

        fields1 = ReplayProcess._list_data(definition, granule1)
        fields2 = ReplayProcess._list_data(definition, granule2)
        #@todo albeit counterintuitive an intersection is the only thing I can support
        merged_paths = {}
        for k,v in fields1.iteritems():
            if fields2.has_key(k):
                merged_paths[k] = v



        for k,v in granule2.identifiables.iteritems():
            # Switch(value):

            # Case Bounds:
            if isinstance(v, QuantityRangeElement):
                # If its not in granule1 just throw it in there
                if k not in granule1.identifiables:
                    llog('%s was not in first granule, appending...' % k)
                    granule1.identifiables[k] = v
                else:
                    llog('%s was in the first one calculating upper and lower bounds.' % k)
                    bounds1 = granule1.identifiables[k].value_pair
                    bounds2 = granule2.identifiables[k].value_pair
                    bounds = np.append(bounds1,bounds2)
                    granule1.identifiables[k].value_pair = [np.nanmin(bounds), np.nanmax(bounds)]


            if isinstance(v, RangeSet): #Including coordinate axis
                if merged_paths.has_key(k) and not granule1.identifiables.has_key(k):
                    granule1.identifiables[k] = v # Copy it over

        # Now make sure granule1 doesnt have excess stuff
        del_list = []
        for k,v in granule1.identifiables.iteritems():
            if isinstance(v, RangeSet):
                if not merged_paths.has_key(k):
                    del_list.append(k)

        for item in del_list:
            del granule1.identifiables[item]

        # generator = acquire_data(files, merged_paths.values(), record_count)
        # genset = generator.next()['arrays_out_dict']
        codec = HDFEncoder()
        dataset = {}
        for field in merged_paths.values():

            # codec.add_hdf_dataset(field, genset[field])
            codec.add_hdf_dataset(field, np.arange(0,record_count,dtype='float32'))

        hdf_string = codec.encoder_close()
        granule1.identifiables[definition.data_stream_id].values = hdf_string
        granule1.identifiables[encoding_id].sha1 = hashlib.sha1(hdf_string).hexdigest().upper()

        return granule1




    @staticmethod
    def _list_data(definition, granule):
        '''
        Lists all the fields in the granule
        '''
        from interface.objects import StreamDefinitionContainer, StreamGranuleContainer, RangeSet, CoordinateAxis
        assert isinstance(definition, StreamDefinitionContainer), 'object is not a definition.'
        assert isinstance(granule, StreamGranuleContainer), 'object is not a granule.'
        retval = {}
        for key, value in granule.identifiables.iteritems():
            if isinstance(value, RangeSet):
                values_path = value.values_path or definition.identifiables[key].values_path
                retval[key] = values_path
            elif isinstance(value, CoordinateAxis):
                values_path = value.values_path or definition.identifiables[key].values_path
                retval[key] = values_path

        return retval



    def _merge(self, msgs):
        '''
        Merges all the granules and datasets into one large dataset (Union)
             n
        D := U [ msgs_i ]
            i=0
        '''
        granule = None
        count = len(msgs)
        for i in xrange(count):
            if i==0:
                granule = msgs[0]['granule']
                continue
            granule = ReplayProcess.merge_granule(definition=self.definition, granule1=granule, granule2=msgs[i]['granule'])

        return granule


#        return {
#            'granule':granule,
#            'records':records,
#            'sha1':sha1,
#            }

    def subset(self,granule,coverages):
        '''
        returns a dataset subset based on the fields

        '''
        assert isinstance(granule, StreamGranuleContainer), 'object is not a granule.'
        field_ids = DefinitionTree.get(self.definition,'%s.element_type_id.data_record_id.field_ids' % self.definition.data_stream_id)
        element_count_id = DefinitionTree.get(self.definition, '%s.element_count_id' % self.definition.data_stream_id)
        encoding_id = DefinitionTree.get(self.definition,'%s.encoding_id' % self.definition.data_stream_id)


        values_path = list()
        domain_ids = list()
        coverage_ids = list()
        coverages = list(coverages)
        log.debug('Coverages include %s of type %s', coverages, type(coverages))
        for field_id in field_ids:
            range_id = self.definition.identifiables[field_id].range_id
            if isinstance(self.definition.identifiables[range_id], CoordinateAxis):
                log.debug('got a domain: %s' % range_id)
                domain_ids.append(field_id)
                if granule.identifiables.has_key(range_id):
                    value_path = granule.identifiables[range_id].values_path or self.definition.identifiables[range_id].values_path
                    values_path.append(value_path)
                else:
                    value_path = self.definition.identifiables[range_id].values_path
                    values_path.append(value_path)
                continue

            if isinstance(self.definition.identifiables[range_id], RangeSet):
                # If its a rangeset, a specified coverage and the granule has it, add it to the list
                if  field_id in coverages:
                    if granule.identifiables.has_key(range_id):
                        log.debug('got a range: %s' % range_id)
                        coverage_ids.append(field_id)
                        if granule.identifiables.has_key(range_id):
                            value_path = granule.identifiables[range_id].values_path or self.definition.identifiables[range_id].values_path
                            values_path.append(value_path)
                        else:
                            value_path = self.definition.identifiables[range_id].values_path
                            values_path.append(value_path)
                        continue

                range_id = self.definition.identifiables[field_id].range_id
                bounds_id = self.definition.identifiables[range_id].bounds_id



                if not (field_id in coverages):
                    log.debug('%s doesn\'t belong in %s.', field_id, coverages)
                    log.debug('rebool: %s', bool(field_id in coverages))
                    if granule.identifiables.has_key(range_id):
                        log.debug('Removing %s from granule', range_id)
                        del granule.identifiables[range_id]
                    if granule.identifiables.has_key(bounds_id):
                        log.debug('Removing %s from granule', bounds_id)
                        del granule.identifiables[bounds_id]
        log.debug('Domains: %s', domain_ids)
        log.debug('Ranges: %s', coverage_ids)
        log.debug('Values_paths: %s', values_path)
        f = FileSystem.mktemp()
        f.write(granule.identifiables[self.definition.data_stream_id].values)
        file_path = f.name
        f.close()

        assert os.path.exists(file_path), 'file didn\'t persist.'
        full_coverage = list(domain_ids + coverage_ids)





        log.debug('Full coverage: %s' % full_coverage)
        log.debug('Calling acquire_data with: %s, %s, %s', [file_path],values_path,granule.identifiables[element_count_id].value)
        generator = acquire_data([file_path],values_path,granule.identifiables[element_count_id].value)
        codec = HDFEncoder()
        dataset = generator.next()['arrays_out_dict']
        for field in dataset.keys():
            codec.add_hdf_dataset(field, dataset[field])
        hdf_string = codec.encoder_close()
        granule.identifiables[self.definition.data_stream_id].values = hdf_string
        granule.identifiables[encoding_id].sha1 = hashlib.sha1(hdf_string).hexdigest().upper()

        return granule