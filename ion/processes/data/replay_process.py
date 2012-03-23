'''
@author Luke Campbell
@file ion/processes/data/replay_process.py
@description Replay Process handling set manipulations for the DataModel
'''

import os
import time
import copy
import hashlib

from gevent.greenlet import Greenlet
from gevent.coros import RLock

from pyon.core.exception import IonException, BadRequest, Inconsistent
from pyon.datastore.datastore import DataStore
from pyon.public import log
from pyon.util.file_sys import FS, FileSystem

from prototype.hdf.hdf_array_iterator import acquire_data
from prototype.hdf.hdf_codec import HDFEncoder
from prototype.sci_data.constructor_apis import DefinitionTree, PointSupplementConstructor

from interface.objects import BlogBase, StreamGranuleContainer, StreamDefinitionContainer, CoordinateAxis, QuantityRangeElement, CountElement, RangeSet
from interface.services.dm.ireplay_process import BaseReplayProcess
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceProcessClient



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

        definition_id = self.delivery_format.get('definition_id')
        rrsc = ResourceRegistryServiceProcessClient(process=self)
        definition = rrsc.read(definition_id)
        self.definition = definition.container

        self.fields = self.delivery_format.get('fields',None)

        self.view_name = self.CFG.get_safe('process.view_name','datasets/dataset_by_id')
        self.key_id = self.CFG.get_safe('process.key_id')
        self.stream_id = self.CFG.get_safe('process.publish_streams.output')

        if not self.stream_id:
            raise Inconsistent('The replay process requires a stream id. Invalid configuration!')

        self.data_stream_id = self.definition.data_stream_id
        self.encoding_id = self.definition.identifiables[self.data_stream_id].encoding_id
        self.element_type_id = self.definition.identifiables[self.data_stream_id].element_type_id
        self.element_count_id = self.definition.identifiables[self.data_stream_id].element_count_id
        self.data_record_id = self.definition.identifiables[self.element_type_id].data_record_id
        self.field_ids = self.definition.identifiables[self.data_record_id].field_ids
        self.domain_ids = self.definition.identifiables[self.data_record_id].domain_ids
        self.time_id = self.definition.identifiables[self.domain_ids[0]].temporal_coordinate_vector_id

    def execute_replay(self):
        '''
        @brief Spawns a greenlet to take care of the query and work
        '''
        if not hasattr(self, 'output'):
            raise Inconsistent('The replay process requires an output stream publisher named output. Invalid configuration!')

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
        @brief Makes the couch query and then callsback to publish
        @param datastore_name Name of the datastore
        @param view_name The name of the design view where the data is organized
        @param opts options to pass
        @param callback the content handler
        '''
        db = self.container.datastore_manager.get_datastore(datastore_name, DataStore.DS_PROFILE.SCIDATA, self.CFG)

        ret = db.query_view(view_name=view_name,opts=opts)

        callback(ret)

    def _publish_query(self, results):
        '''
        @brief Publishes the appropriate data based on the delivery format and data returned from query
        @param results The query results from the couch query
        '''

        if results is None:
            log.info('No Results')
            return

        publish_queue = self._parse_results(results)
        for item in publish_queue:
            log.debug('Item in queue: %s' % type(item))
        granule = self._merge(publish_queue)
        if not granule:
            return # no dataset

        if self.delivery_format.has_key('fields'):
            res = self.subset(granule,self.delivery_format['fields'])
            granule = res

        if self.delivery_format.has_key('time'):
            granule = self.time_subset(granule, self.delivery_format['time'])

        total_records = granule.identifiables[self.element_count_id].value
        granule.identifiables[self.element_count_id].constraint.intervals = [[0, total_records-1],]


        if self.delivery_format.has_key('records'):
            assert isinstance(self.delivery_format['records'], int), 'delivery format is incorrectly formatted.'

            for chunk in self._records(granule,self.delivery_format['records']):
                self.lock.acquire()
                self.output.publish(chunk)
                self.lock.release()
            return


        self.lock.acquire()
        self.output.publish(granule)
        self.lock.release()

    def _parse_results(self, results):
        '''
        @brief Switch-case logic for what packet types replay can handle and how to handle
        @param results List of results returned from couch view
        @return A queue of msgs parsed and formatted to be iterated through and published.
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
                log.debug('Got packet')
                packet = self._parse_granule(packet)
                if packet:
                    log.debug('Appending packet')
                    publish_queue.append(packet)
                continue

            log.info('Unknown packet type in replay.')

        return publish_queue

    def _records(self, granule, n):
        '''
        @brief Yields n records from a granule per iteration
        @param granule consisting of dataset
        @param n number of records to yield
        '''
        bin_size = n
        record_count = granule.identifiables[self.element_count_id].value

        i=0
        while (i+bin_size) < record_count:
            log.debug('Yielding %d to %d', i, i+bin_size)
            yield self._slice(granule,slice(i,i+bin_size))
            i+=bin_size
        if i < record_count:
            yield self._slice(granule, slice(i,i+bin_size))
        return

    def _pair_up(self, granule):
        '''
        @brief Creates a list of tuples consisting of acquire_data friendly var_names and full values_paths
        @param granule consisting of full dataset.
        @return list of tuples
        '''
        fields = self._list_data(self.definition, granule)
        pairs = list()
        for i in fields.values():
            pairs.append((i.split('/').pop(),i))
        return pairs

    def _find_vp(self, pairs, var_name):
        '''
        @brief Determines the value path based on the acquire_data friendly var_name
        @param pairs List of tuples consisting of pair-wise var_name/value_path
        @param var_name Desired var_name
        @return Associated value_path
        '''
        for pair in pairs:
            if var_name == pair[0]:
                return pair[1]
        return

    def _slice(self,granule,slice_):
        '''
        @brief Creates a granule which is a slice of the granule parameter
        @param granule the superset
        @param slice_ The slice values for which to create the granule
        @return Crafted subset granule of the parameter granule.
        '''
        retval = copy.deepcopy(granule)
        fields = self._list_data(self.definition,granule)
        record_count = slice_.stop - slice_.start
        assert record_count > 0, 'slice is malformed'
        pairs = self._pair_up(granule)
        var_names = list([i[0] for i in pairs]) # Get the var_names from the pairs
        log.debug('var_names: %s',var_names)
        file_path = self._get_hdf_from_string(granule.identifiables[self.data_stream_id].values)
        codec = HDFEncoder()
        vectors = acquire_data([file_path],var_names,record_count,slice_ ).next()

        for row, value in vectors.iteritems():
            vp = self._find_vp(pairs, row)
            # Determine the range_id reverse dictionary lookup
            #@todo: improve this pattern
            for field,path in fields.iteritems():
                if vp==path:
                    range_id = field
                    break
            bounds_id = retval.identifiables[range_id].bounds_id
            # Recalculate the bounds for this fields and update the granule
            range = value['range']
            retval.identifiables[bounds_id].value_pair[0] = float(range[0])
            retval.identifiables[bounds_id].value_pair[1] = float(range[1])
            codec.add_hdf_dataset(vp, value['values'])
            record_count = len(value['values'])
            #----- DEBUGGING ---------
            log.debug('slice- row: %s', row)
            log.debug('slice- value_path: %s', vp)
            log.debug('slice- range_id: %s', range_id)
            log.debug('slice- bounds_id: %s', bounds_id)
            log.debug('slice- limits: %s', value['range'])
            #-------------------------


        retval.identifiables[self.element_count_id].value = record_count
        hdf_string = codec.encoder_close()
        self._patch_granule(retval, hdf_string)
        FileSystem.unlink(file_path)
        return retval


    def _parse_granule(self, granule):
        '''
        @brief Ensures the granule is valid and gets some metadata from the granule for building the dataset
        @param granule raw granule straight from couch
        @return metadata in the granule as well as the granule itself if valid.
        '''

        granule.stream_resource_id = self.stream_id

        element_count_id = self.element_count_id
        encoding_id = self.encoding_id

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


        filepath = FileSystem.get_hierarchical_url(FS.CACHE, sha1, '.hdf5')

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
        @brief Merges two granules based on the definition
        @param definition Stream Definition
        @param granule1 First Granule
        @param granule2 Second Granule
        @return Returns granule1 which is then merged with granule2 and the file pair for indexing

        @description granule1 := granule1 U granule2
        '''
        import numpy as np

        assert isinstance(definition,StreamDefinitionContainer), 'object is not a definition.'
        assert isinstance(granule1, StreamGranuleContainer), 'object is not a granule.'
        encoding_id = DefinitionTree.get(definition,'%s.encoding_id' % definition.data_stream_id)

        if not granule2:
            pair = (
                granule1.identifiables['time_bounds'].value_pair[0],
                '%s.hdf5' % granule1.identifiables[encoding_id].sha1
                )
            return {
                'granule':granule1,
                'files':[pair]
            }

        assert isinstance(granule2, StreamGranuleContainer), 'object is not a granule.'

        assert granule1.identifiables.has_key('time_bounds'), 'object has no time bounds and therefore is invalid.'

        assert granule2.identifiables.has_key('time_bounds'), 'object has no time bounds and therefore is invalid.'

        #-------------------------------------------------------------------------------------
        # First step is figure out where each granule belongs on the timeline
        # We do this with a tuple consisting of the point in the timeline and the filename
        # These will get stable sorted later
        #-------------------------------------------------------------------------------------

        pair1 = (
            granule1.identifiables['time_bounds'].value_pair[0],
            '%s.hdf5' % granule1.identifiables[encoding_id].sha1
            )

        pair2 = (
            granule2.identifiables['time_bounds'].value_pair[0],
            '%s.hdf5' % granule2.identifiables[encoding_id].sha1
            )

        files = []

        if encoding_id in granule1.identifiables:
            if granule1.identifiables[encoding_id].sha1:
                files.append('%s.hdf5' % granule1.identifiables[encoding_id].sha1)
        if encoding_id in granule2.identifiables:
            if granule2.identifiables[encoding_id].sha1:
                files.append('%s.hdf5' % granule2.identifiables[encoding_id].sha1)

        element_count_id = DefinitionTree.get(definition,'%s.element_count_id' % definition.data_stream_id)
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
                    granule1.identifiables[k] = v
                else:
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



        return {
            'granule':granule1,
            'files':[pair1, pair2]
        }




    @staticmethod
    def _list_data(definition, granule):
        '''
        @brief Lists all the fields in the granule based on the Stream Definition
        @param definition Stream Definition
        @param granule Stream Granule
        @return dict of field_id : values_path for each field_id that exists
        '''
        from interface.objects import StreamDefinitionContainer, StreamGranuleContainer, RangeSet, CoordinateAxis
        assert isinstance(definition, StreamDefinitionContainer), 'object is not a definition.'
        assert isinstance(granule, StreamGranuleContainer), 'object is not a granule. its a %s' % type(granule)
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
        @brief Merges all the granules and datasets into one large dataset (Union)
        @param msgs raw granules from couch
        @return complete dataset
        @description
             n
        D := U [ msgs_i ]
            i=0
        '''
        granule = None
        file_list = list()
        count = len(msgs)
        used_vals = list()

        #-------------------------------------------------------------------------------------
        # Merge each granule to another granule one by one.
        # After each merge operation keep track of what files belong where on the timeline
        #-------------------------------------------------------------------------------------


        for i in xrange(count):
            if i==0:
                granule = msgs[0]['granule']
                psc = PointSupplementConstructor(point_definition=self.definition)

                res = ReplayProcess.merge_granule(definition=self.definition, granule1=granule, granule2=None)
                granule = res['granule']
                file_pair = res['files']
                log.debug('file_pair: %s', file_pair)

                if file_pair[0] not in file_list and file_pair[0][0] not in used_vals:
                    file_list.append( tuple(file_pair[0]))
                    used_vals.append(file_pair[0][0])


            else:
                res = ReplayProcess.merge_granule(definition=self.definition, granule1=granule, granule2=msgs[i]['granule'])

                granule = res['granule']
                file_pair = res['files']
                log.debug('file_pair: %s', file_pair)

                if file_pair[0] not in file_list and file_pair[0][0] not in used_vals:
                    file_list.append( tuple(file_pair[0]))
                    used_vals.append(file_pair[0][0])
                if file_pair[1] not in file_list and file_pair[1][0] not in used_vals:
                    file_list.append(tuple(file_pair[1]))
                    used_vals.append(file_pair[1][0])

        if not granule:
            return
        log.debug('file_list: %s', file_list)
        #-------------------------------------------------------------------------------------
        # Order the lists using a stable sort from python (by the first value in the tuples
        # Then peel off just the file names
        # Then get the appropriate URL for the file using FileSystem
        #-------------------------------------------------------------------------------------
        file_list.sort()
        file_list = list(i[1] for i in file_list)
        file_list = list([FileSystem.get_hierarchical_url(FS.CACHE, '%s' % i) for i in file_list])

        pairs = self._pair_up(granule)
        var_names = list([i[0] for i in pairs])

        record_count = granule.identifiables[self.element_count_id].value
        codec = HDFEncoder()
        log.debug('acquire_data:')
        log.debug('\tfile_list: %s', file_list)
        log.debug('\tfields: %s', var_names)
        log.debug('\trecords: %s', record_count)

        data = acquire_data(file_list, var_names, record_count).next()

        for row,value in data.iteritems():
            value_path = self._find_vp(pairs,row)
            codec.add_hdf_dataset(value_path,nparray=value['values'])
            #-------------------------------------------------------------------------------------
            # Debugging
            #-------------------------------------------------------------------------------------
            log.debug('row: %s', row)
            log.debug('value path: %s', value_path)
            log.debug('value: %s', value['values'])

        hdf_string = codec.encoder_close()
        self._patch_granule(granule,hdf_string)
        return granule

    def _patch_granule(self, granule, hdf_string):
        '''
        @brief Adds the hdf_string and sha1 to the granule
        @param granule Stream Granule
        @param hdf_string string consisting of raw bytes from an hdf5 file
        '''
        granule.identifiables[self.data_stream_id].values = hdf_string
        granule.identifiables[self.encoding_id].sha1 = hashlib.sha1(hdf_string).hexdigest().upper()


    def time_subset(self, granule, time_bounds):
        '''
        @brief Obtains a subset of the granule dataset based on the specified time_bounds
        @param granule Dataset
        @param time_bounds tuple consisting of a lower and upper bound
        @return A subset of the granule's dataset based on the time boundaries.
        '''
        assert isinstance(granule, StreamGranuleContainer), 'object is not a granule.'
        lower = time_bounds[0]-1
        upper = time_bounds[1]
        granule = self._slice(granule, slice(lower,upper))
        return granule



    def _get_time_index(self, granule, timeval):
        '''
        @brief Obtains the index where a time's value is
        @param granule must be a complete dataset (hdf_string provided)
        @param timeval the vector value
        @return Index value for timeval or closest approx such that timeval is IN the subset
        '''
        assert isinstance(granule, StreamGranuleContainer), 'object is not a granule.'
        assert granule.identifiables[self.data_stream_id].values, 'hdf_string is not provided.'

        hdf_string = granule.identifiables[self.data_stream_id].values
        file_path = self._get_hdf_from_string(hdf_string)

        #-------------------------------------------------------------------------------------
        # Determine the field_id for the temporal coordinate vector (aka time)
        #-------------------------------------------------------------------------------------

        time_field = self.definition.identifiables[self.time_id].coordinate_ids[0]
        value_path = granule.identifiables[time_field].values_path or self.definition.identifiables[time_field].values_path
        record_count = granule.identifiables[self.element_count_id].value

        #-------------------------------------------------------------------------------------
        # Go through the time vector and get the indexes that correspond to the timeval
        # It will find a value such that
        # t_n <= i < t_(n+1), where i is the index
        #-------------------------------------------------------------------------------------


        var_name = value_path.split('/').pop()
        res = acquire_data([file_path], [var_name], record_count).next()
        time_vector = res[var_name]['values']
        retval = 0
        for i in xrange(len(time_vector)):
            if time_vector[i] == timeval:
                retval = i
                break
            elif i==0 and time_vector[i] > timeval:
                retval = i
                break
            elif (i+1) < len(time_vector): # not last val
                if time_vector[i] < timeval and time_vector[i+1] > timeval:
                    retval = i
                    break
            else: # last val
                retval = i
                break
        FileSystem.unlink(file_path)
        return retval

    def _get_hdf_from_string(self, hdf_string):
        '''
        @param hdf_string binary string consisting of an HDF5 file.
        @return temporary file (full path) where the string was written to.
        @note client's responsible to unlink when finished.
        '''
        f = FileSystem.mktemp()
        f.write(hdf_string)
        retval = f.name
        f.close()
        return retval


    def subset(self,granule,coverages):
        '''
        @param granule
        @return dataset subset based on the fields
        '''
        assert isinstance(granule, StreamGranuleContainer), 'object is not a granule.'
        field_ids = self.field_ids
        element_count_id = self.element_count_id


        values_path = list()
        domain_ids = list()
        coverage_ids = list()
        coverages = list(coverages)
        log.debug('Coverages include %s of type %s', coverages, type(coverages))
        #-----------------------------------------------------------------------------------------------------------
        # Iterate through the fields IAW stream definition and check for rangesets and coordinate axises
        #  - If its a coordinate axis, it belongs regardless of what the client desires. (It's part of the domain)
        #  - If its a rangeset make sure that it's part of what the client asked for, if not discard it
        #-----------------------------------------------------------------------------------------------------------


        for field_id in field_ids:

            range_id = self.definition.identifiables[field_id].range_id

            #-------------------------------------------------------------------------------------
            # Coordinate Axis
            # - Keep track of this in our domains
            # - Add it to the paths we need to grab from the file(s)
            #-------------------------------------------------------------------------------------

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

            #-------------------------------------------------------------------------------------
            # Range Set
            # - If it's part of the coverages we want to keep
            #   - Add it to the list of ranges we're tracking
            #   - Add the value path to the paths we're tracking.
            #-------------------------------------------------------------------------------------


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

                # ----
                # We need to track the range and bounds because,
                # you guessed it, we need to update the bounds
                # ----

                range_id = self.definition.identifiables[field_id].range_id
                bounds_id = self.definition.identifiables[range_id].bounds_id


                #---
                # Lastly, if the field is there and we don't want it, we need to strip it
                #---

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

        file_path = self._get_hdf_from_string(granule.identifiables[self.data_stream_id].values)
        full_coverage = list(domain_ids + coverage_ids)

        log.debug('Full coverage: %s' % full_coverage)
        log.debug('Calling acquire_data with: %s, %s, %s', [file_path],values_path,granule.identifiables[element_count_id].value)

        codec = HDFEncoder()

        pairs = self._pair_up(granule)
        var_names = list([i[0] for i in pairs])

        record_count = granule.identifiables[self.element_count_id].value
        data = acquire_data([file_path], var_names, record_count).next()
        for row,value in data.iteritems():
            vp = self._find_vp(pairs, row)
            codec.add_hdf_dataset(vp, value['values'])

        hdf_string = codec.encoder_close()
        self._patch_granule(granule,hdf_string)

        FileSystem.unlink(file_path)

        return granule