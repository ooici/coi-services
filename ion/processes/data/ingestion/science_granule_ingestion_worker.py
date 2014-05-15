#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/processes/data/ingestion/science_granule_ingestion_worker.py
@date 06/26/12 11:38
@description Ingestion Process
'''
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule_utils import time_series_domain
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.core.exception import CorruptionError, NotFound, BadRequest
from pyon.ion.event import handle_stream_exception, EventPublisher
from pyon.ion.event import EventSubscriber
from pyon.public import log, RT, PRED, CFG, OT
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from interface.objects import Granule
from ion.core.process.transform import TransformStreamListener, TransformStreamProcess
from ion.util.time_utils import TimeUtils
from ion.util.stored_values import StoredValueManager
from interface.services.dm.iingestion_worker import BaseIngestionWorker
from pyon.ion.stream import StreamSubscriber
from gevent.coros import RLock
from gevent import event


from coverage_model.parameter_values import SparseConstantValue
from coverage_model import SparseConstantType
from coverage_model import NumpyParameterData, ConstantOverTime
from coverage_model.parameter_types import CategoryType

from ooi.timer import Timer, Accumulator
from ooi.logging import TRACE
from logging import DEBUG

import collections
import gevent
import time
import uuid
import numpy as np
from gevent.queue import Queue

numpy_walk = DatasetManagementService.numpy_walk

REPORT_FREQUENCY=100
MAX_RETRY_TIME=3600

class ScienceGranuleIngestionWorker(TransformStreamListener, BaseIngestionWorker):
    CACHE_LIMIT=CFG.get_safe('container.ingestion_cache',5)

    def __init__(self, *args,**kwargs):
        TransformStreamListener.__init__(self, *args, **kwargs)
        BaseIngestionWorker.__init__(self, *args, **kwargs)

        #--------------------------------------------------------------------------------
        # Ingestion Cache
        # - Datasets
        # - Coverage instances
        #--------------------------------------------------------------------------------
        self._datasets  = collections.OrderedDict()
        self._coverages = collections.OrderedDict()

        self._bad_coverages = {}

        self.time_stats = Accumulator(format='%3f')
        # unique ID to identify this worker in log msgs
        self._id = uuid.uuid1()



    def on_start(self): #pragma no cover
        #--------------------------------------------------------------------------------
        # Explicit on_start
        #--------------------------------------------------------------------------------

        # Skip TransformStreamListener and go to StreamProcess to avoid the subscriber being created
        # We want explicit management of the thread and subscriber object for ingestion

        TransformStreamProcess.on_start(self)
        
        self.queue_name = self.CFG.get_safe('process.queue_name',self.id)
        self.subscriber = StreamSubscriber(process=self, exchange_name=self.queue_name, callback=self.receive_callback)
        self.thread_lock = RLock()
        
        #--------------------------------------------------------------------------------
        # Normal on_start after this point
        #--------------------------------------------------------------------------------

        BaseIngestionWorker.on_start(self)
        self._rpc_server = self.container.proc_manager._create_listening_endpoint(from_name=self.id, process=self)
        self.add_endpoint(self._rpc_server)

        self.event_publisher = EventPublisher(OT.DatasetModified)
        self.stored_value_manager = StoredValueManager(self.container)

        self.lookup_docs = self.CFG.get_safe('process.lookup_docs',[])
        self.input_product = self.CFG.get_safe('process.input_product','')
        self.new_lookups = Queue()
        self.lookup_monitor = EventSubscriber(event_type=OT.ExternalReferencesUpdatedEvent, callback=self._add_lookups, auto_delete=True)
        self.add_endpoint(self.lookup_monitor)
        self.connection_id = ''
        self.connection_index = None
        
        self.start_listener()

    def on_quit(self): #pragma no cover
        self.event_publisher.close()
        if self.subscriber_thread:
            self.stop_listener()
        for stream, coverage in self._coverages.iteritems():
            try:
                coverage.close(timeout=5)
            except:
                log.exception('Problems closing the coverage')
        self._coverages.clear()
        TransformStreamListener.on_quit(self)
        BaseIngestionWorker.on_quit(self)

    
    def start_listener(self):
        # We use a lock here to prevent possible race conditions from starting multiple listeners and coverage clobbering
        with self.thread_lock:
            self.subscriber_thread = self._process.thread_manager.spawn(self.subscriber.listen, thread_name='%s-subscriber' % self.id)

    def stop_listener(self):
        # Avoid race conditions with coverage operations (Don't start a listener at the same time as closing one)
        with self.thread_lock:
            self.subscriber.close()
            self.subscriber_thread.join(timeout=10)
            for stream, coverage in self._coverages.iteritems():
                try:
                    coverage.close(timeout=5)
                except:
                    log.exception('Problems closing the coverage')
            self._coverages.clear()
            self.subscriber_thread = None

    def pause(self):
        if self.subscriber_thread is not None:
            self.stop_listener()


    def resume(self):
        if self.subscriber_thread is None:
            self.start_listener()


    def _add_lookups(self, event, *args, **kwargs):
        if event.origin == self.input_product:
            if isinstance(event.reference_keys, list):
                self.new_lookups.put(event.reference_keys)

    def _new_dataset(self, stream_id):
        '''
        Adds a new dataset to the internal cache of the ingestion worker
        '''
        rr_client = self.container.resource_registry
        datasets, _ = rr_client.find_subjects(subject_type=RT.Dataset,predicate=PRED.hasStream,object=stream_id,id_only=True)
        if datasets:
            return datasets[0]
        return None

    def _get_data_products(self, dataset_id):
        rr_client = self.container.resource_registry
        data_products, _ = rr_client.find_subjects(object=dataset_id, predicate=PRED.hasDataset, subject_type=RT.DataProduct, id_only=False)
        return data_products


    #--------------------------------------------------------------------------------
    # Metadata Handlers
    #--------------------------------------------------------------------------------

    def initialize_metadata(self, dataset_id, rdt):
        '''
        Initializes a metadata document in the object store. The document
        contains information about the bounds and extents of the dataset as
        well other metadata to improve performance.

        '''

        object_store = self.container.object_store
        key = dataset_id
        bounds = {}
        extents = {}
        last_values = {}
        rough_size = 0
        for k,v in rdt.iteritems():
            v = v[:].flatten()
            if v.dtype.char not in ('S', 'O', 'U', 'V'):
                bounds[k] = (np.min(v), np.max(v))
                last_values[k] = v[-1]
            extents[k] = len(rdt)
            rough_size += len(rdt) * 4

        doc = {'bounds':bounds, 'extents':extents, 'last_values':last_values, 'size': rough_size}
        doc = numpy_walk(doc)
        object_store.create_doc(doc, object_id=key)
        return 

    def update_metadata(self, dataset_id, rdt):
        '''
        Updates the metada document with the latest information available
        '''

        self.update_data_product_metadata(dataset_id, rdt)

        # Grab the document
        object_store = self.container.object_store
        key = dataset_id
        try:
            doc = object_store.read_doc(key)
        except NotFound:
            return self.initialize_metadata(dataset_id, rdt)
        # These are the fields we're interested in
        bounds = doc['bounds']
        extents = doc['extents']
        last_values = doc['last_values']
        rough_size = doc['size']
        for k,v in rdt.iteritems():
            if k not in bounds:
                continue

            v = v[:].flatten() # Get the numpy representation (dense array).
            if v.dtype.char not in ('S', 'O', 'U', 'V'):
                l_min = np.min(v)
                l_max = np.max(v)
                o_min, o_max = bounds[k]
                bounds[k] = (min(l_min, o_min), max(l_max, o_max))
                last_values[k] = v[-1]
            # Update the bounds
            # Increase the extents
            extents[k] = extents[k] + len(rdt)
            # How about the last value?

            rough_size += len(rdt) * 4
            doc['size'] = rough_size
        # Sanitize it
        doc = numpy_walk(doc)
        object_store.update_doc(doc)

    def update_data_product_metadata(self, dataset_id, rdt):
        data_products = self._get_data_products(dataset_id)
        for data_product in data_products:
            self.update_time(data_product, rdt[rdt.temporal_parameter][:])
            self.update_geo(data_product, rdt)
            try:
                self.container.resource_registry.update(data_product)
            except: # TODO: figure out WHICH Exception gets raised here when the bounds are off
                log.error("Problem updating the data product metadata", exc_info=True)
                # Carry on :(



    def update_time(self, data_product, t):
        '''
        Sets the nominal_datetime for a data product correctly
        Accounts for things like NTP and out of order data
        '''

        t0, t1 = self.get_datetime_bounds(data_product)
        #TODO: Account for non NTP-based timestamps
        min_t = np.min(t) - 2208988800
        max_t = np.max(t) - 2208988800
        if t0:
            t0 = min(t0, min_t)
        else:
            t0 = min_t

        if t1:
            t1 = max(t1, max_t)
        else:
            t1 = max_t

        if t0 > t1:
            log.error("This should never happen but t0 > t1")

        data_product.nominal_datetime.start_datetime = float(t0)
        data_product.nominal_datetime.end_datetime = float(t1)

    def get_datetime(self, nominal_datetime):
        '''
        Returns a floating point value for the datetime or None if it's an
        empty string
        '''
        t = None
        # So normally this is a string
        if isinstance(nominal_datetime, (float, int)):
            t = nominal_datetime # simple enough
        elif isinstance(nominal_datetime, basestring):
            if nominal_datetime: # not an empty string
                # Try to convert it to a float
                try:
                    t = float(nominal_datetime)
                except ValueError:
                    pass
        return t

    def get_datetime_bounds(self, data_product):
        '''Returns the min and max for the bounds in the nominal_datetime
        attr
        '''
        
        t0 = self.get_datetime(data_product.nominal_datetime.start_datetime)
        t1 = self.get_datetime(data_product.nominal_datetime.end_datetime)
        return (t0, t1)


    def update_geo(self, data_product, rdt):
        '''
        Finds the maximum bounding box
        '''
        lat = None
        lon = None
        for p in rdt:
            if rdt._rd[p] is None:
                continue
            # TODO: Not an all encompassing list of acceptable names for lat and lon
            if p.lower() in ('lat', 'latitude', 'y_axis'):
                lat = np.asscalar(rdt[p][-1])
            elif p.lower() in ('lon', 'longitude', 'x_axis'):
                lon = np.asscalar(rdt[p][-1])
            if lat and lon:
                break

        if lat and lon:
            data_product.geospatial_bounds.geospatial_latitude_limit_north = lat
            data_product.geospatial_bounds.geospatial_latitude_limit_south = lat
            data_product.geospatial_bounds.geospatial_longitude_limit_east = lon
            data_product.geospatial_bounds.geospatial_longitude_limit_west = lon

    
    #--------------------------------------------------------------------------------
    # Cache managemnt
    #--------------------------------------------------------------------------------

    def get_dataset(self,stream_id):
        '''
        Memoization (LRU) of _new_dataset
        '''
        try:
            result = self._datasets.pop(stream_id)
        except KeyError:
            result = self._new_dataset(stream_id)
            if result is None:
                return None
            if len(self._datasets) >= self.CACHE_LIMIT:
                self._datasets.popitem(0)
        self._datasets[stream_id] = result
        return result

    def get_coverage(self, stream_id):
        '''
        Memoization (LRU) of _get_coverage
        '''
        try:
            result = self._coverages.pop(stream_id)
        except KeyError:
            dataset_id = self.get_dataset(stream_id)
            if dataset_id is None:
                return None
            result = DatasetManagementService._get_simplex_coverage(dataset_id, mode='a')
            if result is None:
                return None
            if len(self._coverages) >= self.CACHE_LIMIT:
                k, coverage = self._coverages.popitem(0)
                coverage.close(timeout=5)
        self._coverages[stream_id] = result
        return result


    #--------------------------------------------------------------------------------
    # Granule Parsing and Handling
    #--------------------------------------------------------------------------------


    @handle_stream_exception()
    def recv_packet(self, msg, stream_route, stream_id):
        '''
        The consumer callback to parse and manage the granule.
        The message is ACK'd once the function returns
        '''
        log.trace('received granule for stream %s', stream_id)

        if msg == {}:
            log.error('Received empty message from stream: %s', stream_id)
            return
        # Message validation
        if not isinstance(msg, Granule):
            log.error('Ingestion received a message that is not a granule: %s', msg)
            return


        rdt = RecordDictionaryTool.load_from_granule(msg)
        if rdt is None:
            log.error('Invalid granule (no RDT) for stream %s', stream_id)
            return
        if not len(rdt):
            log.debug('Empty granule for stream %s', stream_id)
            return

        self.persist_or_timeout(stream_id, rdt)

    def persist_or_timeout(self, stream_id, rdt):
        '''
        A loop that tries to parse and store a granule for up to five minutes,
        and waits an increasing amount of time each iteration.
        '''
        done = False
        timeout = 2
        start = time.time()
        while not done:
            if self.parse_granule(stream_id, rdt, start, done):
                return # We're all done, everything worked

            if (time.time() - start) > MAX_RETRY_TIME: # After a while, give up
                dataset_id = self.get_dataset(stream_id)
                log.error("We're giving up, the coverage needs to be inspected %s", DatasetManagementService._get_coverage_path(dataset_id))
                raise

            if stream_id in self._coverages:
                log.info('Popping coverage for stream %s', stream_id)
                self._coverages.pop(stream_id)

            gevent.sleep(timeout)

            timeout = min(60 * 5, timeout * 2)


    def parse_granule(self, stream_id, rdt, start, done):
        try:
            self.add_granule(stream_id, rdt)
            return True 
        except Exception as e:
            log.exception('An issue with coverage, retrying after a bit')
            return False
        return True # never reaches here, Added for clarity


    def dataset_changed(self, dataset_id, window):
        self.event_publisher.publish_event(origin=dataset_id, author=self.id, window=window)

    def build_data_dict(self, rdt):
        np_dict = {}
        
        time_array = rdt[rdt.temporal_parameter]
        if time_array is None:
            raise ValueError("A granule needs a time array")
        for k,v in rdt.iteritems():
            # Sparse values are different and aren't constructed using NumpyParameterData
            if isinstance(rdt.param_type(k), SparseConstantType):
                value = v[0]
                if hasattr(value, 'dtype'):
                    value = np.asscalar(value)
                time_start = np.asscalar(time_array[0])
                np_dict[k] = ConstantOverTime(k, value, time_start=time_start, time_end=None) # From now on
                continue
            elif isinstance(rdt.param_type(k), CategoryType):
                value = np.asarray(v, dtype='S')
            else:
                value = v

            try:
                np_dict[k] = NumpyParameterData(k, value, time_array)
            except:
                raise

        return np_dict

    def insert_values(self, coverage, rdt, stream_id):
        
        np_dict = self.build_data_dict(rdt)

        if 'ingestion_timestamp' in coverage.list_parameters():
            t_now = time.time()
            ntp_time = TimeUtils.ts_to_units(coverage.get_parameter_context('ingestion_timestamp').uom, t_now)
            ntp_time = np.ones_like(rdt[rdt.temporal_parameter]) * rdt[rdt.temporal_parameter]
            np_dict['ingestion_timestamp'] = NumpyParameterData('ingestion_timestamp', ntp_time, rdt[rdt.temporal_parameter])

        # If it's sparse only
        if self.sparse_only(rdt):
            del np_dict[rdt.temporal_parameter]
    

        try:
            coverage.set_parameter_values(np_dict)
        except IOError as e:
            log.error("Couldn't insert values for coverage: %s",
                      coverage.persistence_dir, exc_info=True)
            try:
                coverage.close()
            finally:
                self._bad_coverages[stream_id] = 1
                raise CorruptionError(e.message)
        except KeyError as e:
            if 'has not been initialized' in e.message:
                coverage.refresh()
            raise
        except Exception as e:
            print repr(rdt)
            raise

    
    def add_granule(self,stream_id, rdt):
        ''' Appends the granule's data to the coverage and persists it. '''
        if stream_id in self._bad_coverages:
            log.info('Message attempting to be inserted into bad coverage: %s',
                     DatasetManagementService._get_coverage_path(self.get_dataset(stream_id)))
            
        #--------------------------------------------------------------------------------
        # Coverage determiniation and appending
        #--------------------------------------------------------------------------------
        dataset_id = self.get_dataset(stream_id)
        if not dataset_id:
            log.error('No dataset could be determined on this stream: %s', stream_id)
            return

        try:
            coverage = self.get_coverage(stream_id)
        except IOError as e:
            log.error("Couldn't open coverage: %s",
                      DatasetManagementService._get_coverage_path(self.get_dataset(stream_id)))
            raise CorruptionError(e.message)

        if not coverage:
            log.error('Could not persist coverage from granule, coverage is None')
            return
        #--------------------------------------------------------------------------------
        # Actual persistence
        #--------------------------------------------------------------------------------

        if rdt[rdt.temporal_parameter] is None:
            log.warning("Empty granule received")
            return

        # Parse the RDT and set hte values in the coverage
        self.insert_values(coverage, rdt, stream_id)
        
        # Force the data to be flushed
        DatasetManagementService._save_coverage(coverage)

        self.update_metadata(dataset_id, rdt)

        try:
            window = rdt[rdt.temporal_parameter][[0,-1]]
            window = window.tolist()
        except (ValueError, IndexError):
            window = None
        self.dataset_changed(dataset_id, window)

    def sparse_only(self, rdt):
        '''
        A sparse only rdt will have only a time array AND sparse values, no other data
        '''
        if rdt[rdt.temporal_parameter] is None:
            return False # No time, so it's just empty

        at_least_one = False

        for key in rdt.iterkeys():
            # Skip time, that needs to be there
            if key == rdt.temporal_parameter:
                continue
            if not isinstance(rdt.param_type(key), SparseConstantType):
                return False
            else:
                at_least_one = True

        return at_least_one

