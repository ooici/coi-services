#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file replay_process_a
@date 06/14/12 13:31
@description DESCRIPTION
'''


from interface.services.dm.ireplay_process import BaseReplayProcess
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from pyon.core.exception import BadRequest, IonException
from pyon.util.file_sys import FileSystem,FS
from pyon.core.interceptor.encode import decode_ion
from pyon.ion.granule import combine_granules
from pyon.core.object import IonObjectDeserializer
from pyon.core.bootstrap import get_obj_registry
from gevent.event import Event
from pyon.public import log
import msgpack
import gevent


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
        self.deserializer = IonObjectDeserializer(obj_registry=get_obj_registry())

    def on_start(self):
        super(ReplayProcess,self).on_start()
        dsm_cli = DatasetManagementServiceClient()

        self.dataset_id      = self.CFG.get_safe('process.dataset_id', None)
        self.delivery_format = self.CFG.get_safe('process.delivery_format',{})
        self.start_time      = self.CFG.get_safe('process.query.start_time', None)
        self.end_time        = self.CFG.get_safe('process.query.end_time', None)
        self.publishing      = Event()

        if self.dataset_id is None:
            raise BadRequest('dataset_id not specified')

        self.dataset = dsm_cli.read_dataset(self.dataset_id)


    def execute_retrieve(self):
        datastore = self.container.datastore_manager.get_datastore(self.dataset.datastore_name)
        view_name = 'manifest/by_dataset'

        opts = dict(
            start_key = [self.dataset.primary_view_key, 0],
            end_key   = [self.dataset.primary_view_key, {}],
            include_docs = True
        )
        if self.start_time is not None:
            opts['start_key'][1] = self.start_time

        if self.end_time is not None:
            opts['end_key'][1] = self.end_time
        granules = []
        #--------------------------------------------------------------------------------
        # Gather all the dataset granules and compile the FS cache
        #--------------------------------------------------------------------------------
        log.debug('Getting data from datastore')
        for result in datastore.query_view(view_name,opts=opts):
            doc = result.get('doc')
            if doc is not None:
                log.debug(doc)
                sha1 = doc.get('persisted_sha1')
                encoding = doc.get('encoding_type')
                # Warning: redundant serialization
                byte_string = self.read_persisted_cache(sha1,encoding)
                obj = msgpack.unpackb(byte_string, object_hook=decode_ion)
                ion_obj = self.deserializer.deserialize(obj) # Problem here is that nested objects get deserialized
                granules.append(ion_obj)

        while len(granules) > 1:
            granule = combine_granules(granules.pop(0),granules.pop(0))
            granules.insert(0,granule)
        if granules:
            return granules[0]
        return None

    def execute_replay(self):
        if self.publishing.is_set():
            return False
        gevent.spawn(self.replay)
        return True

    def replay(self):
        self.publishing.set() # Minimal state, supposed to prevent two instances of the same process from replaying on the same stream
        datastore = self.container.datastore_manager.get_datastore(self.dataset.datastore_name)
        view_name = 'manifest/by_dataset'

        opts = dict(
            start_key = [self.dataset.primary_view_key, 0],
            end_key   = [self.dataset.primary_view_key, {}],
            include_docs = True
        )
        if self.start_time is not None:
            opts['start_key'][1] = self.start_time

        if self.end_time is not None:
            opts['end_key'][1] = self.end_time

        #--------------------------------------------------------------------------------
        # Gather all the dataset granules and compile the FS cache
        #--------------------------------------------------------------------------------
        for result in datastore.query_view(view_name,opts=opts):
            log.debug(result)
            doc = result.get('doc')
            if doc is not None:
                sha1 = doc.get('persisted_sha1')
                encoding = doc.get('encoding_type')
                # Warning: redundant serialization
                byte_string = self.read_persisted_cache(sha1,encoding)
                obj = msgpack.unpackb(byte_string, object_hook=decode_ion)
                ion_obj = self.deserializer.deserialize(obj)

                self.output.publish(ion_obj)

        # Need to terminate the stream, null granule = {}
        self.output.publish({})
        self.publishing.clear()
        return True


    def read_persisted_cache(self, sha1, encoding):
        byte_string = None
        path = FileSystem.get_hierarchical_url(FS.CACHE,sha1,'.%s' % encoding)
        try:
            with open(path, 'r') as f:
                byte_string = f.read()
        except IOError as e:
            raise BadRequest('(%s)\n\t\tIOProblem: %s' % (e.message,path))
        return byte_string



