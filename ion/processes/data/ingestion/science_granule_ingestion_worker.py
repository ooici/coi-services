#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ingestion_worker_b
@date 06/26/12 11:38
@description DESCRIPTION
'''
from pyon.core.bootstrap import get_sys_name
from pyon.core.interceptor.encode import encode_ion
from pyon.core.object import ion_serializer
from pyon.ion.process import SimpleProcess
from pyon.net.endpoint import Subscriber
from pyon.datastore.datastore import DataStore
from pyon.util.arg_check import validate_is_instance
from pyon.util.containers import get_ion_ts
from pyon.util.file_sys import FileSystem, FS
from pyon.public import log
from interface.objects import Granule
from gevent import spawn
import hashlib
import msgpack
import re



class ScienceGranuleIngestionWorker(SimpleProcess):
    def on_start(self): #pragma no cover
        self.queue_name = self.CFG.get_safe('processes.queue_name','ingestion_queue')
        self.datastore_name = self.CFG.get_safe('processes.datastore_name', 'datasets')

        self.subscriber = Subscriber(name=(get_sys_name(), self.queue_name), callback=self.consume)
        self.db = self.container.datastore_manager.get_datastore(self.datastore_name, DataStore.DS_PROFILE.SCIDATA)
        self.greenlet = spawn(self.subscriber.listen)

    def on_quit(self): #pragma no cover
        self.subscriber.close()
        self.greenlet.join(timeout=10)

    def consume(self, msg, headers):
        stream_id = headers['routing_key']
        stream_id = re.sub(r'\.data', '', stream_id)
        self.ingest(msg,stream_id)

    def ingest(self, msg, stream_id):
        if msg == {}:
            return
        validate_is_instance(msg,Granule,'Incoming message is not compatible with this ingestion worker')
        simple_dict = ion_serializer.serialize(msg)
        byte_string = msgpack.packb(simple_dict, default=encode_ion)

        encoding_type = 'ion_msgpack'

        calculated_sha1 = hashlib.sha1(byte_string).hexdigest().upper()

        dataset_granule = {
           'stream_id'      : stream_id,
           'dataset_id'     : msg.data_producer_id,
           'persisted_sha1' : encoding_type,
           'ts_create'      : get_ion_ts()
        }
        self.persist(dataset_granule)

        filename = FileSystem.get_hierarchical_url(FS.CACHE, calculated_sha1, ".%s" % encoding_type)

        self.write(filename,byte_string)



    def persist(self, dataset_granule):
        self.db.create_doc(dataset_granule)

    def write(self,filename, data): #pragma no cover
        try:
            with open(filename, mode='wb') as f:
                f.write(data)
                f.close()
        except Exception as e:
            log.error('Problem writing to disk: %s' , e.message)




           

