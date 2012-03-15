#!/usr/bin/env python

'''
@package ion.services.dm.dispatcher
@file ion/services/dm/dispatcher/dispatcher_cache.py
@author Swarbhanu Chatterjee
@brief DispatcherCache Class. An instance of this class acts as an dispatcher cache. It is used to store incoming packets
to couchdb datastore and hdf datastore.
'''

from interface.objects import DataStream, StreamGranuleContainer, Encoding, DatasetIngestionByStream, DatasetIngestionConfiguration
from ion.processes.data.ingestion.ingestion_worker import IngestionWorker
from pyon.datastore.datastore import DataStore
from pyon.public import log
from pyon.ion.transform import TransformDataProcess
from pyon.util.async import spawn
from pyon.core.exception import IonException

from pyon.datastore.couchdb.couchdb_datastore import sha1hex
from interface.objects import DatasetIngestionTypeEnum
from pyon.core.exception import BadRequest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.event.event import EventSubscriber

from pyon.util.file_sys import FS, FileSystem
import hashlib

class DispatcherCacheException(IonException):
    """
    Exception class for IngestionManagementService exceptions. This class inherits from IonException
    and implements the __str__() method.
    """
    def __str__(self):
        return str(self.get_status_code()) + str(self.get_error_message())


class DispatcherCache(IngestionWorker):
    """
    Instances of this class acts as Ingestion Workers. They receive packets and send them to couchdb datastore or
    hdf storage according to the policy in the data stream or the default policy of the dispatcher configuration
    """



    def on_start(self):
        super(IngestionWorker,self).on_start()

        self.datastore_name = self.CFG.get_safe('process.datastore_name','dispatcher_cache')
        self.number = self.CFG.get_safe('process.number', 1)
        try:
            self.datastore_profile = getattr(DataStore.DS_PROFILE, self.CFG.get_safe('datastore_profile','SCIDATA'))
        except AttributeError:
            log.exception('Invalid datastore profile passed to dispatcher cache. Defaulting to SCIDATA')
            self.datastore_profile = DataStore.DS_PROFILE.SCIDATA

        log.debug('datastore_profile %s' % self.datastore_profile)
        self.db = self.container.datastore_manager.get_datastore(ds_name=self.datastore_name, profile = self.datastore_profile)

    def on_quit(self):
        self.db.close()
        super(IngestionWorker,self).on_quit()

    def on_stop(self):
        self.db.close()
        super(IngestionWorker,self).on_start()
    def get_dataset_config(self, incoming_packet):

        config = DatasetIngestionByStream(
            archive_data=True,
            archive_metadata=True,
            stream_id=incoming_packet.stream_resource_id
        )

        return config