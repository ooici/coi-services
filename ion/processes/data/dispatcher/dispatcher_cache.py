#!/usr/bin/env python

'''
@package ion.services.dm.dispatcher
@file ion/services/dm/dispatcher/dispatcher_cache.py
@author Swarbhanu Chatterjee
@brief DispatcherCache Class. An instance of this class acts as an dispatcher cache. It is used to store incoming packets
to couchdb datastore and hdf datastore.
'''

from interface.objects import DataStream, StreamGranuleContainer, Encoding
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


class DispatcherCache(TransformDataProcess):
    """
    Instances of this class acts as Ingestion Workers. They receive packets and send them to couchdb datastore or
    hdf storage according to the policy in the data stream or the default policy of the dispatcher configuration
    """



    def on_start(self):
        super(DispatcherCache,self).on_start()

        self.datastore_name = self.CFG.get_safe('process.datastore_name','dispatcher_cache')
        try:
            self.datastore_profile = getattr(DataStore.DS_PROFILE, self.CFG.get_safe('datastore_profile','SCIDATA'))
        except AttributeError:
            log.exception('Invalid datastore profile passed to dispatcher cache. Defaulting to SCIDATA')
            self.datastore_profile = DataStore.DS_PROFILE.SCIDATA

        log.debug('datastore_profile %s' % self.datastore_profile)
        self.db = self.container.datastore_manager.get_datastore(ds_name=self.datastore_name, profile = self.datastore_profile)



    def process(self, packet):
        """Process incoming data!!!!
        """

        # Process the packet
        self.process_stream(packet)



    def persist_immutable(self, obj):
        """
        This method is not functional yet - the doc object is python specific. The sha1 must be of a language independent form.
        """
        doc = self.db._ion_object_to_persistence_dict(obj)
        sha1 = sha1hex(doc)

        try:
            self.db.create_doc(doc, object_id=sha1)
            log.debug('Persisted document %s', type(obj))
        except BadRequest:
            # Deduplication in action!
            #@TODO why are we getting so many duplicate comments?
            log.exception('Failed to write packet!\n%s' % obj)

        # Do the id or revision have a purpose? do we need a return value?


    def process_stream(self, packet):
        """
        Accepts a stream. Also accepts instruction (a dset_config). According to the received dset_config it processes the
        stream such as store in hfd_storage, couch_storage.
        @param: packet The incoming data stream of type stream.
        """

        # Ignoring is_replay attribute now that we have a policy implementation
        if isinstance(packet, StreamGranuleContainer):
            values_string = ''
            sha1 = ''
            encoding_type = ''
            # Check for a datastream and encoding
            for value in packet.identifiables.values():
                if isinstance(value, DataStream):
                    values_string = value.values
                    value.values=''
                elif isinstance(value, Encoding):
                    sha1 = value.sha1
                    encoding_type = value.encoding_type

            log.debug("Persisting data....")
            self.persist_immutable(packet )

            if values_string:
                calculated_sha1 = hashlib.sha1(values_string).hexdigest().upper()
                filename = FileSystem.get_url(FS.CACHE, calculated_sha1, ".%s" % encoding_type)

                if sha1 != calculated_sha1:
                    raise  DispatcherCacheException('The sha1 stored is different than the calculated from the received hdf_string')

                log.warn('writing to filename: %s' % filename)

                with open(filename, mode='wb') as f:
                    f.write(values_string)

            else:
                log.warn("Nothing to write!")


    def on_stop(self):
        TransformDataProcess.on_stop(self)

        self.db.close()

    def on_quit(self):
        TransformDataProcess.on_quit(self)

        self.db.close()


