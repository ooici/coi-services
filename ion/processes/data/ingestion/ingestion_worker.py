#!/usr/bin/env python

'''
@package ion.services.dm.ingestion
@file ion/services/dm/ingestion/ingestion_worker.py
@author Swarbhanu Chatterjee
@brief IngestionWorker Class. An instance of this class acts as an ingestion worker. It is used to store incoming packets
to couchdb datastore and hdf datastore.
'''

from interface.objects import DataStream, StreamGranuleContainer, Encoding
from pyon.datastore.datastore import DataStore
from pyon.public import log
from pyon.ion.transform import TransformDataProcess
from pyon.util.async import spawn
from pyon.core.exception import IonException

from pyon.datastore.couchdb.couchdb_datastore import sha1hex
from interface.objects import BlogPost, BlogComment, DatasetIngestionByStream, DatasetIngestionTypeEnum
from pyon.core.exception import BadRequest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.event.event import DatasetIngestionConfigurationEventSubscriber

from pyon.util.file_sys import FS, FileSystem
import hashlib

class IngestionWorkerException(IonException):
    """
    Exception class for IngestionManagementService exceptions. This class inherits from IonException
    and implements the __str__() method.
    """
    def __str__(self):
        return str(self.get_status_code()) + str(self.get_error_message())


class IngestionWorker(TransformDataProcess):
    """
    Instances of this class acts as Ingestion Workers. They receive packets and send them to couchdb datastore or
    hdf storage according to the policy in the data stream or the default policy of the ingestion configuration
    """


    def dataset_configs_event_test_hook(self, msg, headers):
        pass

    def ingest_process_test_hook(self,msg, headers):
        pass

    def on_start(self):
        super(IngestionWorker,self).on_start()
        #----------------------------------------------
        # Start up couch
        #----------------------------------------------


        self.couch_config = self.CFG.get('couch_storage')
        self.hdf_storage = self.CFG.get('hdf_storage')

        self.number_of_workers = self.CFG.get('number_of_workers')
        self.description = self.CFG.get('description')

        self.ingest_config_id = self.CFG.get('configuration_id')

        self.datastore_name = self.couch_config.get('datastore_name',None) or 'dm_datastore'
        try:
            self.datastore_profile = getattr(DataStore.DS_PROFILE, self.couch_config.get('datastore_profile','SCIDATA'))
        except AttributeError:
            log.exception('Invalid datastore profile passed to ingestion worker. Defaulting to SCIDATA')

            self.datastore_profile = DataStore.DS_PROFILE.SCIDATA
        log.debug('datastore_profile %s' % self.datastore_profile)
        self.db = self.container.datastore_manager.get_datastore(self.datastore_name, self.datastore_profile, self.CFG)

        self.resource_reg_client = ResourceRegistryServiceClient(node = self.container.node)


        self.dataset_configs = {}
        # update the policy
        def receive_dataset_config_event(event_msg, headers):
            log.info('Updating dataset config in ingestion worker: %s', event_msg)

            if event_msg.type != DatasetIngestionTypeEnum.DATASETINGESTIONBYSTREAM:
                raise IngestionWorkerException('Received invalid type in dataset config event.')

            stream_id = event_msg.configuration.stream_id

            if event_msg.deleted:
                try:
                    del self.dataset_configs[stream_id]
                except KeyError:
                    log.warn('Tried to remove dataset config that does not exist!')
            else:
                self.dataset_configs[stream_id] = event_msg

            # Hook to override just before processing is complete
            self.dataset_configs_event_test_hook(event_msg, headers)


        #Start the event subscriber - really - what a mess!
        self.event_subscriber = DatasetIngestionConfigurationEventSubscriber(
            node = self.container.node,
            origin=self.ingest_config_id,
            callback=receive_dataset_config_event
            )

        self.gl = spawn(self.event_subscriber.listen)
        self.event_subscriber._ready_event.wait(timeout=5)

        log.warn(str(self.db))

    def process(self, packet):
        """Process incoming data!!!!
        """

        # Get the dataset config for this stream
        dset_config = self.get_dataset_config(packet)

        # Process the packet
        self.process_stream(packet, dset_config)

        headers = ''
        # Hook to override just before processing is complete
        self.ingest_process_test_hook(packet, headers)


    def persist_immutable(self, obj):
        """
        This method is not functional yet - the doc object is python specific. The sha1 must be of a language independent form.
        """
        doc = self.db._ion_object_to_persistence_dict(obj)
        sha1 = sha1hex(doc)

        try:
            self.db.create_doc(doc, object_id=sha1)
        except BadRequest:
            # Deduplication in action!
            #@TODO why are we getting so many duplicate comments?
            log.exception('Failed to write packet!\n%s' % obj)

        # Do the id or revision have a purpose? do we need a return value?


    def process_stream(self, packet, dset_config):
        """
        Accepts a stream. Also accepts instruction (a dset_config). According to the received dset_config it processes the
        stream such as store in hfd_storage, couch_storage.
        @param: packet The incoming data stream of type stream.
        @param: dset_config The dset_config telling this method what to do with the incoming data stream.
        """

        # Ignoring is_replay attribute now that we have a policy implementation
        if isinstance(packet, StreamGranuleContainer):

            if dset_config is None:
                log.warn('No dataset config for this stream!')
                return



            hdfstring = ''
            sha1 = ''

            for key,value in packet.identifiables.iteritems():
                if isinstance(value, DataStream):
                    hdfstring = value.values
                    value.values=''

                elif isinstance(value, Encoding):
                    sha1 = value.sha1



            if dset_config.archive_metadata is True:
                log.debug("Persisting data....")
                self.persist_immutable(packet )

            if dset_config.archive_data is True:
                #@todo - grab the filepath to save the hdf string somewhere..

                if hdfstring:

                    calculated_sha1 = hashlib.sha1(hdfstring).hexdigest().upper()

                    filename = FileSystem.get_url(FS.CACHE, calculated_sha1, ".hdf5")

                    if sha1 != calculated_sha1:
                        raise  IngestionWorkerException('The sha1 stored is different than the calculated from the received hdf_string')

                    log.warn('writing to filename: %s' % filename)

                    with open(filename, mode='wb') as f:
                        f.write(hdfstring)
                        f.close()
                else:
                    log.warn("Nothing to write!")


        elif isinstance(packet, BlogPost) and not packet.is_replay:
            self.persist_immutable(packet )


        elif isinstance(packet, BlogComment) and not packet.is_replay:
            self.persist_immutable(packet)

        # Create any events for about the receipt of an update on this stream


    def on_stop(self):
        TransformDataProcess.on_stop(self)
        self.gl.kill()
        self.db.close()

    def on_quit(self):
        TransformDataProcess.on_quit(self)
        self.gl.kill()
        self.db.close()



    def get_dataset_config(self, incoming_packet):
        """
        Gets the dset_config for the data stream
        """

        try:
            stream_id = incoming_packet.stream_resource_id
        except AttributeError:
            log.info('Packet does not have a data_stream_id: using default policy')
            return None


        dset_config = self.dataset_configs.get(stream_id, None)

        configuration = None
        if dset_config is None:
            log.info('No config found for stream id: %s ' % stream_id)
        else:
            log.info('Got config: %s for stream id: %s' % (dset_config, stream_id))
            configuration = dset_config.configuration

        # return the extracted instruction
        return configuration
