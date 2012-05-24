#!/usr/bin/env python

'''
@package ion.services.dm.ingestion
@file ion/services/dm/ingestion/ingestion_worker.py
@author Swarbhanu Chatterjee
@brief IngestionWorker Class. An instance of this class acts as an ingestion worker. It is used to store incoming packets
to couchdb datastore and hdf datastore.
'''

from interface.objects import Granule, CompoundGranule
from pyon.datastore.datastore import DataStore
from pyon.public import log
from pyon.ion.transform import TransformDataProcess
from pyon.util.async import spawn
from pyon.core.exception import IonException

from pyon.core.interceptor.encode import encode_ion
import msgpack
from pyon.core.object import ion_serializer
from pyon.datastore.couchdb.couchdb_datastore import sha1hex
from interface.objects import DatasetIngestionTypeEnum, Coverage, CountElement
from pyon.core.exception import BadRequest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.event.event import EventSubscriber, EventPublisher

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
    Simple implementation of ingestion worker to park Construction period Granules on Disk
    """


    def dataset_configs_event_test_hook(self, msg, headers):
        pass

    def ingest_process_test_hook(self,msg, headers):
        pass

    def on_init(self):
        self.event_pub = EventPublisher()

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
        self.db = self.container.datastore_manager.get_datastore(ds_name=self.datastore_name, profile = self.datastore_profile, config = self.CFG)

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
                    log.info('Tried to remove dataset config that does not exist!')
            else:
                self.dataset_configs[stream_id] = event_msg

            # Hook to override just before processing is complete
            self.dataset_configs_event_test_hook(event_msg, headers)


        #Start the event subscriber - really - what a mess!
        self.event_subscriber = EventSubscriber(
            event_type="DatasetIngestionConfigurationEvent",
            origin=self.ingest_config_id,
            callback=receive_dataset_config_event
        )

        self.gl = spawn(self.event_subscriber.listen)
        self.event_subscriber._ready_event.wait(timeout=5)

        log.info(str(self.db))

    def process(self, packet):
        """Process incoming data!!!!
        """

        if not isinstance(packet, Granule):
            log.info('Received a packet that is not a new granule!')
            return

        # Get the dataset config for this stream
        dset_config = self.get_dataset_config(packet)

        # Process the packet

        ingest_attributes = self.process_stream(packet, dset_config)


        #@todo - get this data from the dataset config...
        if dset_config:
            dataset_id = dset_config.dataset_id
            stream_id = dset_config.stream_id

            self.event_pub.publish_event(event_type="GranuleIngestedEvent", sub_type="DatasetIngest",
                origin=dataset_id, status=200,
                ingest_attributes=ingest_attributes, stream_id=stream_id)


            headers = ''
            # Hook to override just before processing is complete
            self.ingest_process_test_hook(packet, headers)


    def persist_immutable(self, obj):
        """
        This method is not functional yet - the doc object is python specific. The sha1 must be of a language independent form.
        """

        return self.db.create_doc(obj)


    def process_stream(self, packet, dset_config):
        """
        Accepts a stream. Also accepts instruction (a dset_config). According to the received dset_config it processes the
        stream such as store in hfd_storage, couch_storage.
        @param: packet The incoming data stream of type stream.
        @param: dset_config The dset_config telling this method what to do with the incoming data stream.
        """


        ingestion_attributes={'variables':[], 'number_of_records':-1,'updated_metadata':False, 'updated_data':False}

        if dset_config is None:
            log.info('No dataset config for this stream!')
            return


        # Get back to the serialized form - the process receives only the IonObject after the interceptor stack has decoded it...
        simple_dict = ion_serializer.serialize(packet) #packet is an ion_object
        byte_string = msgpack.packb(simple_dict, default=encode_ion)

        encoding_type = 'ion_msgpack'
        calculated_sha1 = hashlib.sha1(byte_string).hexdigest().upper()


        self.persist_immutable({'stream_id':dset_config.stream_id,
                                'dataset_id':dset_config.dataset_id,
                                'persisted_sha1':calculated_sha1,
                                'encoding_type':encoding_type})




        filename = FileSystem.get_hierarchical_url(FS.CACHE, calculated_sha1, ".%s" % encoding_type)

        with open(filename, mode='wb') as f:
            f.write(byte_string)
            f.close()


        return ingestion_attributes

    def on_stop(self):
        TransformDataProcess.on_stop(self)

        # close event subscriber safely
        self.event_subscriber.close()
        self.gl.join(timeout=5)
        self.gl.kill()

        self.db.close()

    def on_quit(self):
        TransformDataProcess.on_quit(self)

        # close event subscriber safely
        self.event_subscriber.close()
        self.gl.join(timeout=5)
        self.gl.kill()

        self.db.close()



    def get_dataset_config(self, incoming_packet):
        """
        Gets the dset_config for the data stream
        """

        try:
            # For now - use the data_producer_id field as a stream_id to get us moving
            #@todo fix the missmatch between producer_id and stream_id!
            stream_id = incoming_packet.data_producer_id
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
