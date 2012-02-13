#!/usr/bin/env python

'''
@package ion.services.dm.ingestion
@file ion/services/dm/ingestion/ingestion_worker.py
@author Swarbhanu Chatterjee
@brief IngestionWorker Class. An instance of this class acts as an ingestion worker. It is used to store incoming packets
to couchdb datastore and hdf datastore.
'''

from interface.objects import DataContainer, DataStream

from pyon.datastore.datastore import DataStore, DatastoreManager
from pyon.public import log
from pyon.ion.transform import TransformDataProcess

from pyon.datastore.couchdb.couchdb_datastore import sha1hex
from interface.objects import BlogPost, BlogComment
from pyon.core.exception import BadRequest
from interface.objects import StreamIngestionPolicy
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient



class IngestionWorker(TransformDataProcess):
    """
    Instances of this class acts as Ingestion Workers. They receive packets and send them to couchdb datastore or
    hdf storage according to the policy in the data stream or the default policy of the ingestion configuration
    """

    def on_start(self):
        super(IngestionWorker,self).on_start()
        #----------------------------------------------
        # Start up couch
        #----------------------------------------------

        self.couch_config = self.CFG.get('couch_storage')
        self.hdf_storage = self.CFG.get('hdf_storage')
        self.default_policy = self.CFG.get('default_policy')
        self.number_of_workers = self.CFG.get('number_of_workers')
        self.description = self.CFG.get('description')
        self.datastore_name = self.couch_config['database'] or 'dm_datastore'
        self.db = self.container.datastore_manager.get_datastore(self.datastore_name, DataStore.DS_PROFILE.EXAMPLES, self.CFG)

        self.resource_reg_client = ResourceRegistryServiceClient(node = self.container.node)


        log.warn(str(self.db))


    def process(self, packet):
        """Process incoming data!!!!
        """

        # Get the policy for this stream
        policy = self.extract_policy_packet(packet)

        # Process the packet
        self.process_stream(packet, policy)


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


    def process_stream(self, packet, policy):
        """
        Accepts a stream. Also accepts instruction (a policy). According to the received policy it processes the
        stream such as store in hfd_storage, couch_storage.
        @param: incoming_stream The incoming data stream of type stream.
        @param: policy The policy telling this method what to do with the incoming data stream.
        """

        #@todo Evaluate policy for this stream and determine what to do.

        if isinstance(packet, DataContainer):
            for key,value in packet.identifiables.iteritems():
                if isinstance(value, DataStream):
                    hdfstring = value
                    packet.identifiables[key]=''
                    
            self.persist_immutable(packet )

        elif isinstance(packet, BlogPost) and not packet.is_replay:
            self.persist_immutable(packet )


        elif isinstance(packet, BlogComment) and not packet.is_replay:
            self.persist_immutable(packet)

        # Create any events for about the receipt of an update on this stream


    def on_stop(self):
        TransformDataProcess.on_stop(self)
        self.db.close()

    def on_quit(self):
        TransformDataProcess.on_quit(self)
        self.db.close()



    def extract_policy_packet(self, incoming_packet):
        """
        Extracts and returns the policy from the data stream
        """

        stream_id = incoming_packet.stream_id
        log.debug('Getting policy for stream id: %s' % stream_id)

        policy = StreamIngestionPolicy(**self.default_policy)

        try:
            # Check for stream specific policy object
            pass
#            policy = self.resource_reg_client.find_objects(incoming_packet, PRED.hasPolicy, RT.Policy, False)

            # Later this would be replaced with a notification and caching scheme
        except :
            # If there is not policy for this stream use the default policy for this Ingestion Configuration
            log.debug('No policy found for stream id: %s' % stream_id)

        # return the extracted instruction
        return policy
