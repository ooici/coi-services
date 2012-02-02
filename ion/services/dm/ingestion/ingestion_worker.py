#!/usr/bin/env python

'''
@package ion.services.dm.ingestion
@file ion/services/dm/ingestion/ingestion.py
@author Swarbhanu Chatterjee
@brief Ingestion Class. When instantiated the ingestion objects will be able to handle one specific scientific request.
The scientific request may involved several data subscriptions, retrievals, processing, and data publishing.
Uses the HDFEncoder and HDFDecoder classes to perform most of its work with the data and the PubSub messaging to
send data far and wide.
'''

# import statements
# import ScienceObject, ScienceObjectTransport, HDFEncoder, HDFDecoder, and the exceptions... i.e.
# everything in science_object_codec.py

from pyon.core.exception import NotFound
from pyon.public import RT, PRED, log, IonObject
from pyon.public import CFG, StreamProcess
from pyon.ion.endpoint import ProcessPublisher
from pyon.net.channel import SubscriberChannel
from pyon.container.procs import ProcManager
from pyon.core.exception import IonException
from pyon.ion.transform import TransformDataProcess

from pyon.datastore.couchdb.couchdb_dm_datastore import CouchDB_DM_DataStore
from interface.objects import BlogPost, BlogComment
from pyon.core.exception import BadRequest
from interface.objects import StreamIngestionPolicy

import time



class IngestionWorker(TransformDataProcess):
    """
    This Class creates an instance of a science object and science object transport
     to contain the data for use in inventory (interacts with the inventory as required).
    It defines a Transformation (interacts with the transformation management),
    defines a Datastream (interacts with the pubsub management as required), and
    defines Preservation (interacts with Preservation).

    Instances of this class acts as Ingestion Workers
    Each instance (i.e. Ingestion Worker) stores a list of configuration ids that it is working on.
    There is a refresh_config_id_store() method that deletes all configuration ids stored.
    Alternatively the instance can be killed and a new worker created.
    """

    def on_start(self):
        super(IngestionWorker,self).on_start()
        #----------------------------------------------
        # Start up couch
        #----------------------------------------------

        self.couch_config = self.CFG.get('couch_storage')
        self.hdf_storage = None # Add it here...
        #@TODO Add the rest of the config args as properties of the instance

        self.db = CouchDB_DM_DataStore(host=self.couch_config['server'], datastore_name = self.couch_config['database'])


        log.warn(str(self.db))

        # Create dm_datastore if it does not exist already
        try:
            self.db.create_datastore()
        except BadRequest:
            print 'Already exists'

    def process(self, packet):
        """Processes incoming data!!!!
        """

        # Get the policy for this stream
        policy = self.extract_policy_packet(packet)

        # Process the packet
        self.process_stream(packet, policy)

    def process_stream(self, packet, policy):
        """
        Accepts a stream. Also accepts instruction as a string, and according to what is contained in the instruction,
        it does things with the stream such as store in hfd_storage, couch_storage or process the data and return the
        output.
        @param: incoming_stream The incoming data stream of type stream.
        @param: policy The policy telling this method what to do with the incoming data stream.
        """

        # Evaluate policy for this stream and determine what to do.

        if isinstance(packet, BlogPost):
            db_post_id, db_post_rev = self.db.create(packet, None )


        if isinstance(packet, BlogComment):
            db_comment_id, db_comment_rev = self.db.create(packet, None)

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

        policy = StreamIngestionPolicy()
        try:
            #@TODO add a resource client so we can get the policy from the resource registry.
            # Later this would be replaced with a notification and caching scheme
            pass
        except NotFound:
            # If there is not policy for this stream use the default policy
            log.debug('No policy found for stream id: %s' % stream_id)

        # return the extracted instruction
        return policy
