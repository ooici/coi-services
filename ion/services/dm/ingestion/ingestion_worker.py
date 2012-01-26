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

from ion.services.dm.ingestion.ingestion_management_service import IngestionManagementService

class IngestionWorker(object):
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

    def find_ingestion_configuration_from_id(self, ingestion_configuration_id = ''):
        """
        Returns an ingestion_configuration when provided an ingestion_configuration_id
        """
        ingestion_configuration = self.clients.resource_registry.read(ingestion_configuration_id)

        return ingestion_configuration

    def process_stream(self, ingestion_configuration_id = '', incoming_stream = '', instruction = ''):
        """
        Accepts a stream. Also accepts instruction as a string, and according to what is contained in the instruction,
        it does things with the stream such as store in hfd_storage, couch_storage or process the data and return the
        output.
        @param: ingestion_configuration_id The id for the configuration of type string.
        @param: incoming_stream The incoming data stream of type stream.
        @param: instruction The instruction telling this method what to do with the incoming data stream.

        @return output_stream: The output stream. This may or may not be returned depending on what the instruction says.
        """

        # Do something for the preservation of the object

        configuration = self.find_ingestion_configuration_from_id(ingestion_configuration_id)

        # do things with the data stream

        # store stuff in couch datastore or in hdf datastore according to the instruction and the configuration
        # specifications


        # return the output stream if that is what the instruction says
        send_output_stream = True # change this line

        if send_output_stream:
            return output_stream
        else:
            return None

    def work_on_datastream(self, ingestion_configuration_id, incoming_stream):
        """
        This method will be called by Transformation.

        The method will be used to work on an incoming stream, and extract the instruction regarding what to do with
        the datastream. According to the instruction, it will process the datastream and/or send output to whatever is
        specified by the ingestion_configuration
        """

        # extract the instruction from the stream
        instruction = self.extract_instruction_from_stream(incoming_stream)

        # now work on the data stream according to the extracted instruction
        output_stream = self.process_stream(ingestion_configuration_id, incoming_stream, instruction, output_stream)

        # now return an output_stream if
        if output_stream:
            return output_stream
        else:
            return True

    def extract_instruction_from_stream(self, incoming_stream):
        """
        Extracts and returns the instruction from the data stream
        """
        instruction = ''
        # do some extraction stuff

        # return the extracted instruction
        return instruction
