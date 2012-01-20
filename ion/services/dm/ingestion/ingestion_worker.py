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

    def __init__(self):
        # store a list of configuration id's
        self.ingestion_config_ids = []

    def store_config_id(self, ingestion_configuration_id):
        self.ingestion_config_ids.append(ingestion_configuration_id)

    def refresh_config_id_store(self):
        self.ingestion_config_ids = []

    def take_science_object(self, ingestion_configuration_id, science_obj):
        """
        Accept an science object and an ingestion configuration id and do something
        """

        # Read the transformation params and define a transformation object


        # Do something for the preservation of the object


    def define_datastream_raw(self, raw_samples_stream):
        """
        Defines a data stream object to send to the distribution service
        and returns the datastream id obtained from distribution service
        """

    def read_params_from_metadata(self):
        """
        Reads the params from the metadata attribute in the science object
        """

    def define_datastream_product(self, raw_product_stream_id):
        """
        Defines a raw product stream and returns the raw_product_stream_id obtained from the distribution service
        """

    def define_preservation(self, params):
        """
        Defines a preservation object and returns a success/fail value obtained from the preservation service
        """

