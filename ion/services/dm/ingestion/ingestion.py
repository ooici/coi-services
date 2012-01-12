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

class Ingestion(object):
    """
    This Class creates an instance of a science object and science object transport
     to contain the data for use in inventory (interacts with the inventory as required),
    defines a Transformation (interacts with the transformation management),
    defines a Datastream (interacts with the pubsub management as required),
    defines Preservation (interacts with Preservation)

    Instances of this class acts as Ingestion Workers
    """

    def __init__(self, ionobject = None):
        self.ionobject = ionobject
        self.params_from_metadata = self.read_params_from_metadata()
        self.ingest()

    def ingest(self):
        """
        This method does everything ingestion is required to do for a particular ion object
        """
        science_object = self.define_science_object(self.ionobject) # define the science object
        self.define_datastream_raw(science_object) # defines a datastream and pushes in the science object

        # Read the transformation params from the metadata in the science_object and define a transformation object
        transform_params = self.params_from_metadata['transform params']
        self.define_transformation(transform_params)

        # Do something for the preservation of the object
        preservation_params = self.params_from_metadata['preservation params']
        self.define_preservation(preservation_params)


    def define_science_object(self, science_object = None):
        """
        Defines a science object and returns the science object id obtained from the inventory
        """
        self.science_object = ScienceObjectTransport(science_object)


    def define_datastream_raw(self, raw_samples_stream):
        """
        Defines a data stream object to send to the distribution service
        and returns the datastream id obtained from distribution service
        """

    def read_params_from_metadata(self):
        """
        Reads the params from the metadata attribute in the science object
        """


    def define_transformation(self, transform_params):
        """
        Defines a transformation object
        and returns the transform_id obtained from the transformation service
        """

    def define_datastream_product(self, raw_product_stream_id):
        """
        Defines a raw product stream and returns the raw_product_stream_id obtained from the distribution service
        """

    def define_preservation(self, params):
        """
        Defines a preservation object and returns a success/fail value obtained from the preservation service
        """

    def update_science_object(self, science_object_id, metadata):
        """
        Updates the metadata content of a science object when provided the science object_id
        """
