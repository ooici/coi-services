#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/vis_stream_publisher.py
@description A simple example process which publishes prototype ctd data for visualization

To Run:
bin/pycc --rel res/deploy/r2dm.yml
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.vis_stream_publisher',cls='SimpleCtdPublisher')

'''

from pyon.public import log

import time
import math
import random
import threading
import gevent

from pyon.service.service import BaseService
from pyon.ion.process import ImmediateProcess
from pyon.public import PRED,RT,Container, log, IonObject, StreamPublisherRegistrar
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from prototype.sci_data.stream_defs import ctd_stream_packet, ctd_stream_definition

#Instrument related imports
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from prototype.sci_data.stream_defs import ctd_stream_definition
from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition, SBE37_RAW_stream_definition
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import IDataProductManagementService, DataProductManagementServiceClient
from interface.objects import HdfStorage, CouchStorage



class VisStreamLauncher(ImmediateProcess):
    """
    Class emulates a stream source from a NetCDF file. It emits a record of data every few seconds on a
    stream identified by a routing key.

    """


    def on_init(self):
        log.debug("VizStreamProducer init. Self.id=%s" % self.id)

    def on_start(self):

        log.debug("VizStreamProducer start")
        self.data_source_name = self.CFG.get('name')
        self.dataset = self.CFG.get('dataset')

        # create a pubsub client and a resource registry client
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.pubsubclient = PubsubManagementServiceClient(node=self.container.node)

        # Dummy instrument related clients
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.IngestClient = IngestionManagementServiceClient(node=self.container.node)

        # create the pubsub client
        self.pubsubclient = PubsubManagementServiceClient(node=self.container.node)


        # Additional code for creating a dummy instrument
        """
        # Set up the preconditions. Look for an existing ingestion config
        while True:
            log.info("VisStreamLauncher:on_start: Waiting for an ingestion configuration to be available.")
            ingestion_cfgs, _ = self.rrclient.find_resources(RT.IngestionConfiguration, None, None, True)

            if len(ingestion_cfgs) > 0:
                break
            else:
                gevent.sleep(1)
        """


        # Check to see if the data_product already exists in the system (for e.g re launching the code after a crash)
        dp_ids,_ = self.rrclient.find_resources(RT.DataProduct, None, self.data_source_name, True)
        if len(dp_ids) > 0:
            data_product_id = dp_ids[0]
            print '>>>>>>>>>>>>> Found dp_id = ', data_product_id
        else:
            # Create InstrumentModel
            instModel_obj = IonObject(RT.InstrumentModel, name=self.data_source_name, description=self.data_source_name, model_label=self.data_source_name)
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)

            # Create InstrumentDevice
            instDevice_obj = IonObject(RT.InstrumentDevice, name=self.data_source_name, description=self.data_source_name, serial_number="12345" )

            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

            # create a stream definition for the data from the ctd simulator
            ctd_stream_def = SBE37_CDM_stream_definition()
            ctd_stream_def_id = self.pubsubclient.create_stream_definition(container=ctd_stream_def)

            print 'Creating new CDM data product with a stream definition'
            dp_obj = IonObject(RT.DataProduct,name=self.data_source_name,description='ctd stream test')
            data_product_id = self.dpclient.create_data_product(dp_obj, ctd_stream_def_id)

            self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id)
            self.dpclient.activate_data_product_persistence(data_product_id=data_product_id, persist_data=True, persist_metadata=True)

            print '>>>>>>>>>>>> New dp_id = ', data_product_id

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id, PRED.hasStream, None, True)

        if self.dataset == 'sinusoidal':
            pid = self.container.spawn_process(name='ctd_test.' + self.data_source_name ,
                module='ion.processes.data.sinusoidal_stream_publisher',cls='SinusoidalCtdPublisher',config={'process':{'stream_id':stream_ids[0]}})
        else:
            pid = self.container.spawn_process(name='ctd_test.' + self.data_source_name ,
                module='ion.processes.data.ctd_stream_publisher',cls='SimpleCtdPublisher',config={'process':{'stream_id':stream_ids[0]}})




    def on_quit(self):
        log.debug("VizStreamProducer quit")

