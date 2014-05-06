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


from pyon.ion.process import ImmediateProcess
from pyon.public import PRED,RT,IonObject 
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

#Instrument related imports
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.dm.iingestion_management_service import IngestionManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.dm.utility.granule_utils import time_series_domain

#from ion.services.ans.test.test_helper import helper_create_highcharts_workflow_def
from ion.services.ans.test.test_helper import preload_ion_params


class VisStreamLauncher(ImmediateProcess):
    """
    Class emulates a stream source from a NetCDF file. It emits a record of data every few seconds on a
    stream identified by a routing key.

    """


    def on_init(self):
        log.debug("VizStreamProducer init. Self.id=%s" % self.id)

    def on_start(self):

        log.debug("VizStreamProducer start")

        self.data_source_name = self.CFG.get_safe('name', 'sine_wave_generator')
        self.dataset = self.CFG.get_safe('dataset', 'sinusoidal')

        # create a pubsub client and a resource registry client
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.pubsubclient = PubsubManagementServiceClient(node=self.container.node)

        # Dummy instrument related clients
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.ingestclient = IngestionManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()


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
        else:
            # Create InstrumentModel
            instModel_obj = IonObject(RT.InstrumentModel, name=self.data_source_name, description=self.data_source_name)
            instModel_id = self.imsclient.create_instrument_model(instModel_obj)

            # Create InstrumentDevice
            instDevice_obj = IonObject(RT.InstrumentDevice, name=self.data_source_name, description=self.data_source_name, serial_number="12345" )

            instDevice_id = self.imsclient.create_instrument_device(instrument_device=instDevice_obj)
            self.imsclient.assign_instrument_model_to_instrument_device(instModel_id, instDevice_id)

            # create a stream definition for the data from the ctd simulator
            pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
            ctd_stream_def_id = self.pubsubclient.create_stream_definition(name="SBE37_CDM", description="SBE37_CDM", parameter_dictionary_id=pdict_id)


            dp_obj = IonObject(RT.DataProduct,
                name=self.data_source_name,
                description='Example ctd stream')

            data_product_id = self.dpclient.create_data_product(dp_obj, stream_definition_id=ctd_stream_def_id)

            self.damsclient.assign_data_product(input_resource_id=instDevice_id, data_product_id=data_product_id)
            self.dpclient.activate_data_product_persistence(data_product_id=data_product_id)

        print ">>>>>>>>>>>>> Dataproduct for sine wave generator : ", data_product_id

        # Retrieve the id of the OUTPUT stream from the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id, PRED.hasStream, None, True)

        if self.dataset == 'sinusoidal':
            self.container.spawn_process(name='ctd_test.' + self.data_source_name ,
                module='ion.processes.data.sinusoidal_stream_publisher',cls='SinusoidalCtdPublisher',config={'process':{'stream_id':stream_ids[0]}})
        else:
            self.container.spawn_process(name='ctd_test.' + self.data_source_name ,
                module='ion.processes.data.ctd_stream_publisher',cls='SimpleCtdPublisher',config={'process':{'stream_id':stream_ids[0]}})

        """
        workflow_def_ids,_ = self.rrclient.find_resources(restype=RT.WorkflowDefinition, name='Realtime_HighCharts', id_only=True)

        if not len(workflow_def_ids):
            helper_create_highcharts_workflow_def(self.container)
        """


    def on_quit(self):
        super(VisStreamLauncher, self).on_quit()
        log.debug("VizStreamProducer quit")

