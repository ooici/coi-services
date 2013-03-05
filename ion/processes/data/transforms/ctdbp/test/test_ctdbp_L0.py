#!/usr/bin/env python

'''
@brief Test to check CTD
@author Swarbhanu Chatterjee
'''


import os
from pyon.ion.stream import  StandaloneStreamPublisher
from pyon.public import log, IonObject, RT
from pyon.util.containers import DotDict
from pyon.util.file_sys import FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.containers import get_safe
from pyon.ion.stream import StandaloneStreamSubscriber
from nose.plugins.attrib import attr

from interface.objects import ProcessDefinition
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient

from interface.objects import StreamRoute, Granule
from ion.processes.data.ctd_stream_publisher import SimpleCtdPublisher
from ion.processes.data.transforms.ctd.ctd_L0_all import ctd_L0_all
from ion.processes.data.transforms.ctd.ctd_L1_conductivity import CTDL1ConductivityTransform
from ion.processes.data.transforms.ctd.ctd_L1_pressure import CTDL1PressureTransform
from ion.processes.data.transforms.ctd.ctd_L1_temperature import CTDL1TemperatureTransform
from ion.processes.data.transforms.ctd.ctd_L2_salinity import SalinityTransform
from ion.processes.data.transforms.ctd.ctd_L2_density import DensityTransform
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule_utils import time_series_domain
from coverage_model import QuantityType
import unittest, gevent
import numpy, random
from seawater.gibbs import SP_from_cndr, rho, SA_from_SP
from seawater.gibbs import cte

@attr('INT', group='dm')
class CtdTransformsIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(CtdTransformsIntTest, self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.pubsub            = PubsubManagementServiceClient()
        self.process_dispatcher = ProcessDispatcherServiceClient()
        self.dataset_management = DatasetManagementServiceClient()
        self.data_process_management = DataProcessManagementServiceClient()
        self.dataproduct_management = DataProductManagementServiceClient()

    def test_ctd_L0_all(self):
        '''
        Test that packets are processed by the ctd_L0_all transform
        '''

        #----------- Data Process Definition --------------------------------
        dpd_obj = IonObject(RT.DataProcessDefinition,
            name='CTDBP_L0_all',
            description='Take parsed stream and put the C, T and P into three separate L0 streams.',
            module='ion.processes.data.transforms.ctdbp.ctdbp_L0',
            class_name='CTDBP_L0_all')

        dprocdef_id = self.data_process_management.create_data_process_definition(dpd_obj)

        log.debug("created data process definition: id = %s", dprocdef_id)

        #----------- Data Products --------------------------------


        # Construct temporal and spatial Coordinate Reference System objects
        tdom, sdom = time_series_domain()

        sdom = sdom.dump()
        tdom = tdom.dump()

        # Get the stream definition for the stream using the parameter dictionary
        parsed_pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict', id_only=True)
        parsed_stream_def_id = self.pubsub.create_stream_definition(name='parsed', parameter_dictionary_id=parsed_pdict_id)

        log.debug("Got the parsed parameter dictionary: id: %s", parsed_pdict_id)
        log.debug("Got the stream def for parsed input: %s", parsed_stream_def_id)

        # Input data product
        parsed_stream_dp_obj = IonObject(RT.DataProduct,
            name='parsed_stream',
            description='Parsed stream input to CTBP L0 transform',
            temporal_domain = tdom,
            spatial_domain = sdom)

        input_dp_id = self.dataproduct_management.create_data_product(data_product=parsed_stream_dp_obj,
            stream_definition_id=parsed_stream_def_id
        )

        # output data product
        L0_stream_dp_obj = IonObject(RT.DataProduct,
            name='L0_stream',
            description='L0_stream output of CTBP L0 transform',
            temporal_domain = tdom,
            spatial_domain = sdom)

        L0_stream_dp_id = self.dataproduct_management.create_data_product(data_product=L0_stream_dp_obj,
                                                                    stream_definition_id=parsed_stream_def_id
                                                                    )
        self.output_products = {'L0_stream' : L0_stream_dp_id}

        dproc_id = self.data_process_management.create_data_process( dprocdef_id, [input_dp_id], self.output_products)

        log.debug("Created a data process for ctdbp_L0. id: %s", dproc_id)



#    def check_cond_algorithm_execution(self, publish_granule, granule_from_transform):
#
#        input_rdt_to_transform = RecordDictionaryTool.load_from_granule(publish_granule)
#        output_rdt_transform = RecordDictionaryTool.load_from_granule(granule_from_transform)
#
#        output_data = output_rdt_transform['conductivity']
#        input_data = input_rdt_to_transform['conductivity']
#
#        self.assertTrue(numpy.array_equal(((input_data / 100000.0) - 0.5), output_data))
#
#
#    def check_granule_splitting(self, publish_granule, out_dict):
#        '''
#        This checks that the ctd_L0_all transform is able to split out one of the
#        granules from the whole granule
#        fed into the transform
#        '''
#
#        input_rdt_to_transform = RecordDictionaryTool.load_from_granule(publish_granule)
#
#        in_cond = input_rdt_to_transform['conductivity']
#        in_pressure = input_rdt_to_transform['pressure']
#        in_temp = input_rdt_to_transform['temp']
#
#        out_cond = out_dict['c']
#        out_pres = out_dict['p']
#        out_temp = out_dict['t']
#
#        self.assertTrue(numpy.array_equal(in_cond,out_cond))
#        self.assertTrue(numpy.array_equal(in_pressure, out_pres))
#        self.assertTrue(numpy.array_equal(in_temp,out_temp))
