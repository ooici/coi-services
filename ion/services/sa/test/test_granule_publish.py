#!/usr/bin/env python
'''
@file ion/services/sa/test/test_granule_publish.py
@date Wed Oct 10 12:06:20 EDT 2012
@description Tests for Tomato/Granule alignment
'''


from pyon.ion.stream import StandaloneStreamPublisher
from pyon.public import RT, PRED, log, IonObject
from pyon.util.int_test import IonIntegrationTestCase

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule_utils import time_series_domain

from interface.objects import ProcessDefinition
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient

from nose.plugins.attrib import attr
import uuid
import numpy
import gevent


@attr('INT', group='sa')
#@unittest.skip('run locally only')
class TestGranulePublish(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.pubsubclient =  PubsubManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.processdispatchclient = ProcessDispatcherServiceClient(node=self.container.node)
        self.dataprocessclient = DataProcessManagementServiceClient(node=self.container.node)
        self.dataproductclient = DataProductManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()


    def create_logger(self, name, stream_id=''):

        # logger process
        producer_definition = ProcessDefinition(name=name+'_logger')
        producer_definition.executable = {
            'module':'ion.processes.data.stream_granule_logger',
            'class':'StreamGranuleLogger'
        }

        logger_procdef_id = self.processdispatchclient.create_process_definition(process_definition=producer_definition)
        configuration = {
            'process':{
                'stream_id':stream_id,
                }
        }
        pid = self.processdispatchclient.schedule_process(process_definition_id=logger_procdef_id,
                                                          configuration=configuration)

        return pid

    #overriding trigger function here to use new granule
    def test_granule_publish(self):
        log.debug("test_granule_publish ")
        self.loggerpids = []


        #retrieve the param dict from the repository
        pdict_id = self.dataset_management.read_parameter_dictionary_by_name('ctd_parsed_param_dict',id_only=True)
        stream_definition_id = self.pubsubclient.create_stream_definition('parsed stream', parameter_dictionary_id=pdict_id)



        dp_obj = IonObject(RT.DataProduct,
            name=str(uuid.uuid4()),
            description='ctd stream test')

        data_product_id1 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id=stream_definition_id)


        # Retrieve the id of the output stream of the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'test_granule_publish: Data product streams1 = %s', stream_ids)

        pid = self.create_logger('ctd_parsed', stream_ids[0] )
        self.loggerpids.append(pid)

        rdt = RecordDictionaryTool(stream_definition_id=stream_definition_id)

        #create the publisher from the stream route
        stream_route = self.pubsubclient.read_stream_route(stream_ids[0])
        publisher = StandaloneStreamPublisher(stream_ids[0], stream_route)

        # this is one sample from the ctd driver
        tomato = {"driver_timestamp": 3555971105.1268806, "instrument_id": "ABC-123", "pkt_format_id": "JSON_Data", "pkt_version": 1, "preferred_timestamp": "driver_timestamp", "quality_flag": "ok", "stream_name": "parsed", "values": [{"value": 22.9304, "value_id": "temp"}, {"value": 51.57381, "value_id": "conductivity"}, {"value": 915.551, "value_id": "pressure"}]}

        for value in tomato['values']:
            log.debug("test_granule_publish: Looping tomato values  key: %s    val: %s ", str(value['value']), str(value['value_id']))

            if value['value_id'] in rdt:
                rdt[value['value_id']] = numpy.array( [ value['value'] ] )
                log.debug("test_granule_publish: Added data item  %s  val: %s ", str(value['value']), str(value['value_id']) )

        g = rdt.to_granule()

        publisher.publish(g)

        gevent.sleep(3)

        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)
  
        #--------------------------------------------------------------------------------
        # Cleanup data products
        #--------------------------------------------------------------------------------
        dp_ids, _ = self.rrclient.find_resources(restype=RT.DataProduct, id_only=True)

        for dp_id in dp_ids:
            self.dataproductclient.delete_data_product(dp_id)

