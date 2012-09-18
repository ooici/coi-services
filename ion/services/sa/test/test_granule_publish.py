
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin

from pyon.public import RT, PRED
from pyon.public import log, IonObject

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_process_management_service import DataProcessManagementServiceClient

### For new granule and stream interface
from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.services.dm.utility.granule.granule import build_granule

from pyon.ion.stream import StandaloneStreamPublisher


from ion.services.dm.utility.granule_utils import CoverageCraft
from ooi.logging import log
from coverage_model.parameter import ParameterContext, ParameterDictionary
from coverage_model.parameter_types import QuantityType
from coverage_model.basic_types import AxisTypeEnum

from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum

from interface.objects import ProcessDefinition

from ion.util.parameter_yaml_IO import get_param_dict

from nose.plugins.attrib import attr
import numpy
import time

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''


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
        parameter_dictionary = get_param_dict('ctd_parsed_param_dict')

        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 1d spatial topology (station/trajectory)

        sdom = sdom.dump()
        tdom = tdom.dump()


        dp_obj = IonObject(RT.DataProduct,
            name='the parsed data',
            description='ctd stream test',
            temporal_domain = tdom,
            spatial_domain = sdom)

        data_product_id1 = self.dpclient.create_data_product(data_product=dp_obj, stream_definition_id='', parameter_dictionary = parameter_dictionary)


        # Retrieve the id of the output stream of the out Data Product
        stream_ids, _ = self.rrclient.find_objects(data_product_id1, PRED.hasStream, None, True)
        log.debug( 'test_granule_publish: Data product streams1 = %s', stream_ids)

        pid = self.create_logger('ctd_parsed', stream_ids[0] )
        self.loggerpids.append(pid)

        rdt = RecordDictionaryTool(param_dictionary=parameter_dictionary)

        #create the publisher from the stream route
        stream_route = self.pubsubclient.read_stream_route(stream_ids[0])
        publisher = StandaloneStreamPublisher(stream_ids[0], stream_route)

        # this is one sample from the ctd driver
        tomato = {"driver_timestamp": 3555971105.1268806, "instrument_id": "ABC-123", "pkt_format_id": "JSON_Data", "pkt_version": 1, "preferred_timestamp": "driver_timestamp", "quality_flag": "ok", "stream_name": "parsed", "values": [{"value": 22.9304, "value_id": "temp"}, {"value": 51.57381, "value_id": "conductivity"}, {"value": 915.551, "value_id": "depth"}]}

        for value in tomato['values']:
            log.debug("test_granule_publish: Looping tomato values  key: %s    val: %s ", str(value['value']), str(value['value_id']))

            if value['value_id'] in parameter_dictionary:
                rdt[value['value_id']] = numpy.array( [ value['value'] ] )
                log.debug("test_granule_publish: Added data item  %s  val: %s ", str(value['value']), str(value['value_id']) )

        g = build_granule(data_producer_id=tomato['instrument_id'], param_dictionary=parameter_dictionary, record_dictionary=rdt)

        publisher.publish(g)

        time.sleep(3)

        for pid in self.loggerpids:
            self.processdispatchclient.cancel_process(pid)

  
