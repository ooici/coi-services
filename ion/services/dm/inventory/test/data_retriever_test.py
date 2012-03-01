'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/test/data_retriever_test.py
@description Testing Platform for Data Retriver Service
'''
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from ion.processes.data.replay_process import llog
from pyon.core.exception import NotFound
from pyon.datastore.datastore import DataStore
from pyon.public import  StreamSubscriberRegistrar
from pyon.public import PRED, log
from pyon.util.containers import DotDict
from pyon.util.file_sys import FS, FileSystem
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.net.endpoint import Subscriber
from interface.objects import Replay, StreamQuery, BlogPost, BlogAuthor, ProcessDefinition
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.idata_retriever_service import DataRetrieverServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from ion.services.dm.inventory.data_retriever_service import DataRetrieverService

from nose.plugins.attrib import attr
from mock import Mock
import unittest, time
import pyon.core.bootstrap as bootstrap
import gevent
import unittest
import os

@attr('UNIT',group='dm')
class DataRetrieverServiceTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('data_retriever')
        self.data_retriever_service = DataRetrieverService()
        self.data_retriever_service.clients = mock_clients
        self.mock_rr_create = self.data_retriever_service.clients.resource_registry.create
        self.mock_rr_create_assoc = self.data_retriever_service.clients.resource_registry.create_association
        self.mock_rr_read = self.data_retriever_service.clients.resource_registry.read
        self.mock_rr_update = self.data_retriever_service.clients.resource_registry.update
        self.mock_rr_delete = self.data_retriever_service.clients.resource_registry.delete
        self.mock_rr_delete_assoc = self.data_retriever_service.clients.resource_registry.delete_association
        self.mock_rr_find_assoc = self.data_retriever_service.clients.resource_registry.find_associations
        self.mock_ps_create_stream = self.data_retriever_service.clients.pubsub_management.create_stream
        self.mock_ps_create_stream_definition = self.data_retriever_service.clients.pubsub_management.create_stream_definition
        self.data_retriever_service.container = DotDict({'id':'123','spawn_process':Mock(),'proc_manager':DotDict({'terminate_process':Mock(),'procs':[]})})
        self.mock_cc_spawn = self.data_retriever_service.container.spawn_process
        self.mock_cc_terminate = self.data_retriever_service.container.proc_manager.terminate_process
        self.mock_pd_schedule = self.data_retriever_service.clients.process_dispatcher.schedule_process
        self.mock_pd_cancel = self.data_retriever_service.clients.process_dispatcher.cancel_process
        self.mock_ds_read = self.data_retriever_service.clients.dataset_management.read_dataset
        self.data_retriever_service.process_definition = ProcessDefinition()
        self.data_retriever_service.process_definition.executable['module'] = 'ion.processes.data.replay_process'
        self.data_retriever_service.process_definition.executable['class'] = 'ReplayProcess'

        self.data_retriever_service.process_definition_id = 'mock_procdef_id'

    def test_define_replay(self):
        #mocks
        self.mock_ps_create_stream.return_value = '12345'
        self.mock_rr_create.return_value = ('replay_id','garbage')
        self.mock_ds_read.return_value = DotDict({
            'datastore_name':'unittest',
            'view_name':'garbage',
            'primary_view_key':'primary key'})

        self.mock_pd_schedule.return_value = 'process_id'

        config = {'process':{
            'query':'myquery',
            'datastore_name':'unittest',
            'view_name':'garbage',
            'key_id':'primary key',
            'delivery_format':None,
            'publish_streams':{'output':'12345'}
        }}


        # execution
        r,s = self.data_retriever_service.define_replay(dataset_id='dataset_id', query='myquery')

        # assertions
        self.assertTrue(self.mock_ps_create_stream_definition.called)
        self.assertTrue(self.mock_ps_create_stream.called)
        self.assertTrue(self.mock_rr_create.called)
        self.mock_rr_create_assoc.assert_called_with('replay_id',PRED.hasStream,'12345',None)
        self.assertTrue(self.mock_pd_schedule.called)
        self.assertTrue(self.mock_rr_update.called)
        self.assertEquals(r,'replay_id')
        self.assertEquals(s,'12345')



    @unittest.skip('Can\'t do unit test here')
    def test_start_replay(self):
        pass


    def test_cancel_replay(self):
        #mocks
        self.mock_rr_find_assoc.return_value = [1,2,3]

        replay = Replay()
        replay.process_id = '1'
        self.mock_rr_read.return_value = replay

        #execution
        self.data_retriever_service.cancel_replay('replay_id')

        #assertions
        self.assertEquals(self.mock_rr_delete_assoc.call_count,3)
        self.mock_rr_delete.assert_called_with('replay_id')

        self.mock_pd_cancel.assert_called_with('1')




@attr('INT', group='dm')
class DataRetrieverServiceIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(DataRetrieverServiceIntTest,self).setUp()
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.couch = self.container.datastore_manager.get_datastore('test_data_retriever', profile=DataStore.DS_PROFILE.EXAMPLES)
        self.datastore_name = 'test_data_retriever'

        self.dr_cli = DataRetrieverServiceClient(node=self.container.node)
        self.dsm_cli = DatasetManagementServiceClient(node=self.container.node)
        self.rr_cli = ResourceRegistryServiceClient(node=self.container.node)
        self.ps_cli = PubsubManagementServiceClient(node=self.container.node)
        self.tms_cli = TransformManagementServiceClient(node=self.container.node)
        self.pd_cli = ProcessDispatcherServiceClient(node=self.container.node)


    def _populate(self):
        self.couch = self.container.datastore_manager.get_datastore('test_replay',profile=DataStore.DS_PROFILE.SCIDATA)
        couch = self.couch.server
        db = couch[self.couch.datastore_name]
        docs = [{u'identifiables': {u'pressure_data': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Pressure', u'reference_frame': u'Atmospheric pressure ?', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'dbar', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'vertex', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': [[0, 10000.0]]}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'', u'updatable': True, u'nil_values_ids': [u'nan_value'], u'optional': False, u'id': u'', u'values_path': u'/fields/pressure_data', u'axis': u'Pressure'}, u'latitude_data': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/spaceCoordinates.owl#Latitude', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'deg', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'vertex', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': [[-90.0, 90.0]]}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'', u'updatable': True, u'nil_values_ids': [u'nan_value'], u'optional': False, u'id': u'', u'values_path': u'/fields/latitude', u'axis': u'Latitude'}, u'record_count': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'axisID': u'', u'quality_id': u'', u'value': 0, u'updatable': True, u'type_': u'CountElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'nan_value': {u'reason': u'No value recorded', u'type_': u'NilValue', u'value': -999.99}, u'temperature': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Temperature', u'description': u'', u'label': u'', u'type_': u'Coverage', u'id': u'', u'range_id': u'temp_data', u'updatable': False, u'optional': True, u'domain_id': u'time_domain'}, u'stream_encoding': {u'sha1': None, u'compression': None, u'label': u'', u'type_': u'Encoding', u'record_dimension': u'', u'encoding_type': u'hdf5', u'id': u'', u'description': u''}, u'point_timeseries': {u'definition': u'', u'description': u'', u'number_of_verticies': 1, u'updatable': True, u'type_': u'Mesh', u'mesh_type': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'Point Time Series', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'number_of_elements': 1, u'label': u'', u'optional': False, u'id': u'', u'index_offset': 0, u'values_path': u'/topology/mesh'}, u'time_domain': {u'definition': u'Spec for ctd data time domain', u'coordinate_vector_id': u'coordinate_vector', u'description': u'', u'label': u'', u'type_': u'Domain', u'mesh_id': u'point_timeseries', u'updatable': u'False', u'optional': u'False', u'id': u''}, u'longitude_data': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/spaceCoordinates.owl#Longitude', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'deg', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'vertex', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': [[0.0, 360.0]]}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'', u'updatable': True, u'nil_values_ids': [u'nan_value'], u'optional': False, u'id': u'', u'values_path': u'/fields/longitude', u'axis': u'Longitude'}, u'longitude': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/spaceCoordinates.owl#longitude', u'description': u'', u'label': u'', u'type_': u'Coverage', u'id': u'', u'range_id': u'longitude_data', u'updatable': False, u'optional': True, u'domain_id': u'time_domain'}, u'data_record': {u'definition': u'Definition of a data record for a CTD', u'description': u'', u'label': u'', u'type_': u'DataRecord', u'field_ids': [u'temperature', u'conductivity', u'pressure', u'latitude', u'longitude', u'time'], u'updatable': False, u'optional': False, u'id': u'', u'domain_ids': [u'time_domain']}, u'pressure': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Pressure', u'description': u'', u'label': u'', u'type_': u'Coverage', u'id': u'', u'range_id': u'pressure_data', u'updatable': False, u'optional': True, u'domain_id': u'time_domain'}, u'element_type': {u'definition': u'Ref to SeaBird data?', u'description': u'', u'label': u'', u'type_': u'ElementType', u'data_record_id': u'data_record', u'updatable': False, u'optional': False, u'id': u''}, u'time': {u'definition': u'http://www.opengis.net/def/property/OGC/0/SamplingTime', u'description': u'', u'label': u'', u'type_': u'Coverage', u'id': u'', u'range_id': u'time_data', u'updatable': False, u'optional': True, u'domain_id': u'time_domain'}, u'temp_data': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Temperature', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'Cel', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'vertex', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': [[-10.0, 50.0]]}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': u'', u'updatable': True, u'nil_values_ids': [u'nan_value'], u'optional': False, u'id': u'', u'values_path': u'/fields/temp_data'}, u'coordinate_vector': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/space.owl#Location', u'reference_frame': u'http://www.opengis.net/def/crs/EPSG/0/4326', u'description': u'', u'coordinate_ids': [u'longitude_data', u'latitude_data', u'pressure_data', u'time_data'], u'updatable': True, u'type_': u'Vector', u'local_frame': u'', u'label': u'', u'optional': False, u'id': u''}, u'latitude': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/spaceCoordinates.owl#Latitude', u'description': u'', u'label': u'', u'type_': u'Coverage', u'id': u'', u'range_id': u'latitude_data', u'updatable': False, u'optional': True, u'domain_id': u'time_domain'}, u'conductivity': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Conductivity', u'description': u'', u'label': u'', u'type_': u'Coverage', u'id': u'', u'range_id': u'cndr_data', u'updatable': False, u'optional': True, u'domain_id': u'time_domain'}, u'cndr_data': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Conductivity', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'mS/cm', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'vertex', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': [[0.0, 85.0]]}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': u'', u'updatable': True, u'nil_values_ids': [u'nan_value'], u'optional': False, u'id': u'', u'values_path': u'/fields/cndr_data'}, u'ctd_data': {u'description': u'Conductivity temperature and depth observations from a Seabird CTD', u'element_type_id': u'element_type', u'element_count_id': u'record_count', u'type_': u'DataStream', u'label': u'Seabird CTD Data', u'encoding_id': u'stream_encoding', u'values': None, u'id': u''}, u'time_data': {u'definition': u'http://www.opengis.net/def/property/OGC/0/SamplingTime', u'reference_frame': u'http://www.opengis.net/def/trs/OGC/0/GPS', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u's', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'vertex', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'', u'updatable': True, u'nil_values_ids': [u'nan_value'], u'optional': False, u'id': u'', u'values_path': u'/fields/time', u'axis': u'Time'}}, u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f', u'_rev': u'1-f2db3e9b4e46dcc5bb49200c85844173', u'_id': u'bf2d718627b54a65bd9542e75a918497', u'type_': u'StreamDefinitionContainer', u'data_stream_id': u'ctd_data'}
            ,
                {u'identifiables': {u'pressure_bounds': {u'definition': u'', u'reference_frame': u'', u'value_pair': [0.040225054893218516, 5.5499162078857625], u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'updatable': True, u'optional': False, u'id': u''}, u'pressure_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'pressure_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}}, u'record_count': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'axisID': u'', u'quality_id': u'', u'value': 6, u'updatable': True, u'type_': u'CountElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'longitude_bounds': {u'definition': u'', u'reference_frame': u'', u'value_pair': [1.0325383147336753, 296.10451428103346], u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'updatable': True, u'optional': False, u'id': u''}, u'stream_encoding': {u'sha1': u'55CB1F4C4D34A9148971815CC8E1F9C93FA6CA8B', u'description': u'', u'encoding_type': u'hdf5', u'type_': u'Encoding', u'record_dimension': u'', u'label': u'', u'id': u'', u'compression': None}, u'temp_bounds': {u'definition': u'', u'reference_frame': u'', u'value_pair': [-0.046320138917800735, 16.458295796245906], u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'updatable': True, u'optional': False, u'id': u''}, u'time_bounds': {u'definition': u'', u'reference_frame': u'', u'value_pair': [34, 39], u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'updatable': True, u'optional': False, u'id': u''}, u'longitude': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'longitude_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}}, u'latitude': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'latitude_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}}, u'time': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'time_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}}, u'cndr_bounds': {u'definition': u'', u'reference_frame': u'', u'value_pair': [6.944138296667554, 54.57866838767616], u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'updatable': True, u'optional': False, u'id': u''}, u'cndr_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'label': u'', u'type_': u'CategoryElement', u'nil_value_ids': [], u'updatable': True, u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': u'cndr_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u''}, u'ctd_data': {u'description': u'', u'element_type_id': u'', u'element_count_id': u'', u'encoding_id': u'', u'label': u'', u'type_': u'DataStream', u'values': u'', u'id': u'0821e596cc4240c3b95401a47ddfbb2f'}, u'latitude_bounds': {u'definition': u'', u'reference_frame': u'', u'value_pair': [-44.480773912009205, 22.986266307284936], u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'updatable': True, u'optional': False, u'id': u''}}, u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f', u'_rev': u'2-648d12b4292eeb6058c4ea0bd1bc4ad4', u'_id': u'110975FFA2B46E3324FBFE69CEB8A68446C326A2', u'type_': u'StreamGranuleContainer', u'data_stream_id': u'ctd_data'}
            ,
                {u'identifiables': {u'pressure_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [0.0745266792697992, 32.86066226307717], u'label': u'', u'optional': False, u'id': u''}, u'pressure_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'pressure_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'record_count': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'axisID': u'', u'quality_id': u'', u'value': 14, u'updatable': True, u'type_': u'CountElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'longitude_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [18.885003847513808, 342.9197708489752], u'label': u'', u'optional': False, u'id': u''}, u'stream_encoding': {u'sha1': u'21F7A781C4F83826780DBE6B15AB1B7BE4D247A5', u'compression': None, u'label': u'', u'type_': u'Encoding', u'record_dimension': u'', u'encoding_type': u'hdf5', u'id': u'', u'description': u''}, u'temp_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [-1.1401749695070225, 20.565461001195967], u'label': u'', u'optional': False, u'id': u''}, u'time_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [20, 33], u'label': u'', u'optional': False, u'id': u''}, u'longitude': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'longitude_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'temp_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': [u'temp_bounds'], u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u''}, u'time': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'time_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'latitude': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'latitude_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'cndr_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [10.737142742161254, 72.57455515631258], u'label': u'', u'optional': False, u'id': u''}, u'cndr_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': u'cndr_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u''}, u'ctd_data': {u'description': u'', u'element_type_id': u'', u'element_count_id': u'', u'type_': u'DataStream', u'label': u'', u'encoding_id': u'', u'values': u'', u'id': u'0821e596cc4240c3b95401a47ddfbb2f'}, u'latitude_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [-88.56008179119722, 88.98372043221099], u'label': u'', u'optional': False, u'id': u''}}, u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f', u'_rev': u'1-b282322ec73a0a74d1aff682f9a97bdb', u'_id': u'27368C7D7BB3B653811A4514E58A6FB1E4ACA71A', u'type_': u'StreamGranuleContainer', u'data_stream_id': u'ctd_data'}
            ,
                {u'identifiables': {u'pressure_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [0.195278815188773, 65.65633331582185], u'label': u'', u'optional': False, u'id': u''}, u'pressure_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'pressure_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'record_count': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'axisID': u'', u'quality_id': u'', u'value': 6, u'updatable': True, u'type_': u'CountElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'longitude_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [94.90215009454022, 295.9261234577359], u'label': u'', u'optional': False, u'id': u''}, u'stream_encoding': {u'sha1': u'FA8D02DE0CAA27C5026ED2255CF4156252164AC1', u'compression': None, u'label': u'', u'type_': u'Encoding', u'record_dimension': u'', u'encoding_type': u'hdf5', u'id': u'', u'description': u''}, u'temp_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [1.9702501679981628, 19.025763915299958], u'label': u'', u'optional': False, u'id': u''}, u'time_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [1, 6], u'label': u'', u'optional': False, u'id': u''}, u'longitude': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'longitude_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'temp_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': [u'temp_bounds'], u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u''}, u'time': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'time_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'latitude': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'latitude_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'cndr_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [0.17038448442782683, 51.169317741105424], u'label': u'', u'optional': False, u'id': u''}, u'cndr_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': u'cndr_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u''}, u'ctd_data': {u'description': u'', u'element_type_id': u'', u'element_count_id': u'', u'type_': u'DataStream', u'label': u'', u'encoding_id': u'', u'values': u'', u'id': u'0821e596cc4240c3b95401a47ddfbb2f'}, u'latitude_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [-79.29736180272789, 41.94293790102634], u'label': u'', u'optional': False, u'id': u''}}, u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f', u'_rev': u'1-195061d361608e222884d9353c92deaa', u'_id': u'2D05B37809E84AACFB547CBF8D4D35466FEDB94B', u'type_': u'StreamGranuleContainer', u'data_stream_id': u'ctd_data'}
            ,
                {u'identifiables': {u'pressure_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [0.2923022238297028, 44.479318986212235], u'label': u'', u'optional': False, u'id': u''}, u'pressure_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'pressure_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'record_count': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'axisID': u'', u'quality_id': u'', u'value': 13, u'updatable': True, u'type_': u'CountElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'longitude_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [21.405743057384644, 346.85146105287237], u'label': u'', u'optional': False, u'id': u''}, u'stream_encoding': {u'sha1': u'9F64199563855388279C0C2D47D46AEF8A2CB38D', u'compression': None, u'label': u'', u'type_': u'Encoding', u'record_dimension': u'', u'encoding_type': u'hdf5', u'id': u'', u'description': u''}, u'temp_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [1.7476851108109714, 20.31502307857048], u'label': u'', u'optional': False, u'id': u''}, u'time_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [7, 19], u'label': u'', u'optional': False, u'id': u''}, u'longitude': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'longitude_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'temp_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': [u'temp_bounds'], u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u''}, u'time': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'time_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'latitude': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'CoordinateAxis', u'bounds_id': u'latitude_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u'', u'axis': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}}, u'cndr_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [0.8459897537126948, 74.27084120679112], u'label': u'', u'optional': False, u'id': u''}, u'cndr_data': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'mesh_location': {u'definition': u'', u'reference_frame': u'', u'description': u'', u'constraint': {u'pattern': u'', u'type_': u'AllowedTokens', u'values': []}, u'axisID': u'', u'quality_id': u'', u'code_space': u'', u'value': u'', u'updatable': True, u'type_': u'CategoryElement', u'nil_value_ids': [], u'label': u'', u'optional': False, u'id': u''}, u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'label': u'', u'type_': u'RangeSet', u'bounds_id': u'cndr_bounds', u'updatable': True, u'nil_values_ids': [], u'optional': False, u'id': u'', u'values_path': u''}, u'ctd_data': {u'description': u'', u'element_type_id': u'', u'element_count_id': u'', u'type_': u'DataStream', u'label': u'', u'encoding_id': u'', u'values': u'', u'id': u'0821e596cc4240c3b95401a47ddfbb2f'}, u'latitude_bounds': {u'definition': u'', u'reference_frame': u'', u'unit_of_measure': {u'type_': u'UnitReferenceProperty', u'code': u'', u'reference': u''}, u'description': u'', u'quality_id': u'', u'axisID': u'', u'constraint': {u'significant_figures': -1, u'type_': u'AllowedValues', u'intervals': [], u'values': []}, u'updatable': True, u'type_': u'QuantityRangeElement', u'nil_value_ids': [], u'value_pair': [-72.8198949433451, 82.5961309943809], u'label': u'', u'optional': False, u'id': u''}}, u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f', u'_rev': u'1-90910d9d2ceafde3df34e149de5da721', u'_id': u'BA520236C5B4682AAB9EC64797C08B32B879410B', u'type_': u'StreamGranuleContainer', u'data_stream_id': u'ctd_data'},

                {u'_rev': u'1-ea4f6217da2c54e78638d4b09c086365', u'_id': u'_design/datasets', u'views': {u'dataset_by_id': {u'map': u'function (doc) {\n    if(doc.type_ == "StreamDefinitionContainer")\n        emit([doc.stream_resource_id,0],doc._id);\n    else if(doc.type_ == "StreamGranuleContainer")\n        emit([doc.stream_resource_id,1], doc._id);\n}\n'}, u'stream_join_granule': {u'map': u'function (doc) {\n    if(doc.type_ == "StreamDefinitionContainer")\n        emit([doc.stream_resource_id,0],doc._id);\n    else if(doc.type_ == "StreamGranuleContainer")\n        emit([doc.stream_resource_id,1], doc._id);\n}\n'}, u'bounds': {u'map': u'\n/*\n * Author: Luke Campbell <lcampbell@asascience.com>\n * Description: Utility and map functions to map the DataContainer\'s bounding elements.\n */\n\n/*\n * Traverses an "identifiable" in a document to see if it contains a CoordinateAxis\n */\nfunction traverse(identifiable) {\n    if(identifiable.type_=="CoordinateAxis")\n    {\n        return identifiable.bounds_id;\n    }\n    else return null;\n}\n/*\n * Gets the CoordinateAxis objects and their bounds_ids\n */\nfunction get_bounds(doc)\n{\n    identifiables = doc.identifiables;\n    var bounds = [];\n    for(var i in identifiables)\n    {\n        var bounds_id = traverse(identifiables[i]);\n        if(bounds_id)\n            bounds.push(bounds_id);\n    }\n    return bounds;\n}\n\n/* Data map */\nfunction (doc) {\n    if(doc.type_ == "StreamGranuleContainer"){\n        var bounds = get_bounds(doc);\n        for(var b in bounds)\n        {\n            var key = bounds[b];\n            var s = String(key)\n            var pack = {};\n            pack[key] = doc.identifiables[key].value_pair;\n\n            emit([doc.stream_resource_id,1], pack);\n        }\n    }\n}\n\n', u'reduce': u'\n\nfunction value_in(value,ary) {\n    for(var i in ary)\n    {\n        if(value == ary[i])\n            return i;\n    }\n}\nfunction get_keys(obj){\n    var keys = [];\n    for(var key in obj)\n        keys.push(key);\n    return keys;\n}\nfunction print_dic(obj) {\n    for(var key in obj)\n    {\n        debug(key);\n        debug(obj[key]);\n    }\n}\n\nfunction(keys,values,rereduce) {\n    var keys = [];\n    var results = values[0];\n        \n    /* Not a rereduce so populate the results dictionary */\n\n\n    for(var i in values)\n    {\n        var key = get_keys(values[i])[0];\n        // The k,v pair is new, put it in results.\n        if(! value_in(key,keys))\n        {\n            keys.push(key);\n            results[key] = values[i][key];\n            continue;\n        }\n        var value = values[i][key];\n        /* minimum between the result and the new value */\n        if(value[0] < results[key][0])\n            results[key][0] = value[0];\n        if(value[1] > results[key][1])\n            results[key][1] = value[1];\n\n    }\n    return results;\n\n    \n    \n}'}}}
        ]
        for doc in docs:
            db.create(doc)
    def _populate2(self):

        self.couch = self.container.datastore_manager.get_datastore('test_replay_advanced', profile=DataStore.DS_PROFILE.SCIDATA)
        couch = self.couch.server
        db = couch[self.couch.datastore_name]

        docs = [{u'_id': u'110975FFA2B46E3324FBFE69CEB8A68446C326A2',
                 u'data_stream_id': u'ctd_data',
                 u'identifiables': {u'cndr_bounds': {u'axisID': u'',
                                                     u'constraint': {u'intervals': [],
                                                                     u'significant_figures': -1,
                                                                     u'type_': u'AllowedValues',
                                                                     u'values': []},
                                                     u'definition': u'',
                                                     u'description': u'',
                                                     u'id': u'',
                                                     u'label': u'',
                                                     u'nil_value_ids': [],
                                                     u'optional': False,
                                                     u'quality_id': u'',
                                                     u'reference_frame': u'',
                                                     u'type_': u'QuantityRangeElement',
                                                     u'unit_of_measure': {u'code': u'',
                                                                          u'reference': u'',
                                                                          u'type_': u'UnitReferenceProperty'},
                                                     u'updatable': True,
                                                     u'value_pair': [6.944138296667554,
                                                                     54.57866838767616]},
                                    u'cndr_data': {u'bounds_id': u'cndr_bounds',
                                                   u'constraint': {u'intervals': [],
                                                                   u'significant_figures': -1,
                                                                   u'type_': u'AllowedValues',
                                                                   u'values': []},
                                                   u'definition': u'',
                                                   u'description': u'',
                                                   u'id': u'',
                                                   u'label': u'',
                                                   u'mesh_location': {u'axisID': u'',
                                                                      u'code_space': u'',
                                                                      u'constraint': {u'pattern': u'',
                                                                                      u'type_': u'AllowedTokens',
                                                                                      u'values': []},
                                                                      u'definition': u'',
                                                                      u'description': u'',
                                                                      u'id': u'',
                                                                      u'label': u'',
                                                                      u'nil_value_ids': [],
                                                                      u'optional': False,
                                                                      u'quality_id': u'',
                                                                      u'reference_frame': u'',
                                                                      u'type_': u'CategoryElement',
                                                                      u'updatable': True,
                                                                      u'value': u''},
                                                   u'nil_values_ids': [],
                                                   u'optional': False,
                                                   u'quality_id': u'',
                                                   u'reference_frame': u'',
                                                   u'type_': u'RangeSet',
                                                   u'unit_of_measure': {u'code': u'',
                                                                        u'reference': u'',
                                                                        u'type_': u'UnitReferenceProperty'},
                                                   u'updatable': True,
                                                   u'values_path': u''},
                                    u'ctd_data': {u'description': u'',
                                                  u'element_count_id': u'',
                                                  u'element_type_id': u'',
                                                  u'encoding_id': u'',
                                                  u'id': u'0821e596cc4240c3b95401a47ddfbb2f',
                                                  u'label': u'',
                                                  u'type_': u'DataStream',
                                                  u'values': u''},
                                    u'latitude': {u'axis': {u'axisID': u'',
                                                            u'code_space': u'',
                                                            u'constraint': {u'pattern': u'',
                                                                            u'type_': u'AllowedTokens',
                                                                            u'values': []},
                                                            u'definition': u'',
                                                            u'description': u'',
                                                            u'id': u'',
                                                            u'label': u'',
                                                            u'nil_value_ids': [],
                                                            u'optional': False,
                                                            u'quality_id': u'',
                                                            u'reference_frame': u'',
                                                            u'type_': u'CategoryElement',
                                                            u'updatable': True,
                                                            u'value': u''},
                                                  u'bounds_id': u'latitude_bounds',
                                                  u'constraint': {u'intervals': [],
                                                                  u'significant_figures': -1,
                                                                  u'type_': u'AllowedValues',
                                                                  u'values': []},
                                                  u'definition': u'',
                                                  u'description': u'',
                                                  u'id': u'',
                                                  u'label': u'',
                                                  u'mesh_location': {u'axisID': u'',
                                                                     u'code_space': u'',
                                                                     u'constraint': {u'pattern': u'',
                                                                                     u'type_': u'AllowedTokens',
                                                                                     u'values': []},
                                                                     u'definition': u'',
                                                                     u'description': u'',
                                                                     u'id': u'',
                                                                     u'label': u'',
                                                                     u'nil_value_ids': [],
                                                                     u'optional': False,
                                                                     u'quality_id': u'',
                                                                     u'reference_frame': u'',
                                                                     u'type_': u'CategoryElement',
                                                                     u'updatable': True,
                                                                     u'value': u''},
                                                  u'nil_values_ids': [],
                                                  u'optional': False,
                                                  u'quality_id': u'',
                                                  u'reference_frame': u'',
                                                  u'type_': u'CoordinateAxis',
                                                  u'unit_of_measure': {u'code': u'',
                                                                       u'reference': u'',
                                                                       u'type_': u'UnitReferenceProperty'},
                                                  u'updatable': True,
                                                  u'values_path': u''},
                                    u'latitude_bounds': {u'axisID': u'',
                                                         u'constraint': {u'intervals': [],
                                                                         u'significant_figures': -1,
                                                                         u'type_': u'AllowedValues',
                                                                         u'values': []},
                                                         u'definition': u'',
                                                         u'description': u'',
                                                         u'id': u'',
                                                         u'label': u'',
                                                         u'nil_value_ids': [],
                                                         u'optional': False,
                                                         u'quality_id': u'',
                                                         u'reference_frame': u'',
                                                         u'type_': u'QuantityRangeElement',
                                                         u'unit_of_measure': {u'code': u'',
                                                                              u'reference': u'',
                                                                              u'type_': u'UnitReferenceProperty'},
                                                         u'updatable': True,
                                                         u'value_pair': [-44.480773912009205,
                                                                         22.986266307284936]},
                                    u'longitude': {u'axis': {u'axisID': u'',
                                                             u'code_space': u'',
                                                             u'constraint': {u'pattern': u'',
                                                                             u'type_': u'AllowedTokens',
                                                                             u'values': []},
                                                             u'definition': u'',
                                                             u'description': u'',
                                                             u'id': u'',
                                                             u'label': u'',
                                                             u'nil_value_ids': [],
                                                             u'optional': False,
                                                             u'quality_id': u'',
                                                             u'reference_frame': u'',
                                                             u'type_': u'CategoryElement',
                                                             u'updatable': True,
                                                             u'value': u''},
                                                   u'bounds_id': u'longitude_bounds',
                                                   u'constraint': {u'intervals': [],
                                                                   u'significant_figures': -1,
                                                                   u'type_': u'AllowedValues',
                                                                   u'values': []},
                                                   u'definition': u'',
                                                   u'description': u'',
                                                   u'id': u'',
                                                   u'label': u'',
                                                   u'mesh_location': {u'axisID': u'',
                                                                      u'code_space': u'',
                                                                      u'constraint': {u'pattern': u'',
                                                                                      u'type_': u'AllowedTokens',
                                                                                      u'values': []},
                                                                      u'definition': u'',
                                                                      u'description': u'',
                                                                      u'id': u'',
                                                                      u'label': u'',
                                                                      u'nil_value_ids': [],
                                                                      u'optional': False,
                                                                      u'quality_id': u'',
                                                                      u'reference_frame': u'',
                                                                      u'type_': u'CategoryElement',
                                                                      u'updatable': True,
                                                                      u'value': u''},
                                                   u'nil_values_ids': [],
                                                   u'optional': False,
                                                   u'quality_id': u'',
                                                   u'reference_frame': u'',
                                                   u'type_': u'CoordinateAxis',
                                                   u'unit_of_measure': {u'code': u'',
                                                                        u'reference': u'',
                                                                        u'type_': u'UnitReferenceProperty'},
                                                   u'updatable': True,
                                                   u'values_path': u''},
                                    u'longitude_bounds': {u'axisID': u'',
                                                          u'constraint': {u'intervals': [],
                                                                          u'significant_figures': -1,
                                                                          u'type_': u'AllowedValues',
                                                                          u'values': []},
                                                          u'definition': u'',
                                                          u'description': u'',
                                                          u'id': u'',
                                                          u'label': u'',
                                                          u'nil_value_ids': [],
                                                          u'optional': False,
                                                          u'quality_id': u'',
                                                          u'reference_frame': u'',
                                                          u'type_': u'QuantityRangeElement',
                                                          u'unit_of_measure': {u'code': u'',
                                                                               u'reference': u'',
                                                                               u'type_': u'UnitReferenceProperty'},
                                                          u'updatable': True,
                                                          u'value_pair': [1.0325383147336753,
                                                                          296.10451428103346]},
                                    u'pressure_bounds': {u'axisID': u'',
                                                         u'constraint': {u'intervals': [],
                                                                         u'significant_figures': -1,
                                                                         u'type_': u'AllowedValues',
                                                                         u'values': []},
                                                         u'definition': u'',
                                                         u'description': u'',
                                                         u'id': u'',
                                                         u'label': u'',
                                                         u'nil_value_ids': [],
                                                         u'optional': False,
                                                         u'quality_id': u'',
                                                         u'reference_frame': u'',
                                                         u'type_': u'QuantityRangeElement',
                                                         u'unit_of_measure': {u'code': u'',
                                                                              u'reference': u'',
                                                                              u'type_': u'UnitReferenceProperty'},
                                                         u'updatable': True,
                                                         u'value_pair': [0.040225054893218516,
                                                                         5.5499162078857625]},
                                    u'pressure_data': {u'axis': {u'axisID': u'',
                                                                 u'code_space': u'',
                                                                 u'constraint': {u'pattern': u'',
                                                                                 u'type_': u'AllowedTokens',
                                                                                 u'values': []},
                                                                 u'definition': u'',
                                                                 u'description': u'',
                                                                 u'id': u'',
                                                                 u'label': u'',
                                                                 u'nil_value_ids': [],
                                                                 u'optional': False,
                                                                 u'quality_id': u'',
                                                                 u'reference_frame': u'',
                                                                 u'type_': u'CategoryElement',
                                                                 u'updatable': True,
                                                                 u'value': u''},
                                                       u'bounds_id': u'pressure_bounds',
                                                       u'constraint': {u'intervals': [],
                                                                       u'significant_figures': -1,
                                                                       u'type_': u'AllowedValues',
                                                                       u'values': []},
                                                       u'definition': u'',
                                                       u'description': u'',
                                                       u'id': u'',
                                                       u'label': u'',
                                                       u'mesh_location': {u'axisID': u'',
                                                                          u'code_space': u'',
                                                                          u'constraint': {u'pattern': u'',
                                                                                          u'type_': u'AllowedTokens',
                                                                                          u'values': []},
                                                                          u'definition': u'',
                                                                          u'description': u'',
                                                                          u'id': u'',
                                                                          u'label': u'',
                                                                          u'nil_value_ids': [],
                                                                          u'optional': False,
                                                                          u'quality_id': u'',
                                                                          u'reference_frame': u'',
                                                                          u'type_': u'CategoryElement',
                                                                          u'updatable': True,
                                                                          u'value': u''},
                                                       u'nil_values_ids': [],
                                                       u'optional': False,
                                                       u'quality_id': u'',
                                                       u'reference_frame': u'',
                                                       u'type_': u'CoordinateAxis',
                                                       u'unit_of_measure': {u'code': u'',
                                                                            u'reference': u'',
                                                                            u'type_': u'UnitReferenceProperty'},
                                                       u'updatable': True,
                                                       u'values_path': u''},
                                    u'record_count': {u'axisID': u'',
                                                      u'constraint': {u'intervals': [],
                                                                      u'significant_figures': -1,
                                                                      u'type_': u'AllowedValues',
                                                                      u'values': []},
                                                      u'definition': u'',
                                                      u'description': u'',
                                                      u'id': u'',
                                                      u'label': u'',
                                                      u'nil_value_ids': [],
                                                      u'optional': False,
                                                      u'quality_id': u'',
                                                      u'reference_frame': u'',
                                                      u'type_': u'CountElement',
                                                      u'updatable': True,
                                                      u'value': 6},
                                    u'stream_encoding': {u'compression': None,
                                                         u'description': u'',
                                                         u'encoding_type': u'hdf5',
                                                         u'id': u'',
                                                         u'label': u'',
                                                         u'record_dimension': u'',
                                                         u'sha1': u'55CB1F4C4D34A9148971815CC8E1F9C93FA6CA8B',
                                                         u'type_': u'Encoding'},
                                    u'temp_bounds': {u'axisID': u'',
                                                     u'constraint': {u'intervals': [],
                                                                     u'significant_figures': -1,
                                                                     u'type_': u'AllowedValues',
                                                                     u'values': []},
                                                     u'definition': u'',
                                                     u'description': u'',
                                                     u'id': u'',
                                                     u'label': u'',
                                                     u'nil_value_ids': [],
                                                     u'optional': False,
                                                     u'quality_id': u'',
                                                     u'reference_frame': u'',
                                                     u'type_': u'QuantityRangeElement',
                                                     u'unit_of_measure': {u'code': u'',
                                                                          u'reference': u'',
                                                                          u'type_': u'UnitReferenceProperty'},
                                                     u'updatable': True,
                                                     u'value_pair': [-0.046320138917800735,
                                                                     16.458295796245906]},
                                    u'time': {u'axis': {u'axisID': u'',
                                                        u'code_space': u'',
                                                        u'constraint': {u'pattern': u'',
                                                                        u'type_': u'AllowedTokens',
                                                                        u'values': []},
                                                        u'definition': u'',
                                                        u'description': u'',
                                                        u'id': u'',
                                                        u'label': u'',
                                                        u'nil_value_ids': [],
                                                        u'optional': False,
                                                        u'quality_id': u'',
                                                        u'reference_frame': u'',
                                                        u'type_': u'CategoryElement',
                                                        u'updatable': True,
                                                        u'value': u''},
                                              u'bounds_id': u'time_bounds',
                                              u'constraint': {u'intervals': [],
                                                              u'significant_figures': -1,
                                                              u'type_': u'AllowedValues',
                                                              u'values': []},
                                              u'definition': u'',
                                              u'description': u'',
                                              u'id': u'',
                                              u'label': u'',
                                              u'mesh_location': {u'axisID': u'',
                                                                 u'code_space': u'',
                                                                 u'constraint': {u'pattern': u'',
                                                                                 u'type_': u'AllowedTokens',
                                                                                 u'values': []},
                                                                 u'definition': u'',
                                                                 u'description': u'',
                                                                 u'id': u'',
                                                                 u'label': u'',
                                                                 u'nil_value_ids': [],
                                                                 u'optional': False,
                                                                 u'quality_id': u'',
                                                                 u'reference_frame': u'',
                                                                 u'type_': u'CategoryElement',
                                                                 u'updatable': True,
                                                                 u'value': u''},
                                              u'nil_values_ids': [],
                                              u'optional': False,
                                              u'quality_id': u'',
                                              u'reference_frame': u'',
                                              u'type_': u'CoordinateAxis',
                                              u'unit_of_measure': {u'code': u'',
                                                                   u'reference': u'',
                                                                   u'type_': u'UnitReferenceProperty'},
                                              u'updatable': True,
                                              u'values_path': u''},
                                    u'time_bounds': {u'axisID': u'',
                                                     u'constraint': {u'intervals': [],
                                                                     u'significant_figures': -1,
                                                                     u'type_': u'AllowedValues',
                                                                     u'values': []},
                                                     u'definition': u'',
                                                     u'description': u'',
                                                     u'id': u'',
                                                     u'label': u'',
                                                     u'nil_value_ids': [],
                                                     u'optional': False,
                                                     u'quality_id': u'',
                                                     u'reference_frame': u'',
                                                     u'type_': u'QuantityRangeElement',
                                                     u'unit_of_measure': {u'code': u'',
                                                                          u'reference': u'',
                                                                          u'type_': u'UnitReferenceProperty'},
                                                     u'updatable': True,
                                                     u'value_pair': [34, 39]}},
                 u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f',
                 u'type_': u'StreamGranuleContainer'}
            ,
                {u'_id': u'27368C7D7BB3B653811A4514E58A6FB1E4ACA71A',
                 u'data_stream_id': u'ctd_data',
                 u'identifiables': {u'record_count': {u'axisID': u'',
                                                      u'constraint': {u'intervals': [],
                                                                      u'significant_figures': -1,
                                                                      u'type_': u'AllowedValues',
                                                                      u'values': []},
                                                      u'definition': u'',
                                                      u'description': u'',
                                                      u'id': u'',
                                                      u'label': u'',
                                                      u'nil_value_ids': [],
                                                      u'optional': False,
                                                      u'quality_id': u'',
                                                      u'reference_frame': u'',
                                                      u'type_': u'CountElement',
                                                      u'updatable': True,
                                                      u'value': 9},
                                    u'ctd_data': {u'description': u'',
                                                  u'element_count_id': u'',
                                                  u'element_type_id': u'',
                                                  u'encoding_id': u'',
                                                  u'id': u'0821e596cc4240c3b95401a47ddfbb2f',
                                                  u'label': u'',
                                                  u'type_': u'DataStream',
                                                  u'values': u''},
                                    u'stream_encoding': {u'compression': None,
                                                         u'description': u'',
                                                         u'encoding_type': u'hdf5',
                                                         u'id': u'',
                                                         u'label': u'',
                                                         u'record_dimension': u'',
                                                         u'sha1': u'590F7E0A26D535738FF5B728CFC0073D71F3CF9D',
                                                         u'type_': u'Encoding'},
                                    u'temp_bounds': {u'axisID': u'',
                                                     u'constraint': {u'intervals': [],
                                                                     u'significant_figures': -1,
                                                                     u'type_': u'AllowedValues',
                                                                     u'values': []},
                                                     u'definition': u'',
                                                     u'description': u'',
                                                     u'id': u'',
                                                     u'label': u'',
                                                     u'nil_value_ids': [],
                                                     u'optional': False,
                                                     u'quality_id': u'',
                                                     u'reference_frame': u'',
                                                     u'type_': u'QuantityRangeElement',
                                                     u'unit_of_measure': {u'code': u'',
                                                                          u'reference': u'',
                                                                          u'type_': u'UnitReferenceProperty'},
                                                     u'updatable': True,
                                                     u'value_pair': [-1.1401749695070225,
                                                                     20.565461001195967]},
                                    u'temp_data': {u'bounds_id': [u'temp_bounds'],
                                                   u'constraint': {u'intervals': [],
                                                                   u'significant_figures': -1,
                                                                   u'type_': u'AllowedValues',
                                                                   u'values': []},
                                                   u'definition': u'',
                                                   u'description': u'',
                                                   u'id': u'',
                                                   u'label': u'',
                                                   u'mesh_location': {u'axisID': u'',
                                                                      u'code_space': u'',
                                                                      u'constraint': {u'pattern': u'',
                                                                                      u'type_': u'AllowedTokens',
                                                                                      u'values': []},
                                                                      u'definition': u'',
                                                                      u'description': u'',
                                                                      u'id': u'',
                                                                      u'label': u'',
                                                                      u'nil_value_ids': [],
                                                                      u'optional': False,
                                                                      u'quality_id': u'',
                                                                      u'reference_frame': u'',
                                                                      u'type_': u'CategoryElement',
                                                                      u'updatable': True,
                                                                      u'value': u''},
                                                   u'nil_values_ids': [],
                                                   u'optional': False,
                                                   u'quality_id': u'',
                                                   u'reference_frame': u'',
                                                   u'type_': u'RangeSet',
                                                   u'unit_of_measure': {u'code': u'',
                                                                        u'reference': u'',
                                                                        u'type_': u'UnitReferenceProperty'},
                                                   u'updatable': True,
                                                   u'values_path': u'temperature'},
                                    u'time': {u'axis': {u'axisID': u'',
                                                        u'code_space': u'',
                                                        u'constraint': {u'pattern': u'',
                                                                        u'type_': u'AllowedTokens',
                                                                        u'values': []},
                                                        u'definition': u'',
                                                        u'description': u'',
                                                        u'id': u'',
                                                        u'label': u'',
                                                        u'nil_value_ids': [],
                                                        u'optional': False,
                                                        u'quality_id': u'',
                                                        u'reference_frame': u'',
                                                        u'type_': u'CategoryElement',
                                                        u'updatable': True,
                                                        u'value': u''},
                                              u'bounds_id': u'time_bounds',
                                              u'constraint': {u'intervals': [],
                                                              u'significant_figures': -1,
                                                              u'type_': u'AllowedValues',
                                                              u'values': []},
                                              u'definition': u'',
                                              u'description': u'',
                                              u'id': u'',
                                              u'label': u'',
                                              u'mesh_location': {u'axisID': u'',
                                                                 u'code_space': u'',
                                                                 u'constraint': {u'pattern': u'',
                                                                                 u'type_': u'AllowedTokens',
                                                                                 u'values': []},
                                                                 u'definition': u'',
                                                                 u'description': u'',
                                                                 u'id': u'',
                                                                 u'label': u'',
                                                                 u'nil_value_ids': [],
                                                                 u'optional': False,
                                                                 u'quality_id': u'',
                                                                 u'reference_frame': u'',
                                                                 u'type_': u'CategoryElement',
                                                                 u'updatable': True,
                                                                 u'value': u''},
                                              u'nil_values_ids': [],
                                              u'optional': False,
                                              u'quality_id': u'',
                                              u'reference_frame': u'',
                                              u'type_': u'CoordinateAxis',
                                              u'unit_of_measure': {u'code': u'',
                                                                   u'reference': u'',
                                                                   u'type_': u'UnitReferenceProperty'},
                                              u'updatable': True,
                                              u'values_path': u''}},
                 u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f',
                 u'type_': u'StreamGranuleContainer'}
            ,
                {u'_id': u'2D05B37809E84AACFB547CBF8D4D35466FEDB94B',
                 u'data_stream_id': u'ctd_data',
                 u'identifiables': {u'record_count': {u'axisID': u'',
                                                      u'constraint': {u'intervals': [],
                                                                      u'significant_figures': -1,
                                                                      u'type_': u'AllowedValues',
                                                                      u'values': []},
                                                      u'definition': u'',
                                                      u'description': u'',
                                                      u'id': u'',
                                                      u'label': u'',
                                                      u'nil_value_ids': [],
                                                      u'optional': False,
                                                      u'quality_id': u'',
                                                      u'reference_frame': u'',
                                                      u'type_': u'CountElement',
                                                      u'updatable': True,
                                                      u'value': 5},
                                    u'stream_encoding': {u'compression': None,
                                                         u'description': u'',
                                                         u'encoding_type': u'hdf5',
                                                         u'id': u'',
                                                         u'label': u'',
                                                         u'record_dimension': u'',
                                                         u'sha1': u'F4C2D691F80BFB05481E172E5768F4329A5C7692',
                                                         u'type_': u'Encoding'},
                                    u'ctd_data': {u'description': u'',
                                                  u'element_count_id': u'',
                                                  u'element_type_id': u'',
                                                  u'encoding_id': u'',
                                                  u'id': u'0821e596cc4240c3b95401a47ddfbb2f',
                                                  u'label': u'',
                                                  u'type_': u'DataStream',
                                                  u'values': u''},
                                    u'temp_bounds': {u'axisID': u'',
                                                     u'constraint': {u'intervals': [],
                                                                     u'significant_figures': -1,
                                                                     u'type_': u'AllowedValues',
                                                                     u'values': []},
                                                     u'definition': u'',
                                                     u'description': u'',
                                                     u'id': u'',
                                                     u'label': u'',
                                                     u'nil_value_ids': [],
                                                     u'optional': False,
                                                     u'quality_id': u'',
                                                     u'reference_frame': u'',
                                                     u'type_': u'QuantityRangeElement',
                                                     u'unit_of_measure': {u'code': u'',
                                                                          u'reference': u'',
                                                                          u'type_': u'UnitReferenceProperty'},
                                                     u'updatable': True,
                                                     u'value_pair': [1.9702501679981628,
                                                                     19.025763915299958]},
                                    u'temp_data': {u'bounds_id': [u'temp_bounds'],
                                                   u'constraint': {u'intervals': [],
                                                                   u'significant_figures': -1,
                                                                   u'type_': u'AllowedValues',
                                                                   u'values': []},
                                                   u'definition': u'',
                                                   u'description': u'',
                                                   u'id': u'',
                                                   u'label': u'',
                                                   u'mesh_location': {u'axisID': u'',
                                                                      u'code_space': u'',
                                                                      u'constraint': {u'pattern': u'',
                                                                                      u'type_': u'AllowedTokens',
                                                                                      u'values': []},
                                                                      u'definition': u'',
                                                                      u'description': u'',
                                                                      u'id': u'',
                                                                      u'label': u'',
                                                                      u'nil_value_ids': [],
                                                                      u'optional': False,
                                                                      u'quality_id': u'',
                                                                      u'reference_frame': u'',
                                                                      u'type_': u'CategoryElement',
                                                                      u'updatable': True,
                                                                      u'value': u''},
                                                   u'nil_values_ids': [],
                                                   u'optional': False,
                                                   u'quality_id': u'',
                                                   u'reference_frame': u'',
                                                   u'type_': u'RangeSet',
                                                   u'unit_of_measure': {u'code': u'',
                                                                        u'reference': u'',
                                                                        u'type_': u'UnitReferenceProperty'},
                                                   u'updatable': True,
                                                   u'values_path': u'temperature'}},
                 u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f',
                 u'type_': u'StreamGranuleContainer'}
            ,
                {u'_id': u'BA520236C5B4682AAB9EC64797C08B32B879410B',
                 u'data_stream_id': u'ctd_data',
                 u'identifiables': {u'pressure_bounds': {u'axisID': u'',
                                                         u'constraint': {u'intervals': [],
                                                                         u'significant_figures': -1,
                                                                         u'type_': u'AllowedValues',
                                                                         u'values': []},
                                                         u'definition': u'',
                                                         u'description': u'',
                                                         u'id': u'',
                                                         u'label': u'',
                                                         u'nil_value_ids': [],
                                                         u'optional': False,
                                                         u'quality_id': u'',
                                                         u'reference_frame': u'',
                                                         u'type_': u'QuantityRangeElement',
                                                         u'unit_of_measure': {u'code': u'',
                                                                              u'reference': u'',
                                                                              u'type_': u'UnitReferenceProperty'},
                                                         u'updatable': True,
                                                         u'value_pair': [0.2923022238297028,
                                                                         44.479318986212235]},
                                    u'ctd_data': {u'description': u'',
                                                  u'element_count_id': u'',
                                                  u'element_type_id': u'',
                                                  u'encoding_id': u'',
                                                  u'id': u'0821e596cc4240c3b95401a47ddfbb2f',
                                                  u'label': u'',
                                                  u'type_': u'DataStream',
                                                  u'values': u''},
                                    u'pressure_data': {u'axis': {u'axisID': u'',
                                                                 u'code_space': u'',
                                                                 u'constraint': {u'pattern': u'',
                                                                                 u'type_': u'AllowedTokens',
                                                                                 u'values': []},
                                                                 u'definition': u'',
                                                                 u'description': u'',
                                                                 u'id': u'',
                                                                 u'label': u'',
                                                                 u'nil_value_ids': [],
                                                                 u'optional': False,
                                                                 u'quality_id': u'',
                                                                 u'reference_frame': u'',
                                                                 u'type_': u'CategoryElement',
                                                                 u'updatable': True,
                                                                 u'value': u''},
                                                       u'bounds_id': u'pressure_bounds',
                                                       u'constraint': {u'intervals': [],
                                                                       u'significant_figures': -1,
                                                                       u'type_': u'AllowedValues',
                                                                       u'values': []},
                                                       u'definition': u'',
                                                       u'description': u'',
                                                       u'id': u'',
                                                       u'label': u'',
                                                       u'mesh_location': {u'axisID': u'',
                                                                          u'code_space': u'',
                                                                          u'constraint': {u'pattern': u'',
                                                                                          u'type_': u'AllowedTokens',
                                                                                          u'values': []},
                                                                          u'definition': u'',
                                                                          u'description': u'',
                                                                          u'id': u'',
                                                                          u'label': u'',
                                                                          u'nil_value_ids': [],
                                                                          u'optional': False,
                                                                          u'quality_id': u'',
                                                                          u'reference_frame': u'',
                                                                          u'type_': u'CategoryElement',
                                                                          u'updatable': True,
                                                                          u'value': u''},
                                                       u'nil_values_ids': [],
                                                       u'optional': False,
                                                       u'quality_id': u'',
                                                       u'reference_frame': u'',
                                                       u'type_': u'CoordinateAxis',
                                                       u'unit_of_measure': {u'code': u'',
                                                                            u'reference': u'',
                                                                            u'type_': u'UnitReferenceProperty'},
                                                       u'updatable': True,
                                                       u'values_path': u''},
                                    u'record_count': {u'axisID': u'',
                                                      u'constraint': {u'intervals': [],
                                                                      u'significant_figures': -1,
                                                                      u'type_': u'AllowedValues',
                                                                      u'values': []},
                                                      u'definition': u'',
                                                      u'description': u'',
                                                      u'id': u'',
                                                      u'label': u'',
                                                      u'nil_value_ids': [],
                                                      u'optional': False,
                                                      u'quality_id': u'',
                                                      u'reference_frame': u'',
                                                      u'type_': u'CountElement',
                                                      u'updatable': True,
                                                      u'value': 30},
                                    u'stream_encoding': {u'compression': None,
                                                         u'description': u'',
                                                         u'encoding_type': u'hdf5',
                                                         u'id': u'',
                                                         u'label': u'',
                                                         u'record_dimension': u'',
                                                         u'sha1': u'2C609FAD036620B6229624D0905310E87296E6D9',
                                                         u'type_': u'Encoding'}},
                 u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f',
                 u'type_': u'StreamGranuleContainer'}
            ,

                {u'_id': u'bf2d718627b54a65bd9542e75a918497',
                 u'data_stream_id': u'ctd_data',
                 u'identifiables': {u'cndr_data': {u'bounds_id': u'',
                                                   u'constraint': {u'intervals': [],
                                                                   u'significant_figures': -1,
                                                                   u'type_': u'AllowedValues',
                                                                   u'values': [[0.0, 85.0]]},
                                                   u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Conductivity',
                                                   u'description': u'',
                                                   u'id': u'',
                                                   u'label': u'',
                                                   u'mesh_location': {u'axisID': u'',
                                                                      u'code_space': u'',
                                                                      u'constraint': {u'pattern': u'',
                                                                                      u'type_': u'AllowedTokens',
                                                                                      u'values': []},
                                                                      u'definition': u'',
                                                                      u'description': u'',
                                                                      u'id': u'',
                                                                      u'label': u'',
                                                                      u'nil_value_ids': [],
                                                                      u'optional': False,
                                                                      u'quality_id': u'',
                                                                      u'reference_frame': u'',
                                                                      u'type_': u'CategoryElement',
                                                                      u'updatable': True,
                                                                      u'value': u'vertex'},
                                                   u'nil_values_ids': [u'nan_value'],
                                                   u'optional': False,
                                                   u'quality_id': u'',
                                                   u'reference_frame': u'',
                                                   u'type_': u'RangeSet',
                                                   u'unit_of_measure': {u'code': u'mS/cm',
                                                                        u'reference': u'',
                                                                        u'type_': u'UnitReferenceProperty'},
                                                   u'updatable': True,
                                                   u'values_path': u'/fields/cndr_data'},
                                    u'conductivity': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Conductivity',
                                                      u'description': u'',
                                                      u'domain_id': u'time_domain',
                                                      u'id': u'',
                                                      u'label': u'',
                                                      u'optional': True,
                                                      u'range_id': u'cndr_data',
                                                      u'type_': u'Coverage',
                                                      u'updatable': False},

                                    u'coordinate_vector': {u'coordinate_ids': [u'longitude_data',
                                                                               u'latitude_data',
                                                                               u'pressure_data',
                                                                               u'time_data'],
                                                           u'definition': u'http://sweet.jpl.nasa.gov/2.0/space.owl#Location',
                                                           u'description': u'',
                                                           u'id': u'',
                                                           u'label': u'',
                                                           u'local_frame': u'',
                                                           u'optional': False,
                                                           u'reference_frame': u'http://www.opengis.net/def/crs/EPSG/0/4326',
                                                           u'type_': u'Vector',
                                                           u'updatable': True},
                                    u'ctd_data': {u'description': u'Conductivity temperature and depth observations from a Seabird CTD',
                                                  u'element_count_id': u'record_count',
                                                  u'element_type_id': u'element_type',
                                                  u'encoding_id': u'stream_encoding',
                                                  u'id': u'',
                                                  u'label': u'Seabird CTD Data',
                                                  u'type_': u'DataStream',
                                                  u'values': None},
                                    u'data_record': {u'definition': u'Definition of a data record for a CTD',
                                                     u'description': u'',
                                                     u'domain_ids': [u'time_domain'],
                                                     u'field_ids': [u'temperature',
                                                                    u'conductivity',
                                                                    u'pressure',
                                                                    u'latitude',
                                                                    u'longitude',
                                                                    u'time'],
                                                     u'id': u'',
                                                     u'label': u'',
                                                     u'optional': False,
                                                     u'type_': u'DataRecord',
                                                     u'updatable': False},
                                    u'element_type': {u'data_record_id': u'data_record',
                                                      u'definition': u'Ref to SeaBird data?',
                                                      u'description': u'',
                                                      u'id': u'',
                                                      u'label': u'',
                                                      u'optional': False,
                                                      u'type_': u'ElementType',
                                                      u'updatable': False},
                                    u'latitude': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/spaceCoordinates.owl#Latitude',
                                                  u'description': u'',
                                                  u'domain_id': u'time_domain',
                                                  u'id': u'',
                                                  u'label': u'',
                                                  u'optional': True,
                                                  u'range_id': u'latitude_data',
                                                  u'type_': u'Coverage',
                                                  u'updatable': False},
                                    u'latitude_data': {u'axis': u'Latitude',
                                                       u'bounds_id': u'',
                                                       u'constraint': {u'intervals': [],
                                                                       u'significant_figures': -1,
                                                                       u'type_': u'AllowedValues',
                                                                       u'values': [[-90.0,
                                                                                    90.0]]},
                                                       u'definition': u'http://sweet.jpl.nasa.gov/2.0/spaceCoordinates.owl#Latitude',
                                                       u'description': u'',
                                                       u'id': u'',
                                                       u'label': u'',
                                                       u'mesh_location': {u'axisID': u'',
                                                                          u'code_space': u'',
                                                                          u'constraint': {u'pattern': u'',
                                                                                          u'type_': u'AllowedTokens',
                                                                                          u'values': []},
                                                                          u'definition': u'',
                                                                          u'description': u'',
                                                                          u'id': u'',
                                                                          u'label': u'',
                                                                          u'nil_value_ids': [],
                                                                          u'optional': False,
                                                                          u'quality_id': u'',
                                                                          u'reference_frame': u'',
                                                                          u'type_': u'CategoryElement',
                                                                          u'updatable': True,
                                                                          u'value': u'vertex'},
                                                       u'nil_values_ids': [u'nan_value'],
                                                       u'optional': False,
                                                       u'quality_id': u'',
                                                       u'reference_frame': u'',
                                                       u'type_': u'CoordinateAxis',
                                                       u'unit_of_measure': {u'code': u'deg',
                                                                            u'reference': u'',
                                                                            u'type_': u'UnitReferenceProperty'},
                                                       u'updatable': True,
                                                       u'values_path': u'/fields/latitude'},
                                    u'longitude': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/spaceCoordinates.owl#longitude',
                                                   u'description': u'',
                                                   u'domain_id': u'time_domain',
                                                   u'id': u'',
                                                   u'label': u'',
                                                   u'optional': True,
                                                   u'range_id': u'longitude_data',
                                                   u'type_': u'Coverage',
                                                   u'updatable': False},
                                    u'longitude_data': {u'axis': u'Longitude',
                                                        u'bounds_id': u'',
                                                        u'constraint': {u'intervals': [],
                                                                        u'significant_figures': -1,
                                                                        u'type_': u'AllowedValues',
                                                                        u'values': [[0.0,
                                                                                     360.0]]},
                                                        u'definition': u'http://sweet.jpl.nasa.gov/2.0/spaceCoordinates.owl#Longitude',
                                                        u'description': u'',
                                                        u'id': u'',
                                                        u'label': u'',
                                                        u'mesh_location': {u'axisID': u'',
                                                                           u'code_space': u'',
                                                                           u'constraint': {u'pattern': u'',
                                                                                           u'type_': u'AllowedTokens',
                                                                                           u'values': []},
                                                                           u'definition': u'',
                                                                           u'description': u'',
                                                                           u'id': u'',
                                                                           u'label': u'',
                                                                           u'nil_value_ids': [],
                                                                           u'optional': False,
                                                                           u'quality_id': u'',
                                                                           u'reference_frame': u'',
                                                                           u'type_': u'CategoryElement',
                                                                           u'updatable': True,
                                                                           u'value': u'vertex'},
                                                        u'nil_values_ids': [u'nan_value'],
                                                        u'optional': False,
                                                        u'quality_id': u'',
                                                        u'reference_frame': u'',
                                                        u'type_': u'CoordinateAxis',
                                                        u'unit_of_measure': {u'code': u'deg',
                                                                             u'reference': u'',
                                                                             u'type_': u'UnitReferenceProperty'},
                                                        u'updatable': True,
                                                        u'values_path': u'/fields/longitude'},
                                    u'nan_value': {u'reason': u'No value recorded',
                                                   u'type_': u'NilValue',
                                                   u'value': -999.99},
                                    u'point_timeseries': {u'definition': u'',
                                                          u'description': u'',
                                                          u'id': u'',
                                                          u'index_offset': 0,
                                                          u'label': u'',
                                                          u'mesh_type': {u'axisID': u'',
                                                                         u'code_space': u'',
                                                                         u'constraint': {u'pattern': u'',
                                                                                         u'type_': u'AllowedTokens',
                                                                                         u'values': []},
                                                                         u'definition': u'',
                                                                         u'description': u'',
                                                                         u'id': u'',
                                                                         u'label': u'',
                                                                         u'nil_value_ids': [],
                                                                         u'optional': False,
                                                                         u'quality_id': u'',
                                                                         u'reference_frame': u'',
                                                                         u'type_': u'CategoryElement',
                                                                         u'updatable': True,
                                                                         u'value': u'Point Time Series'},
                                                          u'number_of_elements': 1,
                                                          u'number_of_verticies': 1,
                                                          u'optional': False,
                                                          u'type_': u'Mesh',
                                                          u'updatable': True,
                                                          u'values_path': u'/topology/mesh'},
                                    u'pressure': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Pressure',
                                                  u'description': u'',
                                                  u'domain_id': u'time_domain',
                                                  u'id': u'',
                                                  u'label': u'',
                                                  u'optional': True,
                                                  u'range_id': u'pressure_data',
                                                  u'type_': u'Coverage',
                                                  u'updatable': False},
                                    u'pressure_data': {u'axis': u'Pressure',
                                                       u'bounds_id': u'',
                                                       u'constraint': {u'intervals': [],
                                                                       u'significant_figures': -1,
                                                                       u'type_': u'AllowedValues',
                                                                       u'values': [[0,
                                                                                    10000.0]]},
                                                       u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Pressure',
                                                       u'description': u'',
                                                       u'id': u'',
                                                       u'label': u'',
                                                       u'mesh_location': {u'axisID': u'',
                                                                          u'code_space': u'',
                                                                          u'constraint': {u'pattern': u'',
                                                                                          u'type_': u'AllowedTokens',
                                                                                          u'values': []},
                                                                          u'definition': u'',
                                                                          u'description': u'',
                                                                          u'id': u'',
                                                                          u'label': u'',
                                                                          u'nil_value_ids': [],
                                                                          u'optional': False,
                                                                          u'quality_id': u'',
                                                                          u'reference_frame': u'',
                                                                          u'type_': u'CategoryElement',
                                                                          u'updatable': True,
                                                                          u'value': u'vertex'},
                                                       u'nil_values_ids': [u'nan_value'],
                                                       u'optional': False,
                                                       u'quality_id': u'',
                                                       u'reference_frame': u'Atmospheric pressure ?',
                                                       u'type_': u'CoordinateAxis',
                                                       u'unit_of_measure': {u'code': u'dbar',
                                                                            u'reference': u'',
                                                                            u'type_': u'UnitReferenceProperty'},
                                                       u'updatable': True,
                                                       u'values_path': u'/fields/pressure_data'},
                                    u'record_count': {u'axisID': u'',
                                                      u'constraint': {u'intervals': [],
                                                                      u'significant_figures': -1,
                                                                      u'type_': u'AllowedValues',
                                                                      u'values': []},
                                                      u'definition': u'',
                                                      u'description': u'',
                                                      u'id': u'',
                                                      u'label': u'',
                                                      u'nil_value_ids': [],
                                                      u'optional': False,
                                                      u'quality_id': u'',
                                                      u'reference_frame': u'',
                                                      u'type_': u'CountElement',
                                                      u'updatable': True,
                                                      u'value': 0},
                                    u'stream_encoding': {u'compression': None,
                                                         u'description': u'',
                                                         u'encoding_type': u'hdf5',
                                                         u'id': u'',
                                                         u'label': u'',
                                                         u'record_dimension': u'',
                                                         u'sha1': None,
                                                         u'type_': u'Encoding'},
                                    u'temp_data': {u'bounds_id': u'',
                                                   u'constraint': {u'intervals': [],
                                                                   u'significant_figures': -1,
                                                                   u'type_': u'AllowedValues',
                                                                   u'values': [[-10.0,
                                                                                50.0]]},
                                                   u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Temperature',
                                                   u'description': u'',
                                                   u'id': u'',
                                                   u'label': u'',
                                                   u'mesh_location': {u'axisID': u'',
                                                                      u'code_space': u'',
                                                                      u'constraint': {u'pattern': u'',
                                                                                      u'type_': u'AllowedTokens',
                                                                                      u'values': []},
                                                                      u'definition': u'',
                                                                      u'description': u'',
                                                                      u'id': u'',
                                                                      u'label': u'',
                                                                      u'nil_value_ids': [],
                                                                      u'optional': False,
                                                                      u'quality_id': u'',
                                                                      u'reference_frame': u'',
                                                                      u'type_': u'CategoryElement',
                                                                      u'updatable': True,
                                                                      u'value': u'vertex'},
                                                   u'nil_values_ids': [u'nan_value'],
                                                   u'optional': False,
                                                   u'quality_id': u'',
                                                   u'reference_frame': u'',
                                                   u'type_': u'RangeSet',
                                                   u'unit_of_measure': {u'code': u'Cel',
                                                                        u'reference': u'',
                                                                        u'type_': u'UnitReferenceProperty'},
                                                   u'updatable': True,
                                                   u'values_path': u'/fields/temp_data'},
                                    u'temperature': {u'definition': u'http://sweet.jpl.nasa.gov/2.0/physThermo.owl#Temperature',
                                                     u'description': u'',
                                                     u'domain_id': u'time_domain',
                                                     u'id': u'',
                                                     u'label': u'',
                                                     u'optional': True,
                                                     u'range_id': u'temp_data',
                                                     u'type_': u'Coverage',
                                                     u'updatable': False},
                                    u'time': {u'definition': u'http://www.opengis.net/def/property/OGC/0/SamplingTime',
                                              u'description': u'',
                                              u'domain_id': u'time_domain',
                                              u'id': u'',
                                              u'label': u'',
                                              u'optional': True,
                                              u'range_id': u'time_data',
                                              u'type_': u'Coverage',
                                              u'updatable': False},
                                    u'time_data': {u'axis': u'Time',
                                                   u'bounds_id': u'',
                                                   u'constraint': {u'intervals': [],
                                                                   u'significant_figures': -1,
                                                                   u'type_': u'AllowedValues',
                                                                   u'values': []},
                                                   u'definition': u'http://www.opengis.net/def/property/OGC/0/SamplingTime',
                                                   u'description': u'',
                                                   u'id': u'',
                                                   u'label': u'',
                                                   u'mesh_location': {u'axisID': u'',
                                                                      u'code_space': u'',
                                                                      u'constraint': {u'pattern': u'',
                                                                                      u'type_': u'AllowedTokens',
                                                                                      u'values': []},
                                                                      u'definition': u'',
                                                                      u'description': u'',
                                                                      u'id': u'',
                                                                      u'label': u'',
                                                                      u'nil_value_ids': [],
                                                                      u'optional': False,
                                                                      u'quality_id': u'',
                                                                      u'reference_frame': u'',
                                                                      u'type_': u'CategoryElement',
                                                                      u'updatable': True,
                                                                      u'value': u'vertex'},
                                                   u'nil_values_ids': [u'nan_value'],
                                                   u'optional': False,
                                                   u'quality_id': u'',
                                                   u'reference_frame': u'http://www.opengis.net/def/trs/OGC/0/GPS',
                                                   u'type_': u'CoordinateAxis',
                                                   u'unit_of_measure': {u'code': u's',
                                                                        u'reference': u'',
                                                                        u'type_': u'UnitReferenceProperty'},
                                                   u'updatable': True,
                                                   u'values_path': u'/fields/time'},
                                    u'time_domain': {u'coordinate_vector_id': u'coordinate_vector',
                                                     u'definition': u'Spec for ctd data time domain',
                                                     u'description': u'',
                                                     u'id': u'',
                                                     u'label': u'',
                                                     u'mesh_id': u'point_timeseries',
                                                     u'optional': u'False',
                                                     u'type_': u'Domain',
                                                     u'updatable': u'False'}},
                 u'stream_resource_id': u'0821e596cc4240c3b95401a47ddfbb2f',
                 u'type_': u'StreamDefinitionContainer'}]
        for doc in docs:
            db.create(doc)

    def _generate_hdf_files(self):
        '''
        This generates three known good hdf files
        '''
        import gzip
        import base64
        files={'590F7E0A26D535738FF5B728CFC0073D71F3CF9D.hdf5':
                   """H4sICAduTk8AAzU5MEY3RTBBMjZENTM1NzM4RkY1QjcyOENGQzAwNzNENzFGM0NG
        OUQuaGRmNQDr9HBx4+WS4mIAAQ4OBhYGAQZk8B8KWjhQ+TD5BCjNCKU7oPQKJpg4
        I1hOAiouCDUfXV1IkKsrSPV/NACzx4AFQnMwjIKRCDxcHQNAdASUD0tPJ5hQ1ZWk
        5hakFiWWlBalgvmwdOlApr2MDGwQMxhhfIjNjIwQAU6oOhjNDJQHyQgqyDNAkqwC
        gzgHg3g9VJ4VmIJB8kxMTGADOKDqmRkToElbBcV+IWiKB6lhz/PzZ2CoINMn1AHB
        fv4uoBwNy4cKzAPqnFEwCkbBoAYN9gzg4tcBiBuAeAEQHwDiB0DM4MjAIOAIAO36
        NGyECAAA""",
               'F4C2D691F80BFB05481E172E5768F4329A5C7692.hdf5':
                   """H4sICHlvTk8AA0Y0QzJENjkxRjgwQkZCMDU0ODFFMTcyRTU3NjhGNDMyOUE1Qzc2
        OTIuaGRmNQDr9HBx4+WS4mIAAQ4OBhYGAQZk8B8KSjhQ+TD5BCjNCKU7oPQKJpg4
        I1hOAiouCDUfXV1IkKsrSPV/NACzx4AFQnMwjIKRCDxcHQNAdASUD0tPJ5hQ1ZWk
        5hakFiWWlBalgvmwdOlApr2MDGwQMxhhfIjNjIwQAVaoOhjNDJQHyQgqyDNAkqwC
        gzgHg3g9XB0HWJ6JiQlsAAdUPTNjAjRpi6DYLwRN8SA1lfl+/gwMFWT6hDog2M/f
        BZSjYflQgXlAnTMKRsEoGNRAwRFYewOxAxAHAHGCIwDpp0VkdAgAAA==""",
               '2C609FAD036620B6229624D0905310E87296E6D9.hdf5':
                   """H4sICJdwTk8AAzJDNjA5RkFEMDM2NjIwQjYyMjk2MjREMDkwNTMxMEU4NzI5NkU2
        RDkuaGRmNQDtlDFrwlAQx++9WHkIBe2iOGg+gt+gCZjipFIdHLO4B0Xo1ox269aO
        jo4dOzo6dnR07Lew/5d3B01GBy00f/hx3P2Puwz38jLoP9zW2jWyMoYqVKffOrEO
        Jp+LH3NUHNcct1rqKvNaXG/w/GLf9DGKbPepINnTq7hoqNR/1CAKxzbOOJd72ut8
        X7KYL5erxVxyucvgzL2Kqm6GktxtVsoVOtwn0YNvnYbfJXeyPjUNNZ/Zv8EFW19r
        nQ0w3O+pmE/7Kbf/ji/e9rwlw1HRv7Qmw1Hfvmh5h7531c8pVarUn1d6T9kvOAAp
        2IAdOAIKierABz0QgDGIQQJSsAav4B1swBZ8gE+wA3vwBQ7gCL7DH8S+pfbYCAAA
        """}
        for k,v in files.iteritems():
            f = open(FileSystem.get_url(FS.TEMP,'%s.gzip' % k),'w')
            f.write(base64.b64decode(v))
            f.close()
            f = gzip.open(FileSystem.get_url(FS.TEMP,'%s.gzip' % k), 'r')
            out = open(FileSystem.get_url(FS.TEMP,'%s' % k), 'w')
            out.write(f.read())
            f.close()
            out.close()
            FileSystem.unlink(FileSystem.get_url(FS.TEMP,'%s.gzip' % k))


    def tearDown(self):
        super(DataRetrieverServiceIntTest,self).tearDown()


    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_define_replay(self):
        dataset_id = self.dsm_cli.create_dataset(
            stream_id='12345',
            datastore_name=self.datastore_name,
            view_name='posts/posts_join_comments',
            name='test define replay'
        )
        replay_id, stream_id = self.dr_cli.define_replay(dataset_id=dataset_id)

        replay = self.rr_cli.read(replay_id)

        # Assert that the process was created

        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        self.dr_cli.cancel_replay(replay_id)

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_cancel_replay(self):
        dataset_id = self.dsm_cli.create_dataset(
            stream_id='12345',
            datastore_name=self.datastore_name,
            view_name='posts/posts_join_comments',
            name='test define replay'
        )
        replay_id, stream_id = self.dr_cli.define_replay(dataset_id=dataset_id)

        replay = self.rr_cli.read(replay_id)

        # Assert that the process was created

        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        self.dr_cli.cancel_replay(replay_id)

        # assert that the process is no more
        self.assertFalse(replay.process_id in self.container.proc_manager.procs)

        # assert that the resource no longer exists
        with self.assertRaises(NotFound):
            self.rr_cli.read(replay_id)

    @unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
    def test_start_replay(self):
        post = BlogPost(title='test blog post', post_id='12345', author=BlogAuthor(name='Jon Doe'), content='this is a blog post',
        updated=time.strftime("%Y-%m-%dT%H:%M%S-05"))

        dataset_id = self.dsm_cli.create_dataset(
            stream_id='12345',
            datastore_name=self.datastore_name,
            view_name='posts/posts_join_comments',
            name='blog posts test'
        )

        self.couch.create(post)

        replay_id, stream_id = self.dr_cli.define_replay(dataset_id)
        replay = self.rr_cli.read(replay_id)


        # assert that the process was created

        self.assertTrue(self.container.proc_manager.procs[replay.process_id])

        # pattern from Tim G
        ar = gevent.event.AsyncResult()
        def consume(message, headers):
            ar.set(message)

        stream_subscriber = StreamSubscriberRegistrar(process=self.container, node=self.container.node)
        subscriber = stream_subscriber.create_subscriber(exchange_name='test_queue', callback=consume)
        subscriber.start()

        query = StreamQuery(stream_ids=[stream_id])
        subscription_id = self.ps_cli.create_subscription(query=query,exchange_name='test_queue')
        self.ps_cli.activate_subscription(subscription_id)

        self.dr_cli.start_replay(replay_id)
        self.assertEqual(ar.get(timeout=10).post_id,post.post_id)

        subscriber.stop()

    def test_fields_replay(self):
        '''
        test_fields_replay
        Tests the capability of Replay to properly replay desired fields
        '''
        self._populate()
        cc = self.container
        dr_cli = self.dr_cli
        dsm_cli = self.dsm_cli
        assertions = self.assertTrue
        '''
        dr_cli = pn['data_retriever']
        dsm_cli = pn['dataset_management']
        '''


        stream_resource_id = '0821e596cc4240c3b95401a47ddfbb2f'

        dataset_id = dsm_cli.create_dataset(stream_id=stream_resource_id,
            datastore_name='test_replay',
            view_name='datasets/dataset_by_id'
        )
        replay_id, stream_id = dr_cli.define_replay(dataset_id=dataset_id, delivery_format={'fields':['temperature','pressure']})


        xs = '.'.join([bootstrap.get_sys_name(),'science_data'])
        results = []
        result = gevent.event.AsyncResult()

        def dump(m,h):
            results.append(1)
            if len(results)==3:
                result.set(True)

        subscriber = Subscriber(name=(xs,'test_replay'), callback=dump)
        g = gevent.Greenlet(subscriber.listen, binding='%s.data' % stream_id)
        g.start()

        time.sleep(1) # let the subscriber catch up

        dr_cli.start_replay(replay_id=replay_id)
        assertions(result.get(timeout=3),'Replay did not replay enough messages (%d) ' % len(results))

    def test_advanced_replay(self):
        self._generate_hdf_files()
        self._populate2()

        cc = self.container
        dr_cli = self.dr_cli
        dsm_cli = self.dsm_cli
        ps_cli = self.ps_cli
        tms_cli = self.tms_cli
        pd_cli = self.pd_cli
        assertions = self.assertTrue
        '''
        dr_cli = pn['data_retriever']
        dsm_cli = pn['dataset_management']
        ps_cli = pn['pubsub_management']
        tms_cli = pn['transform_management']
        pd_cli = pn['process_dispatcher']
        '''

        stream_resource_id = '0821e596cc4240c3b95401a47ddfbb2f'
        dataset_id = dsm_cli.create_dataset(
            stream_id=stream_resource_id,
            datastore_name='test_replay_advanced',
            view_name='datasets/dataset_by_id'
        )
        replay_id, stream_id = dr_cli.define_replay(dataset_id=dataset_id, delivery_format={'fields':['temperature']})

        xs = '.'.join([bootstrap.get_sys_name(),'science_data'])
        results = []
        result = gevent.event.AsyncResult()

        def dump(m,h):
            llog('Got message')
            llog('%s' % m.identifiables['ctd_data'])


        subscriber = Subscriber(name=(xs,'test_replay'), callback=dump)
        g = gevent.Greenlet(subscriber.listen, binding='%s.data' % stream_id)
        g.start()

        time.sleep(1) # let the subscriber catch up

        dr_cli.start_replay(replay_id=replay_id)

