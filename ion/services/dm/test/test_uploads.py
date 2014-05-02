#!/usr/bin/env python

__author__ = 'Brian McKenna <bmckenna@asascience.com>'

import json
import os
import tempfile
import time
import numpy as np
from nose.plugins.attrib import attr
from webtest import TestApp
from gevent.event import Event
from interface.services.coi.iservice_gateway_service import ServiceGatewayServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from ion.services.coi.service_gateway_service import service_gateway_app
from ion.services.dm.test.dm_test_case import DMTestCase
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.util.direct_coverage_utils import DirectCoverageAccess
from pyon.core.exception import NotFound
from pyon.event.event import EventSubscriber
from pyon.public import OT, PRED
from pyon.util.log import log
import unittest

@unittest.skipIf(os.getenv('CEI_LAUNCH_TEST', False), 'Skip test while in CEI LAUNCH mode')
@attr('LOCOINT')
@attr('INT', group='dm')
class TestUploadInt(DMTestCase):

    def setUp(self):

        DMTestCase.setUp(self)

        # clients needed (NOTE:most inherited from DMTestCase)
        self.object_store = self.container.object_store
        self.service_gateway_service = ServiceGatewayServiceClient(node=self.container.node)
        self.parameter_helper = self.ph # rename so know exactly what it is below

        # create a TestApp instance against the ServiceGatewayService
        self.testapp = TestApp(service_gateway_app)

    def test_upload_data(self):

        # create CTD data_product
        dp_id = self.make_ctd_data_product()
        
        # add BOGUS data in coverage for data_product
        rdt = self.parameter_helper.rdt_for_data_product(dp_id)
        t0 = 2208988800 # NTP 1970-01-01
        rdt['time'] = np.arange(t0, t0+10)
        rdt['temp'] = np.arange(10)
        rdt['conductivity'] = np.arange(10)

        # Make sure the data was ingested
        #dataset_monitor = DatasetMonitor(data_product_id=dp_id)
        #self.addCleanup(dataset_monitor.stop)
        self.parameter_helper.publish_rdt_to_data_product(dp_id, rdt)
        #self.assertTrue(dataset_monitor.wait())

        # first check the _L1c or _L2c parameters do not exist
        # check ParameterContext
        self.assertItemsEqual(self.getUploadedParameterContexts(dp_id), [])
        # check coverage
        self.assertItemsEqual(self.getUploadedCoverage(dp_id), [])

        # POST NetCDF file to ServiceGatewayService using TestApp
        NC_FILENAME = 'test_data/test_upload_data.nc'
        '''ncdump output of test_data/test_upload_data.nc:
           NOTE: time is "seconds since 1970-01-01T00:00:00Z"

netcdf test_upload_data {
dimensions:
    row = 50 ;
variables:
    double time(row) ;
        time:_CoordinateAxisType = "Time" ;
        time:actual_range = 1392057607.3601, 1392076747.36011 ;
        time:axis = "T" ;
        time:display_name = "Time, seconds since 1900-01-01" ;
        time:internal_name = "time" ;
        time:ioos_category = "Time" ;
        time:long_name = "Time, seconds since 1900-01-01" ;
        time:standard_name = "time" ;
        time:time_origin = "01-JAN-1970 00:00:00" ;
        time:time_precision = "1970-01-01T00:00:00.000Z" ;
        time:units = "seconds since 1970-01-01T00:00:00Z" ;
    float temp(row) ;
        temp:display_name = "Temperature" ;
        temp:internal_name = "temp" ;
        temp:ioos_category = "Temperature" ;
        temp:long_name = "Temperature" ;
        temp:ooi_short_name = "TEMPWAT_L1" ;
        temp:precision = "4" ;
        temp:standard_name = "sea_water_temperature" ;
        temp:units = "deg_C" ;
        temp:author = "Fox Mulder" ;
        temp:reason = "I want to believe." ;
    float conductivity(row) ;
        conductivity:display_name = "Conductivity, S m-1" ;
        conductivity:internal_name = "conductivity" ;
        conductivity:ioos_category = "Salinity" ;
        conductivity:long_name = "Conductivity, S m-1" ;
        conductivity:ooi_short_name = "CONDWAT_L1" ;
        conductivity:precision = "6" ;
        conductivity:standard_name = "sea_water_electrical_conductivity" ;
        conductivity:units = "S m-1" ;
        conductivity:author = "Dana Scully" ;
        conductivity:reason = "All lies lead to the truth." ;

data:

 time = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 
    38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49 ;

 temp = 65.9268, 76.418, 86.0553, 94.4546, 101.2809, 106.2621, 109.1997, 
    109.9765, 108.5616, 105.0114, 99.4673, 92.1505, 83.3526, 73.4243, 
    62.7616, 51.7894, 40.9452, 30.6614, 21.3478, 13.3759, 7.0633, 2.6619, 
    0.3470001, 0.2109003, 2.2592, 6.41, 12.498, 20.2803, 29.4469, 39.6321, 
    50.4301, 61.4102, 72.1348, 82.1762, 91.1343, 98.6517, 104.4289, 108.2356, 
    109.9199, 109.4147, 106.7402, 102.0029, 95.3918, 87.1704, 77.6665, 
    67.2589, 56.3626, 45.412, 34.8436, 25.0788 ;

 conductivity = 49.93347, 59.47092, 68.23212, 75.8678, 82.07355, 86.60195, 
    89.27249, 89.97868, 88.69238, 85.46487, 80.42482, 73.77316, 65.77507, 
    56.74941, 47.056, 37.08129, 27.22294, 17.87398, 9.40711, 2.15988, 
    -3.57879, -7.5801, -9.68455, -9.80823, -7.94621, -4.17273, 1.36178, 
    8.43667, 16.76989, 26.02923, 35.84553, 45.82746, 55.57707, 64.70567, 
    72.84933, 79.68339, 84.9354, 88.39598, 89.92717, 89.46791, 87.03653, 
    82.72995, 76.71985, 69.24586, 60.60592, 51.1445, 41.23877, 31.28366, 
    21.67604, 12.79894 ;
}
        '''
        upload_files = [('file', NC_FILENAME)]
        upload_result = self.testapp.post('/ion-service/upload/data/%s' % dp_id, upload_files=upload_files, status=200)
        upload_result_json = json.loads(upload_result.body) # reponse JSON data
        upload_result_data = upload_result_json.get('data', {}) # dict

        # read the FileUploadContext (async) return
        #gateway_response = data.get('GatewayResponse', None)
        fuc_id = upload_result_data.get('GatewayResponse', {}).get('fuc_id', None) # CAUTION this is unicode
        #fuc = self.object_store.read(str(fuc_id))

        # wait for status to be complete from upload_data_processing
        status = ''
        limit = 12
        while not "process complete" in status and limit > 0:
            status_result = self.testapp.get('/ion-service/upload/%s' % fuc_id, status=200)
            status_result_json = json.loads(status_result.body)
            status_result_data = status_result_json.get('data', {}) # dict
            status = status_result_data.get('GatewayResponse', {}).get('status', '')
            limit = limit-1
            time.sleep(5) # pause for 5 seconds, will timeout at 1 minute

        # check ParameterContext
        self.assertItemsEqual(self.getUploadedParameterContexts(dp_id), ['temp_L1c', 'conductivity_L1c'])
        # check coverage
        self.assertItemsEqual(self.getUploadedCoverage(dp_id), ['temp_L1c', 'conductivity_L1c'])

    def test_upload_qc(self):

        # verify target object [REFDES01] do not exist in object_store
        self.assertRaises(NotFound, self.object_store.read, 'REFDES01')

        # write CSV to temporary file (contains comment and blank line)
        QC_CSV = '''
global_range,REFDES01,RD01DP01,m/s,Douglas C. Neidermeyer,-1,1
global_range,REFDES01,RD01DP01,m/s,Bluto,-10,10
stuck_value,REFDES01,RD01DP01,C,Otter,0.005,10
trend_test,REFDES01,RD01DP01,K,Pinto,25,4,4.5
spike_test,REFDES01,RD01DP01,degrees,Flounder,0.0001,4,15
# the following should not work (not enough values for spike_test)
spike_test,REFDES01,RD04DP01,degrees,Otter,0.0001,4
gradient_test,REFDES01,RD01DP01,C,Boon,s,[-0.01 0.01],30,,0.1'''
        (QC_HANDLE,QC_FILENAME) = tempfile.mkstemp()
        os.write(QC_HANDLE, QC_CSV)
        os.close(QC_HANDLE)

        # MONKEY PATCH time.time() for volatile ts_updated values in dict (set in POST below)
        CONSTANT_TIME = time.time() # time value we'll use in assert tests
        def new_time():
            return CONSTANT_TIME
        old_time = time.time
        time.time = new_time

        # POST temporary file to ServiceGatewayService using TestApp
        upload_files = [('file', QC_FILENAME)]
        result = self.testapp.post('/ion-service/upload/qc', upload_files=upload_files, status=200)

        # restore MONKEY PATCHed time
        time.time = old_time

        # remove temporary file since using mkstemp
        os.unlink(QC_FILENAME)
 
        # reponse JSON data
        json_data = json.loads(result.body)
        data = json_data.get('data', None) # dict

        # read the FileUploadContext (async) return
        #gateway_response = data.get('GatewayResponse', None)
        #fuc_id = gateway_response.get('fuc_id', None) # CAUTION this is unicode
        #fuc = self.object_store.read(str(fuc_id))

        # check the QC table stored in object_store
        REFDES01 = self.object_store.read('REFDES01')
        RD01DP01 = REFDES01.get('RD01DP01', None)
        self.assertEquals(RD01DP01, {
           'stuck_value':[
              {
                 'units':'C',
                 'consecutive_values':10,
                 'ts_created':CONSTANT_TIME,
                 'resolution':0.005,
                 'author':'Otter'
              }
           ],
           'gradient_test':[
              {
                 'toldat':0.1,
                 'xunits':'s',
                 'mindx':30,
                 'author':'Boon',
                 'startdat':None,
                 'ddatdx':[-0.01, 0.01],
                 'units':'C',
                 'ts_created':CONSTANT_TIME
              }
           ],
           'global_range':[
              {
                 'units':'m/s',
                 'max_value':1,
                 'min_value':-1,
                 'ts_created':CONSTANT_TIME,
                 'author':'Douglas C. Neidermeyer'
              },
              {
                 'units':'m/s',
                 'max_value':10,
                 'min_value':-10,
                 'ts_created':CONSTANT_TIME,
                 'author':'Bluto'
              }
           ],
           'trend_test':[
              {
                 'author':'Pinto',
                 'standard_deviation':4.5,
                 'polynomial_order':4,
                 'sample_length':25,
                 'units':'K',
                 'ts_created':CONSTANT_TIME
              }
           ],
           'spike_test':[
              {
                 'author':'Flounder',
                 'range_multiplier':4,
                 'window_length':15,
                 'units':'degrees',
                 'ts_created':CONSTANT_TIME,
                 'accuracy':0.0001
              }
           ]
        })

    def test_upload_qc_zip(self):

        # verify target object [REFDES01] do not exist in object_store
        self.assertRaises(NotFound, self.object_store.read, 'REFDES02')

        # check ZIP (local_range)
        ZIP_FILENAME = 'test_data/test_local_range.zip'
        '''$ unzip -l local_range_test.zip 
Archive:  local_range_test.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
       55  04-09-2014 13:45   01.csv
      537  04-09-2014 13:53   02.csv
      133  04-09-2014 13:51   master.csv
---------                     -------
      725                     3 files'''

        # MONKEY PATCH time.time() for volatile ts_updated values in dict (set in POST below)
        CONSTANT_TIME = time.time() # time value we'll use in assert tests
        def new_time():
            return CONSTANT_TIME
        old_time = time.time
        time.time = new_time

        upload_files = [('file', ZIP_FILENAME)]
        upload_result = self.testapp.post('/ion-service/upload/qc', upload_files=upload_files, status=200)
        upload_result_json = json.loads(upload_result.body) # reponse JSON data
        upload_result_data = upload_result_json.get('data', {}) # dict

        # restore MONKEY PATCHed time
        time.time = old_time

        # read the FileUploadContext (async) return
        #gateway_response = data.get('GatewayResponse', None)
        fuc_id = upload_result_data.get('GatewayResponse', {}).get('fuc_id', None) # CAUTION this is unicode
        #fuc = self.object_store.read(str(fuc_id))

        # wait for status to be complete from upload_data_processing
        status = ''
        limit = 12
        while not "process complete" in status and limit > 0:
            status_result = self.testapp.get('/ion-service/upload/%s' % fuc_id, status=200)
            status_result_json = json.loads(status_result.body)
            status_result_data = status_result_json.get('data', {}) # dict
            status = status_result_data.get('GatewayResponse', {}).get('status', '')
            limit = limit-1
            time.sleep(5) # pause for 5 seconds, will timeout at 1 minute

        # reponse JSON data
        json_data = json.loads(upload_result.body)
        data = json_data.get('data', None) # dict

        # read the FileUploadContext (async) return
        #gateway_response = data.get('GatewayResponse', None)
        #fuc_id = gateway_response.get('fuc_id', None) # CAUTION this is unicode
        #fuc = self.object_store.read(str(fuc_id))

        # check the QC table stored in object_store
        REFDES02 = self.object_store.read('REFDES02')
        RD02DP01 = REFDES02.get('RD02DP01', None)
        self.assertEquals(RD02DP01, {
           'local_range':[
              {
                 'units':'C',
                 'table':{
                    'datlim2':[
                        10.0,
                        20.0,
                        30.0,
                        40.0
                    ],
                    'datlim1':[
                        0.0,
                        10.0,
                        20.0,
                        30.0
                    ],
                    'temp':[
                        0.0,
                        20.0,
                        40.0,
                        60.0
                    ]
                 },
                 'ts_created':CONSTANT_TIME,
                 'author':'Bodhi'
              }
           ]
        })
        RD02DP02 = REFDES02.get('RD02DP02', None)
        self.assertEquals(RD02DP02, {
           'local_range':[
              {
                 'units':'dbar',
                 'table':{
                    'lat':[
                        4.643692599999999970e+01,
                        4.643692599999999970e+01,
                        4.643692599999999970e+01,
                        4.643692599999999970e+01
                    ],
                    'pressure':[
                        0.000000000000000000e+00,
                        3.750000000000000000e+01,
                        0.000000000000000000e+00,
                        3.750000000000000000e+01
                    ],
                    'datlim1':[
                        0.000000000000000000e+00,
                        1.000000000000000000e+01,
                        1.000000000000000000e+01,
                        2.000000000000000000e+01
                    ],
                    'lon':[
                        -1.248321789999999964e+02,
                        -1.248321789999999964e+02,
                        -1.253596542500000055e+02,
                        -1.253596542500000055e+02
                    ],
                    'datlim2':[
                        3.000000000000000000e+01,
                        4.000000000000000000e+01,
                        4.000000000000000000e+01,
                        5.000000000000000000e+01
                    ]
                 },
                 'ts_created':CONSTANT_TIME,
                 'author':'Johnny Utah'
              }
           ]
        })

    def test_upload_calibration(self):

        # verify target object [REFDES01] do not exist in object_store
        self.assertRaises(NotFound, self.object_store.read, 'IPN01')

        # create Event to check OT.ResetQCEvent is published
        reset_qc_event = Event() # EventSubscriber will set this to true if OT.ResetQCEvent published
        event_subscriber = EventSubscriber(event_type=OT.ResetQCEvent, callback=lambda *args,**kwargs : reset_qc_event.set(), auto_delete=True)
        event_subscriber.start()
        self.addCleanup(event_subscriber.stop)

        # write CSV to temporary file (contains comment and blank line)
        CALIBRATION_CSV = '''
IPN01,NAME01,0.1,m/s,NAME01_0.1,2014-01-01T01:01:01Z
IPN01,NAME01,0.2,m/s,NAME01_0.2,2014-02-02T02:02:02Z
IPN01,NAME01,0.3,m/s,NAME01_0.3,2014-03-03T03:03:03Z'''
        (CALIBRATION_HANDLE,CALIBRATION_FILENAME) = tempfile.mkstemp()
        os.write(CALIBRATION_HANDLE, CALIBRATION_CSV)
        os.close(CALIBRATION_HANDLE)

        # POST temporary file to ServiceGatewayService using TestApp
        upload_files = [('file', CALIBRATION_FILENAME)]
        result = self.testapp.post('/ion-service/upload/calibration', upload_files=upload_files, status=200)

        # remove temporary file since using mkstemp
        os.unlink(CALIBRATION_FILENAME)
 
        # reponse JSON data
        json_data = json.loads(result.body)
        data = json_data.get('data', None) # dict

        # read the FileUploadContext (async) return
        #gateway_response = data.get('GatewayResponse', None)
        #fuc_id = gateway_response.get('fuc_id', None) # CAUTION this is unicode
        #fuc = self.object_store.read(str(fuc_id))

        # check the QC table stored in object_store
        IPN01 = self.object_store.read('IPN01')
        NAME01 = IPN01.get('NAME01', None)
        self.assertEquals(NAME01, [
           {
              'units':'m/s',
              'description':'NAME01_0.1',
              'value':0.1,
              'start_date':'2014-01-01T01:01:01Z'
           },
           {
              'units':'m/s',
              'description':'NAME01_0.2',
              'value':0.2,
              'start_date':'2014-02-02T02:02:02Z'
           },
           {
              'units':'m/s',
              'description':'NAME01_0.3',
              'value':0.3,
              'start_date':'2014-03-03T03:03:03Z'
           }
        ]);

        # check Event
        self.assertTrue(reset_qc_event.wait(5))


    def test_download(self):

        # clients
        dataset_management = DatasetManagementServiceClient()

        # verify target object [REFDES01] do not exist in object_store
        self.assertRaises(NotFound, dataset_management.read_qc_table, 'REFDES01')

        # NOTE: time is again monkey patched in 'test_upload_qc' but should be static for that
        # MONKEY PATCH time.time() for volatile ts_updated values in dict (set in POST below)
        CONSTANT_TIME = time.time() # time value we'll use in assert tests
        def new_time():
            return CONSTANT_TIME
        old_time = time.time
        time.time = new_time

        #upload some data
        self.test_upload_qc()

        # restore MONKEY PATCHed time
        time.time = old_time

        REFDES01 = dataset_management.read_qc_table('REFDES01')
        RD01DP01 = REFDES01.get('RD01DP01', None)
        self.assertEquals(RD01DP01, {
           'stuck_value':[
              {
                 'units':'C',
                 'consecutive_values':10,
                 'ts_created':CONSTANT_TIME,
                 'resolution':0.005,
                 'author':'Otter'
              }
           ],
           'gradient_test':[
              {
                 'toldat':0.1,
                 'xunits':'s',
                 'mindx':30,
                 'author':'Boon',
                 'startdat':None,
                 'ddatdx':[-0.01, 0.01],
                 'units':'C',
                 'ts_created':CONSTANT_TIME
              }
           ],
           'global_range':[
              {
                 'units':'m/s',
                 'max_value':1,
                 'min_value':-1,
                 'ts_created':CONSTANT_TIME,
                 'author':'Douglas C. Neidermeyer'
              },
              {
                 'units':'m/s',
                 'max_value':10,
                 'min_value':-10,
                 'ts_created':CONSTANT_TIME,
                 'author':'Bluto'
              }
           ],
           'trend_test':[
              {
                 'author':'Pinto',
                 'standard_deviation':4.5,
                 'polynomial_order':4,
                 'sample_length':25,
                 'units':'K',
                 'ts_created':CONSTANT_TIME
              }
           ],
           'spike_test':[
              {
                 'author':'Flounder',
                 'range_multiplier':4,
                 'window_length':15,
                 'units':'degrees',
                 'ts_created':CONSTANT_TIME,
                 'accuracy':0.0001
              }
           ]
        })




    def getUploadedParameterContexts(self, dp_id):
        '''
        returns all ParameterContexts ending with _L1c or _L2c
        '''
        # get the ParameterContexts associated with this DataProduct
        sd_id = self.resource_registry.find_objects(dp_id, PRED.hasStreamDefinition, id_only=True)[0][0] # TODO loop
        pd_id = self.resource_registry.find_objects(sd_id, PRED.hasParameterDictionary, id_only=True)[0][0] # TODO loop
        pc_list, _ = self.resource_registry.find_objects(pd_id, PRED.hasParameterContext, id_only=False) # parameter contexts
        return [p.name for p in pc_list if p.name.lower().endswith(('_l1c','_l2c'))]

    def getUploadedCoverage(self, dp_id):
        keys = []
        with DirectCoverageAccess() as dca:
            # get the Dataset IDs associated with this DataProduct
            ds_id_list, _ = self.resource_registry.find_objects(dp_id, PRED.hasDataset, id_only=True)
            for ds_id in ds_id_list: # could be multiple Datasets for this DataProduct
                with dca.get_editable_coverage(ds_id) as cov: # <-- This pauses ingestion
                    keys.extend([k for k in cov.get_value_dictionary().keys() if k.lower().endswith(('_l1c','_l2c'))])
        return keys
