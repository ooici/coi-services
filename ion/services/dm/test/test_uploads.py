#!/usr/bin/env python

__author__ = 'Brian McKenna <bmckenna@asascience.com>'

import json
import os
import tempfile
import time
from nose.plugins.attrib import attr
from webtest import TestApp
from ion.services.coi.service_gateway_service import service_gateway_app
from interface.services.coi.iservice_gateway_service import ServiceGatewayServiceClient
from pyon.core.exception import NotFound
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.log import log

@attr('INT', group='dm')
class TestUploadInt(IonIntegrationTestCase):

    def setUp(self):

        # start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # clients needed
        self.object_store = self.container.object_store
        self.service_gateway_service = ServiceGatewayServiceClient(node=self.container.node)

        # create a TestApp instance against the ServiceGatewayService
        self.testapp = TestApp(service_gateway_app)

    def tearDown(self):
        self._stop_container()

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
spike_test,REFDES01,RD04DP01,degrees,Otter,0.0001,4'''
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
        assert RD01DP01 == {
           'stuck_value':[
              {
                 'units':'C',
                 'consecutive_values':'10',
                 'ts_created':CONSTANT_TIME,
                 'resolution':'0.005',
                 'author':'Otter'
              }
           ],
           'global_range':[
              {
                 'units':'m/s',
                 'max_value':'1',
                 'min_value':'-1',
                 'ts_created':CONSTANT_TIME,
                 'author':'Douglas C. Neidermeyer'
              },
              {
                 'units':'m/s',
                 'max_value':'10',
                 'min_value':'-10',
                 'ts_created':CONSTANT_TIME,
                 'author':'Bluto'
              }
           ],
           'trend_test':[
              {
                 'author':'Pinto',
                 'standard_deviation':'4.5',
                 'polynomial_order':'4',
                 'sample_length':'25',
                 'units':'K',
                 'ts_created':CONSTANT_TIME
              }
           ],
           'spike_test':[
              {
                 'author':'Flounder',
                 'range_multiplier':'4',
                 'window_length':'15',
                 'units':'degrees',
                 'ts_created':CONSTANT_TIME,
                 'accuracy':'0.0001'
              }
           ]
        }
