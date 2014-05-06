"""
@author Andy Bird
@author Jim Case
@brief Test cases for the tableloader, 
table loader is a service to load data products in to postgres and geoserver from the resource registry
"""

from gevent import server
from gevent.baseserver import _tcp_listener
from gevent import pywsgi
from gevent.monkey import patch_all; patch_all()
from pyon.util.breakpoint import breakpoint
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import CFG
from pyon.util.log import log
from nose.plugins.attrib import attr
from pyon.public import PRED
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from coverage_model import SimplexCoverage, QuantityType, ArrayType, ConstantType, CategoryType
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.services.dm.utility.granule_utils import time_series_domain
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from interface.objects import DataProduct
from pydap.client import open_url
import unittest
import gevent
import requests
import json

@attr('INTMAN', group='eoi')
class DatasetLoadTest(IonIntegrationTestCase):
    """
    The following integration tests (INTMAN) are to ONLY be run manually
    """

    def setUp(self):

        self.username = CFG.get_safe('eoi.geoserver.user_name', 'admin')
        self.PASSWORD = CFG.get_safe('eoi.geoserver.password', 'geoserver')
        self.gs_host = CFG.get_safe('eoi.geoserver.server', 'http://localhost:8080')
        self.gs_rest_url = ''.join([self.gs_host, '/geoserver/rest'])
        self.gs_ows_url = ''.join([self.gs_host, '/geoserver/ows'])
        IMPORTER_SERVICE_SERVER = CFG.get_safe('eoi.importer_service.server', 'http://localhost')
        IMPORTER_SERVICE_PORT = str(CFG.get_safe('eoi.importer_service.port', 8844))
        self.importer_service_url = ''.join([IMPORTER_SERVICE_SERVER, ':', IMPORTER_SERVICE_PORT])

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management = DatasetManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.pubsub_management = PubsubManagementServiceClient()
        self.resource_registry = self.container.resource_registry

    @unittest.skipIf(not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test in TABLE LOADER as services are not loaded')
    def test_create_dataset(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_extended_parsed()

        stream_def_id = self.pubsub_management.create_stream_definition('example', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)


        dp = DataProduct(name='example')

        data_product_id = self.data_product_management.create_data_product(dp, stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)

        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)

        dataset_id = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        monitor = DatasetMonitor(dataset_id)
        self.addCleanup(monitor.stop)

        rdt = ph.get_rdt(stream_def_id)
        ph.fill_rdt(rdt, 100)
        ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(monitor.event.wait(10))

        # Yield to other greenlets, had an issue with connectivity
        gevent.sleep(1)

        log.debug("--------------------------------")
        log.debug(dataset_id)
        coverage_path = DatasetManagementService()._get_coverage_path(dataset_id)
        log.debug(coverage_path)
        log.debug("--------------------------------")

        breakpoint(locals(), globals())


@attr('INT', group='eoi')
class ServiceTests(IonIntegrationTestCase):
    """
    Tests the GeoServer and Foreign Data Wrapper (FDW) services.
    """
    def setUp(self):

        self.username = CFG.get_safe('eoi.geoserver.user_name', 'admin')
        self.PASSWORD = CFG.get_safe('eoi.geoserver.password', 'geoserver')
        self.gs_host = CFG.get_safe('eoi.geoserver.server', 'http://localhost:8080')
        self.gs_rest_url = ''.join([self.gs_host, '/geoserver/rest'])
        self.gs_ows_url = ''.join([self.gs_host, '/geoserver/ows'])
        IMPORTER_SERVICE_SERVER = CFG.get_safe('eoi.importer_service.server', 'http://localhost')
        IMPORTER_SERVICE_PORT = str(CFG.get_safe('eoi.importer_service.port', 8844))
        self.importer_service_url = ''.join([IMPORTER_SERVICE_SERVER, ':', IMPORTER_SERVICE_PORT])

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.dataset_management = DatasetManagementServiceClient()
        self.data_product_management = DataProductManagementServiceClient()
        self.pubsub_management = PubsubManagementServiceClient()
        self.resource_registry = self.container.resource_registry
        self.offering_id = ''

    def setup_resource(self):
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_extended_parsed()

        stream_def_id = self.pubsub_management.create_stream_definition('example', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)


        dp = DataProduct(name='example')

        data_product_id = self.data_product_management.create_data_product(dp, stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)

        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)

        dataset_id = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        monitor = DatasetMonitor(dataset_id)
        self.addCleanup(monitor.stop)

        rdt = ph.get_rdt(stream_def_id)
        ph.fill_rdt(rdt, 100)
        ph.publish_rdt_to_data_product(data_product_id, rdt)

        # Yield to other greenlets, had an issue with connectivity
        gevent.sleep(1)

        self.offering_id = dataset_id

    @unittest.skipIf(not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test in TABLE LOADER as services are not loaded')
    def test_reset_store(self):
        # Makes sure store is empty 
        self.assertTrue(self.reset_store())
        url = ''.join([self.gs_rest_url, '/layers.json'])

        # Asserts layers were able to be retrieved
        r = requests.get(url, auth=(self.username, self.PASSWORD))
        self.assertTrue(r.status_code == 200)

        # Asserts there are no layers in the ooi store
        layers = json.loads(r.content)
        self.assertTrue(len(layers['layers']) == 0)

    @unittest.skipIf( not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test in TABLE LOADER as services are not loaded')
    def test_create_dataset_verify_geoserver_layer(self):
        #generate layer and check that the service created it in geoserver
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_extended_parsed()

        stream_def_id = self.pubsub_management.create_stream_definition('example', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)


        dp = DataProduct(name='example')

        data_product_id = self.data_product_management.create_data_product(dp, stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)

        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)

        dataset_id = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        monitor = DatasetMonitor(dataset_id)
        self.addCleanup(monitor.stop)

        rdt = ph.get_rdt(stream_def_id)
        ph.fill_rdt(rdt, 100)
        ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(monitor.event.wait(10))

        gevent.sleep(1) # Yield to other greenlets, had an issue with connectivity

        log.debug("--------------------------------")
        log.debug(dataset_id)
        coverage_path = DatasetManagementService()._get_coverage_path(dataset_id)
        log.debug(coverage_path)
        log.debug("--------------------------------")

        # verify that the layer exists in geoserver
        try:
            r = requests.get(self.gs_rest_url + '/layers/ooi_' + dataset_id + '_ooi.xml', auth=(self.username, self.PASSWORD))
            self.assertTrue(r.status_code == 200)
        except Exception as e:
            log.error("check service and layer exist...%s", e)
            self.assertTrue(False)

    @unittest.skipIf( not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test in TABLE LOADER as services are not loaded')
    def test_verify_importer_service_online(self):
        try:
            r = requests.get(self.importer_service_url)
            self.assertTrue(r.status_code == 200)
        except Exception as e:
            #make it fail
            log.error("check service is started on port...%s", e)
            self.assertTrue(False)

    @unittest.skip('Not yet implemented')
    def test_add_geoserver_layer(self):
        # pass the create command to the service and check that the layer exists in  geoserver similar to the one above
        # send add layer directly to localhost 8844 with some params
        # store gets reset every time container is started
        # Makes sure store is empty
        self.assertTrue(self.reset_store())
        params = {'temp_L1': 'real', 'conductivity_L1': 'real', 'temp': 'real', 'density': 'real',
                  'pressure_L1': 'real', 'lon': 'real', 'lat_lookup': 'real', 'density_lookup': 'real',
                  'pressure': 'real', 'lon_lookup': 'real', 'geom': 'geom', 'time': 'time', 'lat': 'real',
                  'salinity': 'real', 'conductivity': 'real'}
        r = requests.get(self.importer_service_url + '/service=addlayer&name=45a6a3cea12e470b90f3e5a769f22161&id=45a6a3cea12e470b90f3e5a769f22161&params=' + str(params))

        self.assertTrue(r.status_code == 200)

    @unittest.skip('Not yet implemented')
    def test_fdt_created_during(self):
    # generate a data product and check that the FDT exists
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_extended_parsed()

        stream_def_id = self.pubsub_management.create_stream_definition('example', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)


        dp = DataProduct(name='example')

        data_product_id = self.data_product_management.create_data_product(dp, stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)

        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)

        dataset_id = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        monitor = DatasetMonitor(dataset_id)
        self.addCleanup(monitor.stop)

        rdt = ph.get_rdt(stream_def_id)
        ph.fill_rdt(rdt, 100)
        ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(monitor.event.wait(10))

        gevent.sleep(1) # Yield to other greenlets, had an issue with connectivity

        print "--------------------------------"
        print dataset_id
        coverage_path = DatasetManagementService()._get_coverage_path(dataset_id)
        print coverage_path
        print "--------------------------------"

        #assert that the time exists in the DB

        #compare the params in geoserver to those in the table

        #Verify DB contains table
        #eg sql request String sqlcmd = "select column_name from information_schema.columns where table_name = \'" + dataset_id + "\';";

        #Verify Geoserver
        #r = requests.get('http://localhost:8080/geoserver/rest/workspaces/geonode/datastores/ooi/featuretypes/ooi_'+dataset_id+'_ooi.json',auth=('admin','admin'))
        #use r.json() or r.text and parse the output and compare the params

    @unittest.skip('Not yet implemented')
    def test_remove_geolayer_directory(self):
        # pass the remove command to the service and check that the layer exists in geoserver similar to the one above
        # send remove layer directly to localhost 8844 with some params
        # check store
        print ""
        pass

    @unittest.skip('Not yet implemented')
    def test_update_geolayer_directory(self):
        # pass the update command to the service and check that the layer exists in geoserver similar to the one above
        # send update layer directly to localhost 8844 with some params
        # check store
        #does add then remove
        print ""
        pass

    @unittest.skip('Not yet implemented')
    def test_get_data_from_FDW(self):
        # generate a data product and check that the FDW can get data
        ph = ParameterHelper(self.dataset_management, self.addCleanup)
        pdict_id = ph.create_extended_parsed()

        stream_def_id = self.pubsub_management.create_stream_definition('example', parameter_dictionary_id=pdict_id)
        self.addCleanup(self.pubsub_management.delete_stream_definition, stream_def_id)


        dp = DataProduct(name='example')

        data_product_id = self.data_product_management.create_data_product(dp, stream_def_id)
        self.addCleanup(self.data_product_management.delete_data_product, data_product_id)

        self.data_product_management.activate_data_product_persistence(data_product_id)
        self.addCleanup(self.data_product_management.suspend_data_product_persistence, data_product_id)

        dataset_id = self.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        monitor = DatasetMonitor(dataset_id)
        self.addCleanup(monitor.stop)

        rdt = ph.get_rdt(stream_def_id)
        ph.fill_rdt(rdt, 100)
        ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(monitor.event.wait(10))

        gevent.sleep(1) # Yield to other greenlets, had an issue with connectivity

        print "--------------------------------"
        print dataset_id
        coverage_path = DatasetManagementService()._get_coverage_path(dataset_id)
        print coverage_path
        print "--------------------------------"

        #verify table exists in the DB (similar to above)
        # ....code...

        # check that the geoserver layer exists as above
        # ... code ....

        # make a WMS/WFS request...somet like this (or both)
        url = self.gs_host+'/geoserver/geonode/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=geonode:ooi_' + dataset_id + '_ooi&maxFeatures=1&outputFormat=csv'
        r = requests.get(url)
        assertTrue(r.status_code == 200)
        #check r.text does not contain <ServiceException code="InvalidParameterValue" locator="typeName">

    @unittest.skipIf(not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test in TABLE LOADER as services are not loaded')
    def test_sos_response(self):
        expected_content = 'SOS SERVICE IS UP.....Hello World!'
        url = self.gs_ows_url + '?request=echo&service=sos'
        r = requests.get(url)
        self.assertEqual(r.content, expected_content)

    @unittest.skipIf(not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test in TABLE LOADER as services are not loaded')
    def test_sos_get_capabilities(self):
        # Validates reponse is not an exception, assues valid otherwise
        self.setup_resource()
        expected_content = ''
        url = self.gs_ows_url + '?request=getCapabilities&service=sos&version=1.0.0&offering=_' + self.offering_id + '_view'
        r = requests.get(url)
        self.assertEquals(r.status_code, 200)
        self.assertTrue(r.content.find('<sos:Capabilities') >= 0)

    @unittest.skipIf(not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test in TABLE LOADER as services are not loaded')
    def test_sos_get_offering(self):
        # Validates reponse is not an exception, assues valid otherwise
        # TODO: Use deterministic <swe:values> for comparison
        self.setup_resource()
        expected_content = ''
        url = self.gs_ows_url + '?request=getObservation&service=sos&version=1.0.0&offering=_' + self.offering_id + '_view&observedproperty=time,temp,density&responseformat=text/xml'
        r = requests.get(url)
        self.assertEquals(r.status_code, 200)
        self.assertTrue(r.content.find('<om:ObservationCollection') >= 0)
        self.assertTrue(r.content.find('ExceptionReport') == -1)

    def reset_store(self, store_name='ooi', store_id='ooi'):
        """
        Posts a resetstore request to the ImporterService
        """
        try:
            url = ''.join([self.importer_service_url, '/service=resetstore&name=', store_name, '&id=', store_id])
            r = requests.post(url)
            if r.status_code == 200:
                return True
            else:
                return False
        except Exception as e:
            log.error('Unable to reset GeoServer Store. %s', e)
            return False


