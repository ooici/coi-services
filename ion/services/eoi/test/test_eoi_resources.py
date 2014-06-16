"""
@author Andy Bird
@author Jim Case
@brief Test cases for the eoi data provider resources, 
"""
from ion.services.dm.test.dm_test_case import DMTestCase, Streamer
from ion.processes.data.transforms.viz.google_dt import VizTransformGoogleDTAlgorithm
from ion.processes.data.replay.replay_process import RetrieveProcess
from ion.services.dm.utility.test.parameter_helper import ParameterHelper
from ion.services.dm.utility.granule import RecordDictionaryTool
from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.services.dm.utility.tmpsf_simulator import TMPSFSimulator
from ion.services.dm.utility.bad_simulator import BadSimulator
from ion.util.direct_coverage_utils import DirectCoverageAccess
from ion.services.dm.utility.hydrophone_simulator import HydrophoneSimulator
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from ion.services.dm.utility.provenance import graph
from ion.processes.data.registration.registration_process import RegistrationProcess
from coverage_model import ParameterFunctionType, ParameterDictionary, PythonFunction, ParameterContext as CovParameterContext
from ion.processes.data.transforms.transform_worker import TransformWorker
from interface.objects import DataProcessDefinition, InstrumentDevice, ParameterFunction, ParameterFunctionType as PFT, ParameterContext
from nose.plugins.attrib import attr
from pyon.util.breakpoint import breakpoint
from pyon.core.exception import NotFound
from pyon.event.event import EventSubscriber
from pyon.util.file_sys import FileSystem
from pyon.public import IonObject, RT, CFG, PRED, OT
from pyon.util.containers import DotDict
from pydap.client import open_url
from shutil import rmtree
from datetime import datetime, timedelta
from pyon.net.endpoint import RPCClient
from pyon.util.log import log
from pyon.ion.event import EventPublisher
from interface.objects import InstrumentSite, InstrumentModel, PortTypeEnum, Deployment, CabledInstrumentDeploymentContext
from nose.tools import with_setup
import lxml.etree as etree
import simplejson as json
import pkg_resources
import tempfile
import os
import unittest
import numpy as np
import time
import gevent
import requests
from gevent.event import Event
import calendar
from interface.objects import DataSource, ExternalDataProvider,DataProduct

MOCK_HARVESTER_NAME = "test_harvester"

@attr(group='eoi')
class TestEOIExternalResources(DMTestCase):
    
    def preload_ui(self):
        config = DotDict()
        config.op='loadui'
        config.loadui=True
        config.attachments='res/preload/r2_ioc/attachments'
        config.ui_path = "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    
    @attr('UTIL')
    def test_generation_of_dp_load_ui(self):
        url = "http://r3-pg-test02.oceanobservatories.org:8080/geonetwork/srv/eng/main.home?uuid={9C0EC29F-7A36-45AD-9EAB-99D82AB80F6D}"
        url = url.replace("{","%7B")
        url = url.replace("}","%7D")

        dp = DataProduct(name="Example external data product", 
                         category=3,
                         ooi_product_name = 'Type Field',
                         quality_control_level='Not Applicable, or something like that.',
                         processing_level_code='External L0',
                         reference_urls=[url]
                         )

        # Find the time parameter
        time_params, _ = self.resource_registry.find_resources_ext(alt_id="PD7", alt_id_ns='PRE', id_only=True)
        time_param = time_params[0]

        # Make a param dict with only time in it
        pdict_id = self.dataset_management.create_parameter_dictionary(name=dp.name, parameter_context_ids=[time_param], temporal_context='time')

        stream_def_id = self.pubsub_management.create_stream_definition(name=dp.name, parameter_dictionary_id=pdict_id)

        dp_id, _ = self.resource_registry.create(dp)

        self.resource_registry.create_association(subject=dp_id, predicate='hasStreamDefinition', object=stream_def_id)

        #--------------------------------------------------------------------------------
        # Now we add parameters
        #--------------------------------------------------------------------------------
        parameter_json_dict_thing = {
            'temperature' : {
                'description' : 'COMMENT',
                'display_name' : 'NAME',
                'units' : 'unknown',
                'value_encoding' : 'float32',
                'parameter_type' : 'quantity'
                }
        }

        # The important section
        for parameter_name, parameter_def in parameter_json_dict_thing.iteritems():
            param_object = ParameterContext(name=parameter_name, **parameter_def)
            parameter_id = self.dataset_management.create_parameter(param_object)
            self.data_product_management.add_parameter_to_data_product(parameter_id, dp_id)

        self.preload_ui()
        self.launch_ui_facepage(dp_id)
        breakpoint(locals(), globals())




    '''
    tests the addition of external resources in to the system through preload
    checks that there are datasources in geonetwork
    checks that neptune and ioos have been added through preload as resources
    '''
    @attr('UTIL')
    @unittest.skipIf( not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test services are not loaded')  
    def test_external_data_provider_during_preload(self):
        self.preload_external_providers()

        self.rr = self.container.resource_registry

        data_list = self.rr.find_resources(restype=RT.DataSource)
        data_list = data_list[0]
        #more than one?
        self.assertTrue(len(data_list)>1)
        #make sure that the expected list is all there  
        expected_list = ['neptune','ngdc', 'nodc_ioos','ndbc_ioos','ooi']

        for data in data_list:
            self.assertTrue(data.name in expected_list)             

        #try more than once to get the harvester list, as can take a second to update
        for x in xrange(0,3):
            h_list = self.get_harvester_list()
            names = self.get_harvester_names(h_list)

        #check that the preload task loaded the required harvester
        if len(names)>0:
            all_accounted_for =  set(expected_list).issubset(set(names)) 

            if all_accounted_for:
                log.debug("All harvesters accounted for...")    
            else:
                log.warn("All harvesters not accounted for")    
                for expected_name in expected_list:                 
                    if expected_name not in names:
                        log.error("harvester:"+expected_name+" in preload and resources, not in geonetwork")
                    else:
                        log.warn("harvester found:"+expected_name)
                        
                        
        else:
            log.error("no harvester names returned, check geonetwork connection")   


        breakpoint(locals(),globals())

        #remove added harvester
        for expected_name in expected_list: 
            if expected_name in names:      
                log.warn("remoing harvester from geonetwork:"+expected_name)
                self.remove_harvester_list(expected_name)

        self.remove_added_harvesters()


    '''
    tests the addition of external resources in to the system, 
    skipped as not really needed, but might be useful down the road
    '''
    @unittest.skip
    def test_add_datasource_externaldataprovider_to_rr(self):
        self.preload_external_providers()

        ds = DataSource(name='bob')     
        cc.resource_registry.create(ds)

        edp = ExternalDataProvider(name='bob')
        cc.resource_registry.create(edp)

        self.remove_added_harvesters()
        

    '''
    preload data from select scenario
    ''' 
    def preload_external_providers(self):
        config = DotDict()
        config.op = 'load'
        config.loadui=True
        config.ui_path =  "http://userexperience.oceanobservatories.org/database-exports/Candidates"
        config.attachments = "res/preload/r2_ioc/attachments"       
        config.scenario = 'AB_TEST'     
        config.path = 'master'
        self.container.spawn_process('preloader', 'ion.processes.bootstrap.ion_loader', 'IONLoader', config)

    '''
    can get the list of harvesters from th importer service, hits the geonetwork service
    '''
    def get_harvester_list(self):
        IMPORTER_SERVICE_SERVER = CFG.get_safe('eoi.importer_service.server', 'http://localhost')
        IMPORTER_SERVICE_PORT = str(CFG.get_safe('eoi.importer_service.port', 8844))
        self.importer_service_url = ''.join([IMPORTER_SERVICE_SERVER, ':', IMPORTER_SERVICE_PORT])
        #at this point importer service should be up
        #get the harvesters list
        harvester_get_url = self.importer_service_url+"/service=requestharvester&hfilter=all"
        try:
            r = requests.get(harvester_get_url,timeout=10)
            return r.text
        except Exception, e:
            #fail because it should have the service running
            log.error("check service, as it appears to not be running...%s", e)     
        return None 

    '''
    can get the list of harvesters from th importer service, hits the geonetwork service
    '''
    def remove_harvester_list(self,name):
        IMPORTER_SERVICE_SERVER = CFG.get_safe('eoi.importer_service.server', 'http://localhost')
        IMPORTER_SERVICE_PORT = str(CFG.get_safe('eoi.importer_service.port', 8844))
        self.importer_service_url = ''.join([IMPORTER_SERVICE_SERVER, ':', IMPORTER_SERVICE_PORT])
        #at this point importer service should be up
        #get the harvester list
        harvester_get_url = self.importer_service_url+"/service=removeharvester&hfilter="+name
        try:
            r = requests.get(harvester_get_url,timeout=10)
            return r.text
        except Exception, e:
            #fail because it should have the service running
            log.error("check service, as it appears to not be running...%s", e)                     
        return None     

    def get_harvester_names(self,xml):
        #need to strip the encoding
        try:            
            xml = xml.replace('encoding="UTF-8"',"");
            parser = etree.XMLParser(target = EchoTarget())

            root = etree.XML(xml)
            d = root.findall("node/site/name")
            
            for name in d:
                etree.XML(etree.tostring(name), parser)

            name_list = parser.target.events
            corrected_name = []
            for name in name_list:
                n  =  [n for (n, e) in enumerate(name) if e == "'"]
                name_str = name[n[0]+1:n[1]]
                corrected_name.append(name_str)
            return corrected_name   

        except Exception, e:
            return []


    '''
    checks that havester information is available
    can be added too via the importer interface
    '''
    @unittest.skipIf( not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test services are not loaded')  
    def test_adding_removing_harvester(self):

        IMPORTER_SERVICE_SERVER = CFG.get_safe('eoi.importer_service.server', 'http://localhost')
        IMPORTER_SERVICE_PORT = str(CFG.get_safe('eoi.importer_service.port', 8844))
        self.importer_service_url = ''.join([IMPORTER_SERVICE_SERVER, ':', IMPORTER_SERVICE_PORT])

        #create url to check service is alive
        alive_url = self.importer_service_url+"/service=alive&name=ooi&id=ooi"
        #make get request if service is available
        try:
            r = requests.get(alive_url,timeout=5)
            self.assertTrue(r.status_code == 200)           
        except Exception, e:
            #fail because it should have the service running
            log.error("check service, as it appears to not be running...%s", e)         
            #should fail as the service should be running if it has been requested
            self.assertTrue(False)
        
        #current number of harvesters and their names
        names_before = self.get_harvester_names(self.get_harvester_list())
        #check that a harvester of a specific name does not exist

        #generate the harvester using something like below.
        mock_harvester_create = self.importer_service_url+"/service=createharvester&lcstate=DEPLOYED&rev=1&searchterms=mutibeam,RI&availability=AVAILABLE&externalize=1&persistedversion=1&ogctype=&importxslt=gmiTogmd.xsl&addl=%7B%7D&harvestertype=geoPREST&description=IOOS&datasourceattributes=%7B%7D&visibility=1&connectionparams=%7B%7D&tsupdated=1399474190226&tscreated=1399474190226&institution=Institution(%7B%27website%27:%20%27%27,%20%27phone%27:%20%27%27,%20%27name%27:%20%27%27,%20%27email%27:%20%27%27%7D)&protocoltype=&name="+MOCK_HARVESTER_NAME+"&altids=[%27PRE:EDS_ID2%27]&datasourcetype=geoportal&type=DataSource&id=27aa22dc3f6742d3892a5ec41b0cedb2&protocoltype=http://www.google.com"
        try:
            r = requests.get(mock_harvester_create,timeout=15)
            if r.status_code == 200:
                pass
            else:
                log.error("service returned !200 %s", r.text)           

        except Exception, e:
            log.error("check service, as it appears to not be running...%s", e)         
            self.assertTrue(False)          

        names_after = self.get_harvester_names(self.get_harvester_list())

        #overview check to make sure that the number of names is less than it was before the additon
        self.assertTrue(len(names_before) < len(names_after))       
        #check that the added harvester is in the list
        present = False
        for name in names_after:
            if MOCK_HARVESTER_NAME in name:
                present = True
                break

        #if valid process
        self.assertTrue(present)        

        #remove the added harvester
        self.remove_harvester_list(MOCK_HARVESTER_NAME)

        #reset the variable
        names_after = self.get_harvester_names(self.get_harvester_list())

        #reset valid variable
        present = False
        for name in names_after:
            if MOCK_HARVESTER_NAME in name:
                present = True
                break
        #checks it has been removed
        self.assertFalse(present)
        #remove those added during preload
        self.remove_added_harvesters()


    '''
    checks that havester information is available
    can be added too via the importer interface
    '''
    @unittest.skipIf( not (CFG.get_safe('eoi.meta.use_eoi_services', False)), 'Skip test services are not loaded')  
    def remove_added_harvesters(self):
        names = self.get_harvester_names(self.get_harvester_list())
        expected_list = ['neptune','ioos','ooi']
        for n in names:
            if n in expected_list:
                log.warn("remoing harvester from geonetwork:"+n)
                self.remove_harvester_list(n)               


class EchoTarget(object):
    def __init__(self):
        self.events = []
    def start(self, tag, attrib):
        pass
    def end(self, tag):
        pass
    def data(self, data):
        self.events.append("%r" % data)
        pass
    def comment(self, text):
        pass
    def close(self):
        return "closed!"
