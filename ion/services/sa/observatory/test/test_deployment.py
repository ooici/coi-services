#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, log, IonObject
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient

from prototype.sci_data.stream_defs import SBE37_CDM_stream_definition

from pyon.util.context import LocalContextMixin
from pyon.core.exception import BadRequest, NotFound, Conflict, Inconsistent
from pyon.public import RT, PRED
#from mock import Mock, patch
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
import unittest
from pyon.util.log import log

from ion.services.sa.test.helpers import any_old
from ion.util.parameter_yaml_IO import get_param_dict

from coverage_model.parameter import ParameterDictionary, ParameterContext
from coverage_model.parameter_types import QuantityType
from coverage_model.coverage import GridDomain, GridShape, CRS
from coverage_model.basic_types import MutabilityEnum, AxisTypeEnum

class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='mmm')
class TestDeployment(IonIntegrationTestCase):

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.rrclient = ResourceRegistryServiceClient(node=self.container.node)
        self.omsclient = ObservatoryManagementServiceClient(node=self.container.node)
        self.imsclient = InstrumentManagementServiceClient(node=self.container.node)
        self.dmpsclient = DataProductManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.psmsclient = PubsubManagementServiceClient(node=self.container.node)

    #@unittest.skip("targeting")
    def test_create_deployment(self):

        #create a deployment with metadata and an initial site and device
        platform_site__obj = IonObject(RT.PlatformSite,
                                        name='PlatformSite1',
                                        description='test platform site')
        site_id = self.omsclient.create_platform_site(platform_site__obj)

        platform_device__obj = IonObject(RT.PlatformDevice,
                                        name='PlatformDevice1',
                                        description='test platform device')
        device_id = self.imsclient.create_platform_device(platform_device__obj)

        deployment_obj = IonObject(RT.Deployment,
                                        name='TestDeployment',
                                        description='some new deployment')
        deployment_id = self.omsclient.create_deployment(deployment_obj)
        self.omsclient.deploy_platform_site(site_id, deployment_id)
        self.imsclient.deploy_platform_device(device_id, deployment_id)

        log.debug("test_create_deployment: created deployment id: %s ", str(deployment_id) )

        #retrieve the deployment objects and check that the assoc site and device are attached
        read_deployment_obj = self.omsclient.read_deployment(deployment_id)
        log.debug("test_create_deployment: created deployment obj: %s ", str(read_deployment_obj) )

        site_ids, _ = self.rrclient.find_subjects(RT.PlatformSite, PRED.hasDeployment, deployment_id, True)
        self.assertEqual(len(site_ids), 1)

        device_ids, _ = self.rrclient.find_subjects(RT.PlatformDevice, PRED.hasDeployment, deployment_id, True)
        self.assertEqual(len(device_ids), 1)

        #delete the deployment
        self.omsclient.delete_deployment(deployment_id)
        # now try to get the deleted dp object
        try:
            deployment_obj = self.omsclient.read_deployment(deployment_id)
        except NotFound as ex:
            pass
        else:
            self.fail("deleted deployment was found during read")




    #@unittest.skip("targeting")
    def test_activate_deployment(self):

        #create a deployment with metadata and an initial site and device
        platform_site__obj = IonObject(RT.PlatformSite,
                                        name='PlatformSite1',
                                        description='test platform site')
        site_id = self.omsclient.create_platform_site(platform_site__obj)

        platform_device_obj = IonObject(RT.PlatformDevice,
                                        name='PlatformDevice1',
                                        description='test platform device')
        platform_device_id = self.imsclient.create_platform_device(platform_device_obj)

        platform_model__obj = IonObject(RT.PlatformModel,
                                        name='PlatformModel1',
                                        description='test platform model')
        model_id = self.imsclient.create_platform_model(platform_model__obj)
        self.imsclient.assign_platform_model_to_platform_device(model_id, platform_device_id)
        self.omsclient.assign_platform_model_to_platform_site(model_id, site_id)



        #create a deployment with metadata and an initial site and device
        instrument_site_obj = IonObject(RT.InstrumentSite,
                                        name='InstrumentSite1',
                                        description='test instrument site')
        instrument_site_id = self.omsclient.create_instrument_site(instrument_site_obj, site_id)

        #assign data products appropriately
        #set up stream (this would be preload)
        ctd_stream_def = SBE37_CDM_stream_definition()
        ctd_stream_def_id = self.psmsclient.create_stream_definition(container=ctd_stream_def)

        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 1d spatial topology (station/trajectory)

        sdom = sdom.dump()
        tdom = tdom.dump()

        parameter_dictionary = get_param_dict('ctd_parsed_param_dict')

        parameter_dictionary = parameter_dictionary.dump()

        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        log_data_product_id = self.dmpsclient.create_data_product(dp_obj, ctd_stream_def_id, parameter_dictionary)
        self.omsclient.create_site_data_product(instrument_site_id, log_data_product_id)


        instrument_device_obj = IonObject(RT.InstrumentDevice,
                                        name='InstrumentDevice1',
                                        description='test instrument device')
        instrument_device_id = self.imsclient.create_instrument_device(instrument_device_obj)
        self.rrclient.create_association(platform_device_id, PRED.hasDevice, instrument_device_id)


        dp_obj = IonObject(RT.DataProduct,
            name='DP1',
            description='some new dp',
            temporal_domain = tdom,
            spatial_domain = sdom)

        inst_data_product_id = self.dmpsclient.create_data_product(dp_obj, ctd_stream_def_id, parameter_dictionary)

        #assign data products appropriately
        self.damsclient.assign_data_product(input_resource_id=instrument_device_id,
                                            data_product_id=inst_data_product_id)


        instrument_model_obj = IonObject(RT.InstrumentModel,
                                        name='InstrumentModel1',
                                        description='test instrument model')
        instrument_model_id = self.imsclient.create_instrument_model(instrument_model_obj)
        self.imsclient.assign_instrument_model_to_instrument_device(instrument_model_id, instrument_device_id)
        self.omsclient.assign_instrument_model_to_instrument_site(instrument_model_id, instrument_site_id)
        #self.rrclient.create_association(instrument_site_id, PRED.hasModel, instrument_model_id)

        deployment_obj = IonObject(RT.Deployment,
                                        name='TestDeployment',
                                        description='some new deployment')
        deployment_id = self.omsclient.create_deployment(deployment_obj)
        self.omsclient.deploy_instrument_site(instrument_site_id, deployment_id)
        self.imsclient.deploy_instrument_device(instrument_device_id, deployment_id)

        log.debug("test_create_deployment: created deployment id: %s ", str(deployment_id) )

        self.omsclient.activate_deployment(deployment_id)

