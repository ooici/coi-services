#!/usr/bin/env python
import string
import unittest
import datetime
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from pyon.util.containers import DotDict, get_ion_ts
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.public import RT, PRED, OT, log, LCE
from pyon.public import IonObject
from pyon.event.event import EventPublisher
from pyon.agent.agent import ResourceAgentState
from ion.services.dm.utility.granule_utils import time_series_domain
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient
from pyon.core.governance import get_actor_header
from nose.plugins.attrib import attr
from interface.objects import ComputedValueAvailability
from ion.processes.bootstrap.ion_loader import TESTED_DOC, IONLoader
from pyon.util.ion_time import IonTime

from ion.services.sa.test.helpers import any_old


class FakeProcess(LocalContextMixin):
    name = ''


@attr('INT', group='sa')
class TestObservatoryManagementFullIntegration(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self.RR = ResourceRegistryServiceClient(node=self.container.node)
        self.RR2 = EnhancedResourceRegistryClient(self.RR)
        self.OMS = ObservatoryManagementServiceClient(node=self.container.node)
        self.org_management_service = OrgManagementServiceClient(node=self.container.node)
        self.IMS =  InstrumentManagementServiceClient(node=self.container.node)
        self.dpclient = DataProductManagementServiceClient(node=self.container.node)
        self.pubsubcli =  PubsubManagementServiceClient(node=self.container.node)
        self.damsclient = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.dataset_management = DatasetManagementServiceClient()

    def assert_can_load(self, scenarios, loadui=False, loadooi=False, path=TESTED_DOC, ui_path='default'):
        """ perform preload for given scenarios and raise exception if there is a problem with the data """
        config = dict(op="load",
                      scenario=scenarios,
                      attachments="res/preload/r2_ioc/attachments",
                      loadui=loadui,
                      loadooi=loadooi,
                      path=path, ui_path=ui_path,
                      assets='res/preload/r2_ioc/ooi_assets',
                      bulk=loadooi,
                      debug=True,
                      ooiexclude='DataProduct,DataProductLink',
                      )
        self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=config)

        # 'DataProduct,DataProductLink,WorkflowDefinition,ExternalDataProvider,ExternalDatasetModel,ExternalDataset,ExternalDatasetAgent,ExternalDatasetAgentInstance',

    def load_summer_deploy_assets(self):
        """ make sure UI assets are valid using DEFAULT_UI_ASSETS = 'https://userexperience.oceanobservatories.org/database-exports/' """
        ui_path = 'https://userexperience.oceanobservatories.org/database-exports/Candidates'
        self.assert_can_load(scenarios="X", loadui=True, loadooi=True, ui_path=ui_path)

    @unittest.skip('under construction.')
    def test_observatory(self):
        # Perform OOI preload for summer deployments (production mode, no debug, no bulk)
        self.load_summer_deploy_assets()

        #test an asset
        res_list, _  = self.RR.find_resources_ext(alt_id_ns="OOI", alt_id="CE04OSBP-LJ01C-06-CTDBPO108")
        log.debug('test_observatory  retrieve test:  %s', res_list)

        # Check OOI preloaded resources to see if they match needs for this test and for correctness
        observatory_list, _ = self.RR.find_resources_ext(restype=RT.Observatory)
        self.assertEquals(42, len(observatory_list))

        platform_site_list, _ = self.RR.find_resources(RT.PlatformSite, None, None, False)
        for ps in platform_site_list:
            log.debug('platform site: %s', ps.name)
        self.assertEquals(38, len(platform_site_list))

        platform_device_list, _ = self.RR.find_resources(RT.PlatformDevice, None, None, False)
        for pd in platform_device_list:
            log.debug('platform device: %s', pd.name)
        self.assertEquals(38, len(platform_site_list))

        platform_agent_list, _ = self.RR.find_resources(RT.PlatformAgent, None, None, False)
        self.assertEquals(2, len(platform_agent_list))
        for pa in platform_agent_list:
            log.debug('platform agent: %s', pa.name)

        deployment_list, _ = self.RR.find_resources(RT.Deployment, None, None, False)
        self.assertEquals(62, len(deployment_list))
        for d in deployment_list:
            log.debug('deployment: %s', d.name)

        # Check lcstates for select OOI resources: Some PLANNED, some INTEGRATED, some DEPLOYED
        for obs in observatory_list:
            self.assertEquals(obs.lcstate, 'DRAFT')
        for pdev in platform_device_list:
            self.assertEquals(pdev.lcstate, 'PLANNED')
        for pagent in platform_agent_list:
            self.assertEquals(pagent.lcstate, 'DEPLOYED')

        # See if Deployment for primary nodes is already active and in DEPLOYED lcstate, in particular CE04OSHY-PN01C
        for deploy in deployment_list:
            self.assertEquals(deploy.lcstate, 'PLANNED')

        dp_list, _  = self.RR.find_resources_ext(alt_id_ns="PRE", alt_id="CE04OSHY-PN01C_DEP")
        self.assertEquals(len(dp_list), 1)
        self.assertEquals(dp_list[0].availability, 'AVAILABLE')
        log.debug('test_observatory  retrieve CE04OSHY-PN01C_DEP deployment:  %s', res_list[0])

        # Check existing RSN node CE04OSHY-LV01C Deployment (PLANNED lcstate)
        CE04OSHY_LV01C_deployment = self.retrieve_ooi_asset(namespace='PRE', alt_id='CE04OSHY-LV01C_DEP')
        self.assertEquals(CE04OSHY_LV01C_deployment.lcstate, 'PLANNED')
        log.debug('test_observatory  retrieve RSN node CE04OSHY-LV01C Deployment:  %s', CE04OSHY_LV01C_deployment)

        # Set CE04OSHY-LV01C device to DEVELOPED state
        # MATURITY = ['DRAFT', 'PLANNED', 'DEVELOPED', 'INTEGRATED', 'DEPLOYED', 'RETIRED']
        CE04OSHY_LV01C_device = self.retrieve_ooi_asset(namespace='PRE', alt_id='CE04OSHY-LV01C_PD')
        #ret = self.RR.execute_lifecycle_transition(resource_id=CE04OSHY_LV01C_device._id, transition_event=LCE.PLANNED)
        self.transition_lcs_then_verify(resource_id=CE04OSHY_LV01C_device._id, new_lcs_state=LCE.DEVELOP, verify='DEVELOPED')


        # Set CE04OSHY-LV01C device to INTEGRATED state
        self.transition_lcs_then_verify(resource_id=CE04OSHY_LV01C_device._id, new_lcs_state=LCE.INTEGRATE, verify='INTEGRATED')

        # Set CE04OSHY-LV01C device to DEPLOYED state
        self.transition_lcs_then_verify(resource_id=CE04OSHY_LV01C_device._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Set CE04OSHY-LV01C Deployment to DEPLOYED state
        self.transition_lcs_then_verify(resource_id=CE04OSHY_LV01C_deployment._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Activate Deployment for CE04OSHY-LV01C
        self.OMS.activate_deployment(CE04OSHY_LV01C_deployment._id)

        # (optional) Start CE04OSHY-LV01C platform agent with simulator

        # Set DataProduct for CE04OSHY-LV01C platform to DEPLOYED state

        # Check events for CE04OSHY-LV01C platform


        # Check existing CE04OSBP-LJ01C Deployment (PLANNED lcstate)
#        dp_list, _  = self.RR.find_resources_ext(alt_id_ns="PRE", alt_id="CE04OSBP-LJ01C_DEP")
#        self.assertEquals(len(dp_list), 1)
#        CE04OSHY_LV01C_deployment = dp_list[0]
#        self.assertEquals(CE04OSHY_LV01C_deployment.lcstate, 'PLANNED')
#        log.debug('test_observatory  retrieve RSN node CE04OSBP-LJ01C Deployment:  %s', CE04OSHY_LV01C_deployment)


        # Set CE04OSBP-LJ01C Deployment to DEPLOYED state

        # Update description and other attributes for CE04OSBP-LJ01C device resource

        # Create attachment (JPG image) for CE04OSBP-LJ01C device resource

        # Activate Deployment for CE04OSBP-LJ01C

        # (optional) Add/register CE04OSBP-LJ01C platform agent to parent agent

        # (optional) Start CE04OSBP-LJ01C platform agent


        # Check existing RSN instrument CE04OSBP-LJ01C-06-CTDBPO108 Deployment (PLANNED lcstate)
        CE04OSBP_LJ01C_06_CTDBPO108_deploy = self.retrieve_ooi_asset(namespace='PRE', alt_id='CE04OSBP-LJ01C-06-CTDBPO108_DEP')
        self.assertEquals(CE04OSBP_LJ01C_06_CTDBPO108_deploy.lcstate, 'PLANNED')

        # Set CE04OSBP-LJ01C-06-CTDBPO108 device to DEVELOPED state
        CE04OSBP_LJ01C_06_CTDBPO108_device = self.retrieve_ooi_asset(namespace='PRE', alt_id='CE04OSBP-LJ01C-06-CTDBPO108_ID')
        self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_device._id, new_lcs_state=LCE.DEVELOP, verify='DEVELOPED')

        # Set CE04OSBP-LJ01C-06-CTDBPO108 device to INTEGRATED state
        self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_device._id, new_lcs_state=LCE.INTEGRATE, verify='INTEGRATED')

        # Set CE04OSBP-LJ01C-06-CTDBPO108 device to DEPLOYED state
        self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_device._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Set CE04OSBP-LJ01C-06-CTDBPO108 Deployment to DEPLOYED state
        self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_deploy._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Activate Deployment for CE04OSBP-LJ01C-06-CTDBPO108 instrument
        self.OMS.activate_deployment(CE04OSBP_LJ01C_06_CTDBPO108_deploy._id)

        # (optional) Add/register CE04OSBP-LJ01C-06-CTDBPO108 instrument agent to parent agent

        # (optional) Start CE04OSBP-LJ01C-06-CTDBPO108 instrument agent with simulator

        # Set all DataProducts for CE04OSBP-LJ01C-06-CTDBPO108 to DEPLOYED state


        # (optional) Create a substitute Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with a comparable device
        CE04OSBP_LJ01C_06_CTDBPO108_isite = self.retrieve_ooi_asset(namespace='PRE', alt_id='CE04OSBP-LJ01C-06-CTDBPO108')

        ## create device here: retrieve CTD Mooring on Mooring Riser 001 - similiar?
        GP03FLMB_RI001_10_CTDMOG999_ID_idevice = self.retrieve_ooi_asset(namespace='PRE', alt_id='GP03FLMB-RI001-10-CTDMOG999_ID')

        deploy_id_2 = self.create_basic_deployment(name='CE04OSBP-LJ01C-06-CTDBPO108_DEP2', description='substitute Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with a comparable device')
        self.IMS.deploy_instrument_device(instrument_device_id=GP03FLMB_RI001_10_CTDMOG999_ID_idevice._id, deployment_id=deploy_id_2)
        self.OMS.deploy_instrument_site(instrument_site_id=CE04OSBP_LJ01C_06_CTDBPO108_isite._id, deployment_id=deploy_id_2)

        # (optional) Activate this second deployment - check first deployment is deactivated
        #self.OMS.activate_deployment(deploy_id_2)
        #todo: check first deployment is deactivated

        # (optional) Set first CE04OSBP-LJ01C-06-CTDBPO108 Deployment to INTEGRATED state

        # Set first CE04OSBP-LJ01C-06-CTDBPO108 device to INTEGRATED state


        # (optional) Create a third Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with a same device from first deployment

        # Set first CE04OSBP-LJ01C-06-CTDBPO108 device to DEPLOYED state

        # (optional) Activate this third deployment - check second deployment is deactivated


        # Check that glider GP05MOAS-GL001 assembly is defined by OOI preload (3 instruments)
        GP05MOAS_GL001_device = self.retrieve_ooi_asset(namespace='PRE', alt_id='GP05MOAS-GL001_PD')
        child_devs, assns =self.RR.find_objects(subject=GP05MOAS_GL001_device._id, predicate=PRED.hasDevice, id_only=True)
        self.assertEquals(len(child_devs), 3)

        # Set GP05MOAS-GL001 Deployment to DEPLOYED
        GP05MOAS_GL001_deploy = self.retrieve_ooi_asset(namespace='PRE', alt_id='GP05MOAS-GL001_DEP')
        self.transition_lcs_then_verify(resource_id=GP05MOAS_GL001_deploy._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Activate Deployment for GP05MOAS-GL001
        #self.OMS.activate_deployment(GP05MOAS_GL001_deploy._id)

        # Deactivate Deployment for GP05MOAS-GL001
        #self.OMS.deactivate_deployment(GP05MOAS_GL001_deploy._id)


        # Create a new Deployment resource X without any assignment
        x_deploy_id = self.create_basic_deployment(name='X_Deployment', description='new Deployment resource X without any assignment')

        # Assign Deployment X to site GP05MOAS-GL001
        GP05MOAS_GL001_psite = self.retrieve_ooi_asset(namespace='PRE', alt_id='GP05MOAS-GL001')
        self.OMS.deploy_platform_site(GP05MOAS_GL001_psite._id, x_deploy_id)

        # Assign Deployment X to first device for GP05MOAS-GL001
        GP05MOAS_GL001_device = self.retrieve_ooi_asset(namespace='PRE', alt_id='GP05MOAS-GL001_PD')
        self.IMS.deploy_platform_device(GP05MOAS_GL001_device._id, x_deploy_id)

        # Set GP05MOAS-GL001 Deployment to PLANNED state
        #self.transition_lcs_then_verify(resource_id=x_deploy_id, new_lcs_state=LCE.PLAN, verify='PLANNED')
        # ??? already in planned

        # Set second GP05MOAS-GL001 Deployment to DEPLOYED
        self.transition_lcs_then_verify(resource_id=x_deploy_id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Activate second Deployment for GP05MOAS-GL001
        #self.OMS.activate_deployment(x_deploy_id)

        # Deactivate second Deployment for GP05MOAS-GL001
        #self.OMS.deactivate_deployment(x_deploy_id)


        # Set several CE01ISSM-RI002-* instrument devices to DEVELOPED state

        # Assemble several CE01ISSM-RI002-* instruments to a CG CE01ISSM-RI002 component platform

        # Set several CE01ISSM-RI002-* instrument devices to INTEGRATED state

        # Assemble CE01ISSM-RI002 platform to CG CE01ISSM-LM001 station platform

        # Set CE01ISSM-RI002 component device to INTEGRATED state

        # Set CE01ISSM-LM001 station device to INTEGRATED state

        # Set CE01ISSM-LM001 station device to DEPLOYED state (children maybe too?)

        # Set CE01ISSM-LM001 Deployment to DEPLOYED

        # Activate CE01ISSM-LM001 platform assembly deployment


        # Dectivate CE01ISSM-LM001 platform assembly deployment

        # Set CE01ISSM-LM001 Deployment to INTEGRATED state

        # Set CE01ISSM-LM001 station device to INTEGRATED state

        # Set CE01ISSM-RI002 component device to INTEGRATED state

        # Set CE01ISSM-RI002 component device to INTEGRATED state

        # Disassemble CE01ISSM-RI002 platform from CG CE01ISSM-LM001 station platform

        # Disassemble all CE01ISSM-RI002-* instruments from a CG CE01ISSM-RI002 component platform


        # Retire instrument one for CE01ISSM-RI002-*

        # Retire device one for CE01ISSM-RI002

        # Retire device one for CE01ISSM-LM001


        # Add a new instrument agent

        # Add a new instrument agent instance

        # Check DataProducts

        # Check provenance

        pass

    def retrieve_ooi_asset(self, namespace='', alt_id=''):
        dp_list, _  = self.RR.find_resources_ext(alt_id_ns=namespace, alt_id=alt_id)
        self.assertEquals(len(dp_list), 1)
        return dp_list[0]

    def transition_lcs_then_verify(self, resource_id, new_lcs_state, verify):
        ret = self.RR2.advance_lcs(resource_id, new_lcs_state)
        resource_obj = self.RR.read(resource_id)
        self.assertEquals(resource_obj.lcstate, verify)

    def create_basic_deployment(self, name='', description=''):
        start = IonTime(datetime.datetime(2013,1,1))
        end = IonTime(datetime.datetime(2014,1,1))
        temporal_bounds = IonObject(OT.TemporalBounds, name='planned', start_datetime=start.to_string(), end_datetime=end.to_string())
        deployment_obj = IonObject(RT.Deployment,
            name=name,
            description=description,
            context=IonObject(OT.CabledNodeDeploymentContext),
            constraint_list=[temporal_bounds])
        return self.OMS.create_deployment(deployment_obj)