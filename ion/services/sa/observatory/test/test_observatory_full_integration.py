#!/usr/bin/env python

import datetime
import unittest
import inspect
from nose.plugins.attrib import attr

from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import RT, PRED, OT, log, LCE, LCS, AS
from pyon.public import IonObject
from pyon.util.ion_time import IonTime

from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.coi.iorg_management_service import OrgManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.dm.idataset_management_service import DatasetManagementServiceClient


STAGE_LOAD_ORGS = 1
STAGE_LOAD_PARAMS = 2
STAGE_LOAD_AGENTS = 3
STAGE_LOAD_ASSETS = 4


@attr('INT', group='sa')
class TestObservatoryManagementFullIntegration(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()

        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        self.RR = ResourceRegistryServiceClient()
        self.RR2 = EnhancedResourceRegistryClient(self.RR)
        self.OMS = ObservatoryManagementServiceClient()
        self.org_management_service = OrgManagementServiceClient()
        self.IMS =  InstrumentManagementServiceClient()
        self.dpclient = DataProductManagementServiceClient()
        self.pubsubcli =  PubsubManagementServiceClient()
        self.damsclient = DataAcquisitionManagementServiceClient()
        self.dataset_management = DatasetManagementServiceClient()

        self._load_stage = 0
        self._resources = {}

    def preload_ooi(self, stage=STAGE_LOAD_ASSETS):
        # Preloads OOI up to a given stage

        if self._load_stage >= stage:
            return

        if self._load_stage < STAGE_LOAD_ORGS:
            log.info("--------------------------------------------------------------------------------------------------------")
            log.info("Preloading stage: %s (OOIR2 Orgs, users, roles)", STAGE_LOAD_ORGS)
            # load_OOIR2_scenario
            self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=dict(
                op="load",
                scenario="OOIR2",
                path="master",
                ))
            self._load_stage = STAGE_LOAD_ORGS

        if self._load_stage < STAGE_LOAD_PARAMS:
            log.info("--------------------------------------------------------------------------------------------------------")
            log.info("Preloading stage: %s (BASE params, streamdefs)", STAGE_LOAD_PARAMS)
            # load_parameter_scenarios
            self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=dict(
                op="load",
                scenario="BETA",
                path="master",
                categories="ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition",
                clearcols="owner_id,org_ids",
                assets="res/preload/r2_ioc/ooi_assets",
                parseooi="True",
                ))
            self._load_stage = STAGE_LOAD_PARAMS

        if self._load_stage < STAGE_LOAD_AGENTS:
            log.info("--------------------------------------------------------------------------------------------------------")
            log.info("Preloading stage: %s (OOIR2_I agents, model links)", STAGE_LOAD_AGENTS)
            # load_OOIR2_agents
            self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=dict(
                op="load",
                scenario="OOIR2_I",
                path="master",
                ))
            self._load_stage = STAGE_LOAD_AGENTS

        if self._load_stage < STAGE_LOAD_ASSETS:
            log.info("--------------------------------------------------------------------------------------------------------")
            log.info("Preloading stage: %s (OOI assets linked to params, agents)", STAGE_LOAD_ASSETS)
            # load_ooi_assets
            self.container.spawn_process("Loader", "ion.processes.bootstrap.ion_loader", "IONLoader", config=dict(
                op="load",
                loadooi="True",
                path="master",
                assets="res/preload/r2_ioc/ooi_assets",
                bulk="True",
                debug="True",
                ooiuntil="9/1/2013",
                ooiparams="True",
                #excludecategories: DataProduct,DataProductLink,Deployment,Workflow,WorkflowDefinition
                ))
            self._load_stage = STAGE_LOAD_ASSETS

        # 'DataProduct,DataProductLink,WorkflowDefinition,ExternalDataProvider,ExternalDatasetModel,ExternalDataset,ExternalDatasetAgent,ExternalDatasetAgentInstance',


    #@unittest.skip('Work in progress')
    def test_observatory(self):
        self._load_stage = 0
        self._resources = {}
        passing = True

        self.assertTrue(True)

        # LOAD STEP 1
        self.preload_ooi(stage=STAGE_LOAD_ORGS)

        passing &= self.orguserrole_assertions()


        # LOAD STEP 2
        self.preload_ooi(stage=STAGE_LOAD_PARAMS)

        passing &= self.parameter_assertions()


        # LOAD STEP 3
        self.preload_ooi(stage=STAGE_LOAD_AGENTS)

        passing &= self.agent_assertions()


        # LOAD STEP 4
        self.preload_ooi(stage=STAGE_LOAD_ASSETS)

        # Check OOI preloaded resources to see if they match needs for this test and for correctness
        passing &= self.sites_assertions()
        passing &= self.device_assertions()
        passing &= self.deployment_assertions()

        # Extensive tests on select RSN nodes
        passing &= self.rsn_node_checks()

        # Extensive tests on select RSN instruments
        passing &= self.check_rsn_instrument()

        passing &= self.check_rsn_instrument_data_product()

        # Extensive tests on a glider
        #passing &= self.check_glider()

        # Extensive tests on a CG assembly
        #passing &= self.check_cg_assembly()



        # Add a new instrument agent
        # Add a new instrument agent instance
        # Check DataProducts
        # Check Provenance

        IonIntegrationTestCase.assertTrue(self, passing)


    # -------------------------------------------------------------------------

    def orguserrole_assertions(self):
        passing = True

        passing &= self._check_marine_facility("MF_CGSN")
        passing &= self._check_marine_facility("MF_RSN")
        passing &= self._check_marine_facility("MF_EA")

        return passing

    def _check_marine_facility(self, preload_id):
        passing = True
        log.debug("Checking marine facility %s and associations", preload_id)

        mf_obj = self.retrieve_ooi_asset(preload_id)
        mf_id = mf_obj._id
        self._resources[preload_id] = mf_id

        passing &= self.assertEquals(mf_obj.lcstate, LCS.DEPLOYED)

        res_list, _ = self.RR.find_objects(subject=mf_id, predicate=PRED.hasMembership, id_only=True)
        passing &= self.assertTrue(len(res_list) >= 3)

        res_list, _ = self.RR.find_objects(subject=mf_id, predicate=PRED.hasRole, id_only=False)
        passing &= self.assertTrue(len(res_list) >= 5)

        passing &= self._check_role_assignments(res_list, "ORG_MANAGER")
        passing &= self._check_role_assignments(res_list, "OBSERVATORY_OPERATOR")
        passing &= self._check_role_assignments(res_list, "INSTRUMENT_OPERATOR")

        return passing

    def _check_role_assignments(self, role_list, role_name):
        passing = True
        role_obj = self._find_resource_in_list(role_list, "governance_name", role_name)
        if role_obj:
            res_list = self.RR.find_subjects(predicate=PRED.hasRole, object=role_obj._id, id_only=True)
            passing &= self.assertTrue(len(res_list) >= 1)

        return passing


    def parameter_assertions(self):
        passing = True

        pctx_list, _ = self.RR.find_resources_ext(restype=RT.ParameterContext)
        passing &= self.assertTrue(len(pctx_list) >= 10)

        pdict_list, _ = self.RR.find_resources_ext(restype=RT.ParameterDictionary)
        passing &= self.assertTrue(len(pdict_list) >= 10)

        sdef_list, _ = self.RR.find_resources_ext(restype=RT.StreamDefinition)
        passing &= self.assertTrue(len(sdef_list) >= 10)

        # TODO: More tests?

        return passing

    def agent_assertions(self):
        passing = True

        # TODO: More tests?

        return passing

    def sites_assertions(self):
        passing = True
        observatory_list, _ = self.RR.find_resources_ext(restype=RT.Observatory)
        passing &= self.assertTrue(len(observatory_list) >= 40)
        for obs in observatory_list:
            passing &= self.assertEquals(obs.lcstate, LCS.DEPLOYED)

        platform_site_list, _ = self.RR.find_resources(RT.PlatformSite, id_only=False)
        log.debug('platform sites: %s', [ps.name for ps in platform_site_list])
        passing &= self.assertTrue(len(platform_site_list) >= 30)

        return passing

    def device_assertions(self):
        passing = True
        platform_device_list, _ = self.RR.find_resources(RT.PlatformDevice, id_only=False)
        passing &= self.assertTrue(len(platform_device_list) >= 30)
        for pdev in platform_device_list:
            log.debug('platform device: %s', pdev.name)
            passing &= self.assertEquals(pdev.lcstate, LCS.PLANNED)

        platform_agent_list, _ = self.RR.find_resources(RT.PlatformAgent, id_only=False)
        passing &= self.assertTrue(len(platform_agent_list) >= 2)
        for pagent in platform_agent_list:
            log.debug('platform agent: %s', pagent.name)
            passing &= self.assertEquals(pagent.lcstate, LCS.DEPLOYED)

        instrument_agent_list, _ = self.RR.find_resources(RT.InstrumentAgent, id_only=False)
        passing &= self.assertTrue(len(instrument_agent_list) >= 3)
        for iagent in instrument_agent_list:
            log.debug('instrument agent: %s', iagent.name)
            passing &= self.assertEquals(iagent.lcstate, LCS.DEPLOYED)

            model_list, _ = self.RR.find_objects(subject=iagent._id, predicate=PRED.hasModel, id_only=True)
            passing &= self.assertTrue(len(model_list) >= 1, "IA %s" % iagent.name)

        return passing

    def deployment_assertions(self):
        passing = True
        deployment_list, _ = self.RR.find_resources(RT.Deployment, id_only=False)
        passing &= self.assertTrue(len(deployment_list) >= 30)
        for deploy in deployment_list:
            log.debug('deployment: %s', deploy.name)
            passing &= self.assertEquals(deploy.lcstate, LCS.DEPLOYED)
        return passing


    def rsn_node_checks(self):
        """
        Current preload creates:
        - PlatformDevice in PLANNED
        - PlatformSite in DEPLOYED
        - Deployment in DEPLOYED
        - Deployment is NOT activated
        """
        passing = True

        dp_obj = self.retrieve_ooi_asset("CE04OSHY-PN01C_DEP")

        passing &= self.assertEquals(dp_obj.lcstate, LCS.DEPLOYED)
        passing &= self.assertEquals(dp_obj.availability, AS.AVAILABLE)
        log.debug('test_observatory  retrieve CE04OSHY-PN01C_DEP deployment:  %s', dp_obj)

        # Check existing RSN node CE04OSHY-LV01C Deployment (PLANNED lcstate)
        CE04OSHY_LV01C_deployment = self.retrieve_ooi_asset('CE04OSHY-LV01C_DEP')

        #self.dump_deployment(CE04OSHY_LV01C_deployment._id)
        log.debug('test_observatory  retrieve RSN node CE04OSHY-LV01C Deployment:  %s', CE04OSHY_LV01C_deployment)
        #passing &= self.assertEquals(CE04OSHY_LV01C_deployment.lcstate, 'PLANNED')

        CE04OSHY_LV01C_device = self.retrieve_ooi_asset('CE04OSHY-LV01C_PD')
        #ret = self.RR.execute_lifecycle_transition(resource_id=CE04OSHY_LV01C_device._id, transition_event=LCE.PLANNED)
        
        # Set CE04OSHY-LV01C device to DEVELOPED state
        passing &= self.transition_lcs_then_verify(resource_id=CE04OSHY_LV01C_device._id, new_lcs_state=LCE.DEVELOP, verify=LCS.DEVELOPED)

        # Set CE04OSHY-LV01C device to INTEGRATED state
        passing &= self.transition_lcs_then_verify(resource_id=CE04OSHY_LV01C_device._id, new_lcs_state=LCE.INTEGRATE, verify=LCS.INTEGRATED)

        # Set CE04OSHY-LV01C device to DEPLOYED state
        passing &= self.transition_lcs_then_verify(resource_id=CE04OSHY_LV01C_device._id, new_lcs_state=LCE.DEPLOY, verify=LCS.DEPLOYED)

        # Set CE04OSHY-LV01C Deployment to DEPLOYED state
        # NOTE: Deployments are created in DEPLOYED state, currently
        #self.transition_lcs_then_verify(resource_id=CE04OSHY_LV01C_deployment._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')
        
        # Activate Deployment for CE04OSHY-LV01C
        self.OMS.activate_deployment(CE04OSHY_LV01C_deployment._id)
        log.debug('---------    activate_deployment CE04OSHY_LV01C_deployment -------------- ')
        self.dump_deployment(CE04OSHY_LV01C_deployment._id)
        passing &= self.validate_deployment_activated(CE04OSHY_LV01C_deployment._id)
        
        # (optional) Start CE04OSHY-LV01C platform agent with simulator

        # NOTE: DataProduct is generated in DEPLOYED state
        # # Set DataProduct for CE04OSHY-LV01C platform to DEPLOYED state
        # output_data_product_ids, assns =self.RR.find_objects(subject=CE04OSHY_LV01C_device._id, predicate=PRED.hasOutputProduct, id_only=True)
        # if output_data_product_ids:
        #     #self.assertEquals(len(child_devs), 3)
        #     for output_data_product_id in output_data_product_ids:
        #         log.debug('DataProduct for CE04OSHY-LV01C platform:  %s', output_data_product_id)
        #         self.transition_lcs_then_verify(resource_id=output_data_product_id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

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

        return passing
    
    def check_rsn_instrument(self):
        """
        Check existing RSN instrument CE04OSBP-LJ01C-06-CTDBPO108 Deployment (PLANNED lcstate)
        Current preload creates:
        - InstrumentDevice in PLANNED
        - InstrumentSite in DEPLOYED
        - Deployment in DEPLOYED
        - Deployment is activated
        """

        passing = True
        CE04OSBP_LJ01C_06_CTDBPO108_deploy = self.retrieve_ooi_asset('CE04OSBP-LJ01C-06-CTDBPO108_DEP')
        self.dump_deployment(CE04OSBP_LJ01C_06_CTDBPO108_deploy._id)
        #passing &= self.assertEquals(CE04OSBP_LJ01C_06_CTDBPO108_deploy.lcstate, 'PLANNED')

        # Set CE04OSBP-LJ01C-06-CTDBPO108 device to DEVELOPED state
        CE04OSBP_LJ01C_06_CTDBPO108_device = self.retrieve_ooi_asset('CE04OSBP-LJ01C-06-CTDBPO108_ID')
        passing &= self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_device._id, new_lcs_state=LCE.DEVELOP, verify='DEVELOPED')

        # Set CE04OSBP-LJ01C-06-CTDBPO108 device to INTEGRATED state
        passing &= self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_device._id, new_lcs_state=LCE.INTEGRATE, verify='INTEGRATED')

        # Set CE04OSBP-LJ01C-06-CTDBPO108 device to DEPLOYED state
        passing &= self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_device._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Set CE04OSBP-LJ01C-06-CTDBPO108 Deployment to DEPLOYED state
        #self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_deploy._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Activate Deployment for CE04OSBP-LJ01C-06-CTDBPO108 instrument
        log.debug('---------    activate_deployment CE04OSBP-LJ01C-06-CTDBPO108 deployment -------------- ')
        self.OMS.activate_deployment(CE04OSBP_LJ01C_06_CTDBPO108_deploy._id)
        passing &= self.validate_deployment_activated(CE04OSBP_LJ01C_06_CTDBPO108_deploy._id)

        # (optional) Add/register CE04OSBP-LJ01C-06-CTDBPO108 instrument agent to parent agent

        # (optional) Start CE04OSBP-LJ01C-06-CTDBPO108 instrument agent with simulator

        # Set all DataProducts for CE04OSBP-LJ01C-06-CTDBPO108 to DEPLOYED state


        # (optional) Create a substitute Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with a comparable device
        CE04OSBP_LJ01C_06_CTDBPO108_isite = self.retrieve_ooi_asset('CE04OSBP-LJ01C-06-CTDBPO108')

        ## create device here: retrieve CTD Mooring on Mooring Riser 001 - similiar?
        GP03FLMB_RI001_10_CTDMOG999_ID_idevice = self.retrieve_ooi_asset('GP03FLMB-RI001-10-CTDMOG999_ID')

        deploy_id_2 = self.create_basic_deployment(name='CE04OSBP-LJ01C-06-CTDBPO108_DEP2', description='substitute Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with a comparable device')
        self.IMS.deploy_instrument_device(instrument_device_id=GP03FLMB_RI001_10_CTDMOG999_ID_idevice._id, deployment_id=deploy_id_2)
        self.OMS.deploy_instrument_site(instrument_site_id=CE04OSBP_LJ01C_06_CTDBPO108_isite._id, deployment_id=deploy_id_2)
        self.dump_deployment(deploy_id_2)

        # (optional) Activate this second deployment - check first deployment is deactivated
        self.OMS.deactivate_deployment(CE04OSBP_LJ01C_06_CTDBPO108_deploy._id)
        passing &= self.validate_deployment_deactivated(CE04OSBP_LJ01C_06_CTDBPO108_deploy._id)


        # log.debug('Activate deployment deploy_id_2')
        # self.get_deployment_ids(deploy_id_2)
        # self.dump_deployment(deploy_id_2, "deploy_id_2")
        # self.OMS.activate_deployment(deploy_id_2)
        # passing &= self.validate_deployment_deactivated(CE04OSBP_LJ01C_06_CTDBPO108_deploy._id)
        #
        # # (optional) Set first CE04OSBP-LJ01C-06-CTDBPO108 Deployment to INTEGRATED state
        # passing &= self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_deploy._id, new_lcs_state=LCE.INTEGRATE, verify='INTEGRATED')
        #
        # # Set first CE04OSBP-LJ01C-06-CTDBPO108 device to INTEGRATED state
        # passing &= self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_device._id, new_lcs_state=LCE.INTEGRATE, verify='INTEGRATED')
        #
        #
        # # (optional) Create a third Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with a same device from first deployment
        # deploy_id_3 = self.create_basic_deployment(name='CE04OSBP-LJ01C-06-CTDBPO108_DEP3', description='substitute Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with same device as first')
        # self.IMS.deploy_instrument_device(instrument_device_id=GP03FLMB_RI001_10_CTDMOG999_ID_idevice._id, deployment_id=deploy_id_3)
        # self.OMS.deploy_instrument_site(instrument_site_id=CE04OSBP_LJ01C_06_CTDBPO108_isite._id, deployment_id=deploy_id_3)
        # self.dump_deployment(deploy_id_3)
        #
        #
        # # Set first CE04OSBP-LJ01C-06-CTDBPO108 device to DEPLOYED state
        # passing &= self.transition_lcs_then_verify(resource_id=CE04OSBP_LJ01C_06_CTDBPO108_device._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')
        #
        # # (optional) Activate this third deployment - check second deployment is deactivated
        # log.debug('Activate deployment deploy_id_3')
        # self.dump_deployment(deploy_id_3)
        # self.OMS.activate_deployment(deploy_id_3)
        # #todo: check second deployment is deactivated

        return passing

    def check_rsn_instrument_data_product(self):
        passing = True
        return passing

    def check_glider(self):
        '''
        # Check that glider GP05MOAS-GL001 assembly is defined by OOI preload (3 instruments)
        '''
        passing = True
        GP05MOAS_GL001_device = self.retrieve_ooi_asset('GP05MOAS-GL001_PD')
        child_devs, assns =self.RR.find_objects(subject=GP05MOAS_GL001_device._id, predicate=PRED.hasDevice, id_only=True)
        passing &= self.assertEquals(len(child_devs), 3)

        # Set GP05MOAS-GL001 Deployment to DEPLOYED
        GP05MOAS_GL001_deploy = self.retrieve_ooi_asset('GP05MOAS-GL001_DEP')
        passing &= self.transition_lcs_then_verify(resource_id=GP05MOAS_GL001_deploy._id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')

        # Activate Deployment for GP05MOAS-GL001
        #self.OMS.activate_deployment(GP05MOAS_GL001_deploy._id)

        # Deactivate Deployment for GP05MOAS-GL001
        #self.OMS.deactivate_deployment(GP05MOAS_GL001_deploy._id)


        # Create a new Deployment resource X without any assignment
        x_deploy_id = self.create_basic_deployment(name='X_Deployment', description='new Deployment resource X without any assignment')

        # Assign Deployment X to site GP05MOAS-GL001
        GP05MOAS_GL001_psite = self.retrieve_ooi_asset('GP05MOAS-GL001')
        self.OMS.deploy_platform_site(GP05MOAS_GL001_psite._id, x_deploy_id)

        # Assign Deployment X to first device for GP05MOAS-GL001
        GP05MOAS_GL001_device = self.retrieve_ooi_asset('GP05MOAS-GL001_PD')
        self.IMS.deploy_platform_device(GP05MOAS_GL001_device._id, x_deploy_id)

        # Set GP05MOAS-GL001 Deployment to PLANNED state
        #self.transition_lcs_then_verify(resource_id=x_deploy_id, new_lcs_state=LCE.PLAN, verify='PLANNED')
        # ??? already in planned

        # Set second GP05MOAS-GL001 Deployment to DEPLOYED
        passing &= self.transition_lcs_then_verify(resource_id=x_deploy_id, new_lcs_state=LCE.DEPLOY, verify='DEPLOYED')
        self.dump_deployment(x_deploy_id)

        # Activate second Deployment for GP05MOAS-GL001
        #self.OMS.activate_deployment(x_deploy_id)

        # Deactivate second Deployment for GP05MOAS-GL001
        #self.OMS.deactivate_deployment(x_deploy_id)
        return passing


    def check_cg_assembly(self):
        passing = True

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

        return passing

    # -------------------------------------------------------------------------

    def retrieve_ooi_asset(self, alt_id='', namespace='PRE'):
        dp_list, _  = self.RR.find_resources_ext(alt_id_ns=namespace, alt_id=alt_id)
        self.assertEquals(len(dp_list), 1)
        return dp_list[0]

    def transition_lcs_then_verify(self, resource_id, new_lcs_state, verify):
        ret = self.RR2.advance_lcs(resource_id, new_lcs_state)
        resource_obj = self.RR.read(resource_id)
        return self.assertEquals(resource_obj.lcstate, verify)

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

    def validate_deployment_activated(self, deployment_id=''):
        site_id, device_id = self.get_deployment_ids(deployment_id)
        assocs = self.RR.find_associations(subject=site_id, predicate=PRED.hasDevice, object=device_id)
        return self.assertEquals(len(assocs), 1)

    def validate_deployment_deactivated(self, deployment_id=''):
        site_id, device_id = self.get_deployment_ids(deployment_id)
        assocs = self.RR.find_associations(subject=site_id, predicate=PRED.hasDevice, object=device_id)
        return self.assertEquals(len(assocs), 0)

    def dump_deployment(self, deployment_id='', name=""):
        #site_id, device_id = self.get_deployment_ids(deployment_id)
        resource_list,_ = self.RR.find_subjects(predicate=PRED.hasDeployment, object=deployment_id, id_only=True)
        resource_list.append(deployment_id)
        resources = self.RR.read_mult(resource_list )
        log.debug('---------   dump_deployment %s summary---------------', name)
        for resource in resources:
            log.debug('%s: %s (%s)', resource._get_type(), resource.name, resource._id)

        log.debug('---------   dump_deployment %s full dump ---------------', name)

        for resource in resources:
            log.debug('resource: %s ', resource)
        log.debug('---------   dump_deployment %s end  ---------------', name)


        #assocs = self.container.resource_registry.find_assoctiations(anyside=deployment_id)
#        assocs = Container.instance.resource_registry.find_assoctiations(anyside=deployment_id)
#        log.debug('---------   dump_deployment  ---------------')
#        for assoc in assocs:
#            log.debug('SUBJECT: %s      PREDICATE: %s OBJET: %s', assoc.s, assoc.p, assoc.o)
#        log.debug('---------   dump_deployment  end  ---------------')


    def get_deployment_ids(self, deployment_id=''):
        devices = []
        sites = []
        idevice_list,_ = self.RR.find_subjects(RT.InstrumentDevice, PRED.hasDeployment, deployment_id, id_only=True)
        pdevice_list,_ = self.RR.find_subjects(RT.PlatformDevice, PRED.hasDeployment, deployment_id, id_only=True)
        devices = idevice_list + pdevice_list
        self.assertEquals(1, len(devices))
        isite_list,_ = self.RR.find_subjects(RT.InstrumentSite, PRED.hasDeployment, deployment_id, id_only=True)
        psite_list,_ = self.RR.find_subjects(RT.PlatformSite, PRED.hasDeployment, deployment_id, id_only=True)
        sites = isite_list + psite_list
        self.assertEquals(1, len(sites))
        return sites[0], devices[0]

    def _find_resource_in_list(self, res_list, attr, attr_val, assert_found=True):
        for res in res_list:
            v = getattr(res, attr, None)
            if v == attr_val:
                return res
        if assert_found:
            self.assertTrue(False, "Attribute %s value %s not found in list" % (attr, attr_val))
        return None

    # -------------------------------------------------------------------------

    def _get_caller(self):
        s = inspect.stack()
        return "%s:%s" % (s[2][1], s[2][2])

    def assertEquals(self, *args, **kwargs):
        try:
            IonIntegrationTestCase.assertEquals(self, *args, **kwargs)
            return True
        except AssertionError:
            log.exception('Assertion Failed in %s', self._get_caller())
        return False

    def assertTrue(self, *args, **kwargs):
        try:
            IonIntegrationTestCase.assertTrue(self, *args, **kwargs)
            return True
        except AssertionError:
            log.exception('Assertion Failed in %s', self._get_caller())
        return False
