#!/usr/bin/env python
# coding=utf-8

"""
@package ion.services.sa.observatory.test.test_observatory_device_activation
@file ion/services/sa/observatory/test/test_observatory_device_activation.py
@author Edward Hunter
@brief Integration test cases to confirm registration and activation services
for marine device resources.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon object and resource imports.
from pyon.public import IonObject, log, RT, PRED, LCS, LCE, OT, CFG, AS

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

# Standard imports
import time

# Nonstandard imports.
from nose.plugins.attrib import attr

# Service clients.
from interface.services.coi.iidentity_management_service \
    import IdentityManagementServiceClient
from interface.services.sa.iobservatory_management_service \
    import ObservatoryManagementServiceClient
from interface.services.sa.iinstrument_management_service \
    import InstrumentManagementServiceClient
from ion.util.enhanced_resource_registry_client \
    import EnhancedResourceRegistryClient

# Preload agumented and new resources.
from activation_test_resources import *

"""
--with-queueblame   report leftover queues
--with-pycc         run in separate container
--with-greenletleak ewpoer leftover greenlets
bin/nosetests -s -v --nologcapture ion/services/sa/observatory/test/test_observatory_device_activation.py:TestDeviceActivation
bin/nosetests -s -v --nologcapture ion/services/sa/observatory/test/test_observatory_device_activation.py:TestDeviceActivation.test_device_activation
"""

# The followind defines preload phases used for test.
# Total load time: ~180s (but varies widely).
LOAD_PHASES = [
    {
        'phase' : 'OOIR2',
        'op' : 'load',
        'scenario' : 'OOIR2'
    },
    {
        'phase' : 'BETA',
        'op' : 'load',
        'scenario' : 'BETA',
        'categories' : 'ParameterFunctions,ParameterDefs,ParameterDictionary,StreamDefinition',
        'clearcols' : 'owner_id,org_ids',
        'assets' : 'res/preload/r2_ioc/ooi_assets'
    },
    {
        'phase' : 'OOI_ASSETS',
        'op' : 'load',
        'loadooi' : 'True',
        'path' : 'master',
        'assets' : 'res/preload/r2_ioc/ooi_assets',
        'ooiuntil' : '12/31/2013', #Note: this is broken if set to future.
        'ooiparams' : 'True'
    }
]


@attr('INT', group='sa')
class TestDeviceActivation(IonIntegrationTestCase):
    """
    Integration test cases to confirm registration and activation services
    for marine device resources.
    """

    def setUp(self):
        """
        Test setup.
        """
        # Start container.
        log.info('Staring capability container.')
        self._start_container()

        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Setup service clients.
        self.idms = IdentityManagementServiceClient(node=self.container.node)
        self.oms = ObservatoryManagementServiceClient(node=self.container.node)
        self.ims = InstrumentManagementServiceClient(node=self.container.node)
        self.RR2 = EnhancedResourceRegistryClient(self.container.resource_registry)

        # Load the system resources.
        self._preload()

        # Augment preload with resources that will eventually reside there.
        # This to be removed when preload is updated or a suitable test
        # scenario developed. Will require adjustments to the test likely.
        self._augment_preload()

        # Dump augmented registry for inspection. For debugging.
        """
        self.container.spawn_process('DSLoader',
            'ion.processes.bootstrap.datastore_loader', 'DatastoreLoader',
            config=dict(op='dumpres'))
        """

    def _preload(self):
        """
        Preload the system into its initial state with OOI resources.
        """

        beginning = time.time()
        for x in LOAD_PHASES:
            phase = x.pop('phase')
            log.info('-'*80)
            log.info('Loading: %s', phase)
            log.info('-'*80)
            start = time.time()
            self.container.spawn_process('Loader',
                'ion.processes.bootstrap.ion_loader', 'IONLoader', config=x)

            finish = time.time()
            delta = finish - start
            log.info('-'*80)
            log.info('Loaded %s in %f seconds.',phase, delta)
            log.info('-'*80)

        delta = time.time() - beginning
        log.info('-'*80)
        log.info('Total load time: %f seconds.', delta)
        log.info('-'*80)


    def _dump_obj(self, obj):
        """
        Dump object and associations to log.
        """

        dump_str = '\n'
        dump_str += '='*200 + '\n'
        for k,v in obj.__dict__.iteritems():
            dump_str += '%-50s %s\n' % (str(k), str(v))
        dump_str += '='*200 + '\n'
        subjects = self.container.resource_registry.find_associations(
                                                            object=obj['_id'])
        for s in subjects:
            sobj = self.container.resource_registry.read(s['s'])
            dump_str += '%-30s %-120s %-30s this %-30s\n' % \
                (s['st'], sobj['name'], s['p'], s['ot'])
        dump_str += '='*200 + '\n'
        objects = self.container.resource_registry.find_associations(
                                                            subject=obj['_id'])
        for o in objects:
            oobj = self.container.resource_registry.read(o['o'])
            dump_str += 'this %-30s %-30s %-30s %-120s\n' % \
                (o['st'],  o['p'], o['ot'], oobj['name'])
        dump_str += '='*200
        log.info(dump_str)


    def _retrieve_ooi_asset(self, alt_id='', namespace='PRE'):
        """
        Retrieve a unique OOI asset by alt_id.
        """
        dp_list, _  = self.container.resource_registry.find_resources_ext(
            alt_id_ns=namespace, alt_id=alt_id)
        self.assertEquals(len(dp_list), 1)
        return dp_list[0]


    def _augment_preload(self):
        """
        Supplements preload as of 2/12/2014 to create site and deployment
        objects that would normally already exist. Remove when preload is
        updated.
        """

        self._orgs = {}
        mf_rsn = self._retrieve_ooi_asset(RSN_FACILITY_ALT_ID)
        self._orgs['rsn'] = mf_rsn

        # Create and link instrument sites.
        # Note: No LCS on sites.
        #BadRequest: 400 - Resource id=93e720217d95412487c602bab4031150
        #type=InstrumentSite has no lifecycle
        for config in AUGMENT_INSTRUMENT_SITES:
            org = config.pop('org')
            parent_site_alt_id = config.pop('parent_site')
            inst_models = config.pop('instrument_models')
            parent_site_id = self._retrieve_ooi_asset(parent_site_alt_id)['_id']
            site = IonObject('InstrumentSite', config)
            site_id = self.oms.create_instrument_site(instrument_site=site,
                                        parent_id=parent_site_id)
            for model in inst_models:
                model_id = self._retrieve_ooi_asset(model)['_id']
                self.oms.assign_instrument_model_to_instrument_site(model_id,
                                                                    site_id)
            self.oms.assign_resource_to_observatory_org(site_id,
                                                        self._orgs[org]['_id'])


        # Create and link platform devices.
        for config in AUGMENT_PLATFORM_DEVICES:
            org = config.pop('org')
            platform_model_alt_id = config.pop('platform_model')
            parent_device_alt_id = config.pop('parent_device')
            network_parent_alt_id = config.pop('network_parent')
            device = IonObject('PlatformDevice', config)
            device_id = self.ims.create_platform_device(device)
            model_id = self._retrieve_ooi_asset(platform_model_alt_id)['_id']
            self.ims.assign_platform_model_to_platform_device(model_id,
                                                              device_id)
            parent_id = self._retrieve_ooi_asset(parent_device_alt_id)['_id']
            network_parent_id = self._retrieve_ooi_asset(
                                                network_parent_alt_id)['_id']
            self.ims.assign_platform_device_to_platform_device(device_id,
                                                               parent_id)
            self.oms.assign_device_to_network_parent(device_id,
                                                     network_parent_id)
            self.container.resource_registry.set_lifecycle_state(device_id,
                                                                 LCS.DEPLOYED)
            self.container.resource_registry.set_lifecycle_state(device_id,
                                                                 AS.AVAILABLE)
            self.oms.assign_resource_to_observatory_org(device_id,
                                                        self._orgs[org]['_id'])

        # Create and link instrument devices.
        for config in AUGMENT_INSTRUMENT_DEVICES:
            org = config.pop('org')
            inst_model_alt_id = config.pop('instrument_model')
            platform_device_alt_id = config.pop('platform_device')
            site_alt_id = config.pop('site')
            device = IonObject('InstrumentDevice', config)
            device_id = self.ims.create_instrument_device(device)
            model_id = self._retrieve_ooi_asset(inst_model_alt_id)['_id']
            self.ims.assign_instrument_model_to_instrument_device(model_id,
                                                                  device_id)
            platform_device_id = self._retrieve_ooi_asset(
                                                platform_device_alt_id)['_id']
            self.ims.assign_instrument_device_to_platform_device(device_id,
                                                            platform_device_id)
            site_id = self._retrieve_ooi_asset(site_alt_id)['_id']
            self.oms.assign_device_to_site(device_id, site_id)

            self.container.resource_registry.set_lifecycle_state(device_id,
                                                                 LCS.DEPLOYED)
            self.container.resource_registry.set_lifecycle_state(device_id,
                                                                 AS.AVAILABLE)
            self.oms.assign_resource_to_observatory_org(device_id,
                                                        self._orgs[org]['_id'])

        for config in AUGMENT_PLATFORM_AGENTS:
            org = config.pop('org')
            models = config.pop('models')

            platform_agent = IonObject('PlatformAgent', config)
            platform_agent_id = self.ims.create_platform_agent(platform_agent)
            for model in models:
                model_id = self._retrieve_ooi_asset(model)['_id']
                self.ims.assign_platform_model_to_platform_agent(model_id,
                                                        platform_agent_id)
            self.container.resource_registry.set_lifecycle_state(platform_agent_id,
                                                                 LCS.DEPLOYED)
            self.container.resource_registry.set_lifecycle_state(platform_agent_id,
                                                                 AS.AVAILABLE)
            self.oms.assign_resource_to_observatory_org(platform_agent_id,
                                                        self._orgs[org]['_id'])

        for config in AUGMENT_PLATFORM_AGENT_INSTANCES:
            org = config.pop('org')
            agent_alt_id = config.pop('agent')
            device_alt_id = config.pop('device')
            instance = IonObject('PlatformAgentInstance', config)
            instance_id = self.ims.create_platform_agent_instance(instance)
            agent_id = self._retrieve_ooi_asset(agent_alt_id)['_id']
            device_id = self._retrieve_ooi_asset(device_alt_id)['_id']
            self.ims.assign_platform_agent_to_platform_agent_instance(
                agent_id, instance_id)
            self.ims.assign_platform_agent_instance_to_platform_device(
                instance_id, device_id)
            self.oms.assign_resource_to_observatory_org(instance_id,
                                                        self._orgs[org]['_id'])

        for config in AUGMENT_INSTRUMENT_AGENTS:
            org = config.pop('org')
            models = config.pop('models')
            instrument_agent = IonObject('InstrumentAgent', config)
            instrument_agent_id = self.ims.create_instrument_agent(instrument_agent)
            for model in models:
                model_id = self._retrieve_ooi_asset(model)['_id']
                self.ims.assign_instrument_model_to_instrument_agent(model_id,
                                                        instrument_agent_id)
            self.container.resource_registry.set_lifecycle_state(instrument_agent_id,
                                                                 LCS.DEPLOYED)
            self.container.resource_registry.set_lifecycle_state(instrument_agent_id,
                                                                 AS.AVAILABLE)
            self.oms.assign_resource_to_observatory_org(instrument_agent_id,
                                                        self._orgs[org]['_id'])

        for config in AUGMENT_INSTRUMENT_AGENT_INSTANCES:
            org = config.pop('org')
            agent_alt_id = config.pop('agent')
            device_alt_id = config.pop('device')
            instance = IonObject('InstrumentAgentInstance', config)
            instance_id = self.ims.create_instrument_agent_instance(instance)
            agent_id = self._retrieve_ooi_asset(agent_alt_id)['_id']
            device_id = self._retrieve_ooi_asset(device_alt_id)['_id']
            self.ims.assign_instrument_agent_to_instrument_agent_instance(
                agent_id, instance_id)
            self.ims.assign_instrument_agent_instance_to_instrument_device(
                instance_id, device_id)
            self.oms.assign_resource_to_observatory_org(instance_id,
                                                        self._orgs[org]['_id'])

        for config in AUGMENT_DATASET_AGENTS:
            pass

        for config in AUGMENT_DATASET_AGENT_INSTANCES:
            pass

        # Create, link and activate platform deployments.
        # Note: No AV setting for deployments.
        # BadRequest: 400 - Target state DEPLOYED_AVAILABLE not reachable
        # for resource in state DEPLOYED_AVAILABLE
        for config in AUGMENT_PLATFORM_DEPLOYMENTS:
            org = config.pop('org')
            platform_site = config.pop('platform_site')
            platform_device = config.pop('platform_device')
            site_id = self._retrieve_ooi_asset(platform_site)['_id']
            device_id = self._retrieve_ooi_asset(platform_device)['_id']
            deployment = IonObject('Deployment', config)
            deployment_id = self.oms.create_deployment(deployment)
            self.oms.deploy_platform_site(site_id, deployment_id)
            self.ims.deploy_platform_device(device_id, deployment_id)
            self.container.resource_registry.set_lifecycle_state(deployment_id,
                                                                 LCS.DEPLOYED)
            self.oms.activate_deployment(deployment_id)
            self.oms.assign_resource_to_observatory_org(deployment_id,
                                                        self._orgs[org]['_id'])

        # Create, link and activate instrument deployments.
        # Note: No AV setting for deployments.
        # BadRequest: 400 - Target state DEPLOYED_AVAILABLE not reachable
        # for resource in state DEPLOYED_AVAILABLE
        for config in AUGMENT_INSTRUMENT_DEPLOYMENTS:
            org = config.pop('org')
            instrument_site = config.pop('instrument_site')
            instrument_device = config.pop('instrument_device')
            site_id = self._retrieve_ooi_asset(instrument_site)['_id']
            device_id = self._retrieve_ooi_asset(instrument_device)['_id']
            deployment = IonObject('Deployment', config)
            deployment_id = self.oms.create_deployment(deployment)
            self.oms.deploy_instrument_site(site_id, deployment_id)
            self.ims.deploy_instrument_device(device_id, deployment_id)
            self.container.resource_registry.set_lifecycle_state(deployment_id,
                                                                 LCS.DEPLOYED)
            self.oms.activate_deployment(deployment_id)
            self.oms.assign_resource_to_observatory_org(deployment_id,
                                                        self._orgs[org]['_id'])


    def test_device_activation(self):
        """
        A master test incrementally tests all distinct system configurations.
        """

        # Perform RSN instrument activation test cycle.
        self._cycle_rsn_instrument()

        # Perform CGSN Mooring activation test cycle.
        self._cycle_cgsn_mooring()

        # Perfom glider activation test cycle.
        # TODO
        # Note this is less urgent as the previous two patterns capture
        # much of the goal and the UI will have create its own orchistration
        # pattern guided by user use case requirements.

    def _cycle_cgsn_mooring(self):
        """
        Perform deactivation - activation cycle on a cgsn mooring.
        @return:
        """

        ############################################################
        # Get existing resources to be deactivated.
        ############################################################
        mooring_id = self._retrieve_ooi_asset(CGSN_MOORING_PLATFORM_ALT_ID)['_id']
        mooring = self.container.resource_registry.read(mooring_id)

        riser_id = self._retrieve_ooi_asset(CGSN_RISER_PLATFORM_ALT_ID)['_id']
        riser = self.container.resource_registry.read(riser_id)

        deployment_id = self._retrieve_ooi_asset(CGSN_MOORING_DEPLOYMENT_ALT_ID)['_id']
        deployment = self.container.resource_registry.read(deployment_id)

        inst_id = self._retrieve_ooi_asset(CGSN_RISER_INSTRUMENT_ALT_ID)['_id']
        inst = self.container.resource_registry.read(inst_id)

        ############################################################
        # Activate deployment to simulate running state and verify.
        ############################################################
        self.oms.activate_deployment(deployment_id)
        self._verify_cgsn_mooring_deployed(mooring_id, riser_id, 16)

        ############################################################
        # OPERATIONS SOFTWARE STEP: Deactivate deployemnt.
        ############################################################
        self.oms.deactivate_deployment(deployment_id)

        ############################################################
        # OPERATIONS SOFTWARE STEP: Disintegrate assembly and verify.
        ############################################################

        ############################################################
        # Transition platforms to DEVELOPED.
        ############################################################
        self.container.resource_registry.set_lifecycle_state(mooring_id,
                                                             AS.PRIVATE)
        self.container.resource_registry.set_lifecycle_state(mooring_id,
                                                            LCS.DEVELOPED)
        self.container.resource_registry.set_lifecycle_state(riser_id,
                                                             AS.PRIVATE)
        self.container.resource_registry.set_lifecycle_state(riser_id,
                                                            LCS.DEVELOPED)
        ############################################################
        # Verify no running agents on devices.
        # Dissociate running agents from all devices.
        ############################################################
        # TODO
        ############################################################

        ############################################################
        # Transition instruments to DEVELOPED and dissociate from platforms.
        ############################################################
        ret = self.container.resource_registry.find_objects(
            object_type='InstrumentDevice',predicate=PRED.hasDevice,
            subject=riser_id)
        for x in ret[0]:
            self.container.resource_registry.set_lifecycle_state(x['_id'],
                                                                 AS.PRIVATE)
            self.container.resource_registry.set_lifecycle_state(x['_id'],
                                                                 LCS.DEVELOPED)
            self.ims.unassign_instrument_device_from_platform_device(x['_id'],
                                                                riser_id)

        ############################################################
        # Verify deactivation and dis-integration.
        ############################################################
        self._verify_cgsn_mooring_deactivated(mooring_id, riser_id)

        ############################################################
        # OPERATIONS SOFTWARE STEP: Develop new assembly.
        ############################################################
        org_id = self._retrieve_ooi_asset(CGSN_FACILITY_ALT_ID)['_id']

        ############################################################
        # Create and register mooring platform device, assign model.
        ############################################################
        mooring_2 = IonObject('PlatformDevice', CGSN_MOORING_PLATFORM_2)
        mooring_2_id = self.ims.create_platform_device(mooring_2)
        mooring_model_id = self._retrieve_ooi_asset(CGSN_MOORING_MODEL_ALT_ID)['_id']
        self.ims.assign_platform_model_to_platform_device(mooring_model_id,mooring_2_id)
        self.container.resource_registry.set_lifecycle_state(mooring_2_id,
                                                             LCS.DEVELOPED)
        self.oms.assign_resource_to_observatory_org(mooring_2_id,org_id)

        ############################################################
        # Create and register riser platform device, assign model,
        # associate to mooring device.
        ############################################################
        riser_2 = IonObject('PlatformDevice', CGSN_RISER_PLATFORM_2)
        riser_2_id = self.ims.create_platform_device(riser_2)
        riser_model_id = self._retrieve_ooi_asset(CGSN_RISER_MODEL_ALT_ID)['_id']
        self.ims.assign_platform_model_to_platform_device(riser_model_id,
                                                          riser_2_id)
        self.ims.assign_platform_device_to_platform_device(riser_2_id,
                                                        mooring_2_id)
        self.container.resource_registry.set_lifecycle_state(riser_2_id,
                                                             LCS.DEVELOPED)
        self.oms.assign_resource_to_observatory_org(riser_2_id,org_id)

        ############################################################
        # Create and register instruments, associate models,
        # associate to riser device, transition to DEVELOPED.
        ############################################################
        inst_model_id = self._retrieve_ooi_asset(CGSN_INSTRUMENT_MODEL_ALT_ID)['_id']
        inst_ids = []
        for x in CGSN_INSTRUMENTS_2:
            inst_2 = IonObject('InstrumentDevice', x)
            inst_2_id = self.ims.create_instrument_device(inst_2)
            self.ims.assign_instrument_model_to_instrument_device(inst_model_id, inst_2_id)
            self.ims.assign_instrument_model_to_instrument_device(inst_model_id,
                                                                  inst_2_id)
            self.ims.assign_instrument_device_to_platform_device(inst_2_id,
                                                                 riser_2_id)
            self.container.resource_registry.set_lifecycle_state(inst_2_id,
                                                             LCS.DEVELOPED)
            self.oms.assign_resource_to_observatory_org(inst_2_id,org_id)
            inst_ids.append(inst_2_id)

        ############################################################
        # Verify developed assembly.
        ############################################################
        self._verify_cgsn_mooring_developed(mooring_2_id, riser_2_id)

        ############################################################
        # OPERATIONS SOFTWARE STEP: Integrate new assembly.
        ############################################################

        ############################################################
        # Create and register new deployment.
        ############################################################
        deployment_2 = IonObject('Deployment', CGSN_DEPLOYMENT_2)
        deployment_2_id = self.oms.create_deployment(deployment_2)
        self.oms.assign_resource_to_observatory_org(deployment_2_id,org_id)

        ############################################################
        # Assign mooring device and site to deployment.
        ############################################################
        mooring_site_id = self._retrieve_ooi_asset(CGSN_MOORING_SITE_ALT_ID)['_id']
        self.oms.deploy_platform_site(mooring_site_id,deployment_2_id)
        self.ims.deploy_platform_device(mooring_2_id, deployment_2_id)
        #self.oms.assign_device_to_deployment(mooring_2_id, deployment_2_id)
        #self.oms.assign_site_to_deployment(mooring_site_id,deployment_2_id)
        #self.oms.assign_device_to_site(mooring_2_id, mooring_site_id)

        ############################################################
        # Transition all devices.
        ############################################################
        self.container.resource_registry.set_lifecycle_state(mooring_2_id,
                                                        LCS.INTEGRATED)
        self.container.resource_registry.set_lifecycle_state(riser_2_id,
                                                        LCS.INTEGRATED)
        for x in inst_ids:
            self.container.resource_registry.set_lifecycle_state(x,
                                                        LCS.INTEGRATED)

        ############################################################
        # Verify integrated assembly.
        ############################################################
        self._verify_cgsn_mooring_integrated(mooring_2_id, riser_2_id)

        ############################################################
        # OPERATIONS SOFTWARE STEP: Activate new assembly.
        ############################################################

        ############################################################
        # Transition all devices.
        ############################################################
        self.container.resource_registry.set_lifecycle_state(mooring_2_id,
                                                        LCS.DEPLOYED)
        self.container.resource_registry.set_lifecycle_state(mooring_2_id,
                                                        AS.AVAILABLE)
        self.container.resource_registry.set_lifecycle_state(riser_2_id,
                                                        LCS.DEPLOYED)
        self.container.resource_registry.set_lifecycle_state(riser_2_id,
                                                        AS.AVAILABLE)
        for x in inst_ids:
            self.container.resource_registry.set_lifecycle_state(x,
                                                        LCS.DEPLOYED)
            self.container.resource_registry.set_lifecycle_state(x,
                                                        AS.AVAILABLE)
        ############################################################
        # Activate deployment.
        ############################################################
        self.oms.activate_deployment(deployment_2_id)

        ############################################################
        # Verify activated assembly.
        ############################################################
        self._verify_cgsn_mooring_deployed(mooring_2_id, riser_2_id, 3)


    def _verify_cgsn_mooring_deployed(self, mooring_dev_id, riser_dev_id, no_insts):
        """
        Verify csgn assembly in deployed state.
        @param mooring_id: Resource ID of the subsurface mooring.
        @param riser_id: Resource ID of the mooring riser.
        @return:
        """
        # Read objects.
        mooring_obj = self.container.resource_registry.read(mooring_dev_id)
        riser_obj = self.container.resource_registry.read(riser_dev_id)

        ########################################################
        # Verify mooring device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(mooring_obj['lcstate'], LCS.DEPLOYED)
        self.assertEqual(mooring_obj['availability'], AS.AVAILABLE)

        # This PlatformDevice has one DataProducer (hasDataProducer).
        ret = self.container.resource_registry.find_objects(
            object_type='DataProducer',predicate=PRED.hasDataProducer,
            subject=mooring_dev_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one Deployment (hasDeployment).
        ret = self.container.resource_registry.find_objects(
            object_type='Deployment',predicate=PRED.hasDeployment,
            subject=mooring_dev_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformDevice',predicate=PRED.hasDevice,
            subject=mooring_dev_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one PlatformModel (hasModel).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformModel',predicate=PRED.hasModel,
            subject=mooring_dev_id)
        self.assertEqual(len(ret[0]),1)

        # One org has this PlatformDevice (hasResource).
        ret = self.container.resource_registry.find_subjects(
            subject_type='Org',predicate=PRED.hasResource,
            object=mooring_dev_id)
        self.assertEqual(len(ret[0]),1)

        # One PlatformSite has this PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformSite',predicate=PRED.hasDevice,
            object=mooring_dev_id)
        self.assertEqual(len(ret[0]),1)

        ########################################################
        # Verify riser device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(riser_obj['lcstate'], LCS.DEPLOYED)
        self.assertEqual(riser_obj['availability'], AS.AVAILABLE)

        # This PlatformDevice has one DataProducer (hasDataProducer).
        ret = self.container.resource_registry.find_objects(
            object_type='DataProducer',predicate=PRED.hasDataProducer,
            subject=riser_dev_id)
        self.assertEqual(len(ret[0]),1)

        # No deployment
        ret = self.container.resource_registry.find_objects(
            object_type='Deployment',predicate=PRED.hasDeployment,
            subject=riser_dev_id)
        self.assertEqual(len(ret[0]),0)

        # No platform device.
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformDevice',predicate=PRED.hasDevice,
            subject=riser_dev_id)
        self.assertEqual(len(ret[0]),0)

        # This PlatformDevice has one PlatformModel (hasModel).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformModel',predicate=PRED.hasModel,
            subject=riser_dev_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has 16 InstrumentDevicse (hasDevice).
        ret = self.container.resource_registry.find_objects(
            object_type='InstrumentDevice',predicate=PRED.hasDevice,
            subject=riser_dev_id)
        self.assertEqual(len(ret[0]),no_insts)

        # One org has this PlatformDevice (hasResource).
        ret = self.container.resource_registry.find_subjects(
            subject_type='Org',predicate=PRED.hasResource,
            object=riser_dev_id)
        self.assertEqual(len(ret[0]),1)

        # One PlatformSite has this PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformSite',predicate=PRED.hasDevice,
            object=riser_dev_id)
        self.assertEqual(len(ret[0]),1)

        # One PlatformDevice has this PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice',predicate=PRED.hasDevice,
            object=riser_dev_id)
        self.assertEqual(len(ret[0]),1)


    def _verify_cgsn_mooring_deactivated(self, mooring_id, riser_id):
        """
        Verify csgn assembly in deactivated state.
        @param mooring_id: Resource ID of the subsurface mooring.
        @param riser_id: Resource ID of the mooring riser.
        @return:
        """
        # Read objects.
        mooring_obj = self.container.resource_registry.read(mooring_id)
        riser_obj = self.container.resource_registry.read(riser_id)

        ########################################################
        # Verify mooring device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(mooring_obj['lcstate'], LCS.DEVELOPED)
        self.assertEqual(mooring_obj['availability'], AS.PRIVATE)

        # This PlatformDevice has one DataProducer (hasDataProducer).
        ret = self.container.resource_registry.find_objects(
            object_type='DataProducer',predicate=PRED.hasDataProducer,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one Deployment (hasDeployment).
        ret = self.container.resource_registry.find_objects(
            object_type='Deployment',predicate=PRED.hasDeployment,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformDevice',predicate=PRED.hasDevice,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one PlatformModel (hasModel).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformModel',predicate=PRED.hasModel,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # One org has this PlatformDevice (hasResource).
        ret = self.container.resource_registry.find_subjects(
            subject_type='Org',predicate=PRED.hasResource,
            object=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # One PlatformSite has this PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformSite',predicate=PRED.hasDevice,
            object=mooring_id)
        self.assertEqual(len(ret[0]),0)

        ########################################################
        # Verify riser device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(riser_obj['lcstate'], LCS.DEVELOPED)
        self.assertEqual(riser_obj['availability'], AS.PRIVATE)

        # This PlatformDevice has one DataProducer (hasDataProducer).
        ret = self.container.resource_registry.find_objects(
            object_type='DataProducer',predicate=PRED.hasDataProducer,
            subject=riser_id)
        self.assertEqual(len(ret[0]),1)

        # No deployment
        ret = self.container.resource_registry.find_objects(
            object_type='Deployment',predicate=PRED.hasDeployment,
            subject=riser_id)
        self.assertEqual(len(ret[0]),0)

        # No platform device.
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformDevice',predicate=PRED.hasDevice,
            subject=riser_id)
        self.assertEqual(len(ret[0]),0)

        # This PlatformDevice has one PlatformModel (hasModel).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformModel',predicate=PRED.hasModel,
            subject=riser_id)
        self.assertEqual(len(ret[0]),1)

        # No more associated instruments.
        ret = self.container.resource_registry.find_objects(
            object_type='InstrumentDevice',predicate=PRED.hasDevice,
            subject=riser_id)
        self.assertEqual(len(ret[0]),0)

        # One org has this PlatformDevice (hasResource).
        ret = self.container.resource_registry.find_subjects(
            subject_type='Org',predicate=PRED.hasResource,
            object=riser_id)
        self.assertEqual(len(ret[0]),1)

        # No platform site.
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformSite',predicate=PRED.hasDevice,
            object=riser_id)
        self.assertEqual(len(ret[0]),0)

        # One PlatformDevice has this PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice',predicate=PRED.hasDevice,
            object=riser_id)
        self.assertEqual(len(ret[0]),1)


    def _verify_cgsn_mooring_developed(self, mooring_id, riser_id):
        """
        Verify csgn assembly in developed state.
        @param mooring_id: Resource ID of the subsurface mooring.
        @param riser_id: Resource ID of the mooring riser.
        @return:
        """
        # Read objects.
        mooring_obj = self.container.resource_registry.read(mooring_id)
        riser_obj = self.container.resource_registry.read(riser_id)

        ########################################################
        # Verify mooring device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(mooring_obj['lcstate'], LCS.DEVELOPED)
        self.assertEqual(mooring_obj['availability'], AS.PRIVATE)

        # This PlatformDevice has one DataProducer (hasDataProducer).
        ret = self.container.resource_registry.find_objects(
            object_type='DataProducer',predicate=PRED.hasDataProducer,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has no Deployment (hasDeployment).
        ret = self.container.resource_registry.find_objects(
            object_type='Deployment',predicate=PRED.hasDeployment,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),0)

        # This PlatformDevice has one PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformDevice',predicate=PRED.hasDevice,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one PlatformModel (hasModel).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformModel',predicate=PRED.hasModel,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # One org has this PlatformDevice (hasResource).
        ret = self.container.resource_registry.find_subjects(
            subject_type='Org',predicate=PRED.hasResource,
            object=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # No site until activated.
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformSite',predicate=PRED.hasDevice,
            object=mooring_id)
        self.assertEqual(len(ret[0]),0)

        ########################################################
        # Verify riser device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(riser_obj['lcstate'], LCS.DEVELOPED)
        self.assertEqual(riser_obj['availability'], AS.PRIVATE)

        # This PlatformDevice has one DataProducer (hasDataProducer).
        ret = self.container.resource_registry.find_objects(
            object_type='DataProducer',predicate=PRED.hasDataProducer,
            subject=riser_id)
        self.assertEqual(len(ret[0]),1)

        # No deployment
        ret = self.container.resource_registry.find_objects(
            object_type='Deployment',predicate=PRED.hasDeployment,
            subject=riser_id)
        self.assertEqual(len(ret[0]),0)

        # No platform device.
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformDevice',predicate=PRED.hasDevice,
            subject=riser_id)
        self.assertEqual(len(ret[0]),0)

        # This PlatformDevice has one PlatformModel (hasModel).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformModel',predicate=PRED.hasModel,
            subject=riser_id)
        self.assertEqual(len(ret[0]),1)

        # Three instruments.
        ret = self.container.resource_registry.find_objects(
            object_type='InstrumentDevice',predicate=PRED.hasDevice,
            subject=riser_id)
        self.assertEqual(len(ret[0]),3)

        # One org has this PlatformDevice (hasResource).
        ret = self.container.resource_registry.find_subjects(
            subject_type='Org',predicate=PRED.hasResource,
            object=riser_id)
        self.assertEqual(len(ret[0]),1)

        # No platform site.
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformSite',predicate=PRED.hasDevice,
            object=riser_id)
        self.assertEqual(len(ret[0]),0)

        # One PlatformDevice has this PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice',predicate=PRED.hasDevice,
            object=riser_id)
        self.assertEqual(len(ret[0]),1)


    def _verify_cgsn_mooring_integrated(self, mooring_id, riser_id):
        """
        Verify csgn assembly in integrated state.
        @param mooring_id: Resource ID of the subsurface mooring.
        @param riser_id: Resource ID of the mooring riser.
        @return:
        """
        # Read objects.
        mooring_obj = self.container.resource_registry.read(mooring_id)
        riser_obj = self.container.resource_registry.read(riser_id)

        ########################################################
        # Verify mooring device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(mooring_obj['lcstate'], LCS.INTEGRATED)
        self.assertEqual(mooring_obj['availability'], AS.PRIVATE)

        # This PlatformDevice has one DataProducer (hasDataProducer).
        ret = self.container.resource_registry.find_objects(
            object_type='DataProducer',predicate=PRED.hasDataProducer,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has no Deployment (hasDeployment).
        ret = self.container.resource_registry.find_objects(
            object_type='Deployment',predicate=PRED.hasDeployment,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformDevice',predicate=PRED.hasDevice,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # This PlatformDevice has one PlatformModel (hasModel).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformModel',predicate=PRED.hasModel,
            subject=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # One org has this PlatformDevice (hasResource).
        ret = self.container.resource_registry.find_subjects(
            subject_type='Org',predicate=PRED.hasResource,
            object=mooring_id)
        self.assertEqual(len(ret[0]),1)

        # No site until activated.
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformSite',predicate=PRED.hasDevice,
            object=mooring_id)
        self.assertEqual(len(ret[0]),0)

        ########################################################
        # Verify riser device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(riser_obj['lcstate'], LCS.INTEGRATED)
        self.assertEqual(riser_obj['availability'], AS.PRIVATE)

        # This PlatformDevice has one DataProducer (hasDataProducer).
        ret = self.container.resource_registry.find_objects(
            object_type='DataProducer',predicate=PRED.hasDataProducer,
            subject=riser_id)
        self.assertEqual(len(ret[0]),1)

        # No deployment
        ret = self.container.resource_registry.find_objects(
            object_type='Deployment',predicate=PRED.hasDeployment,
            subject=riser_id)
        self.assertEqual(len(ret[0]),0)

        # No platform device.
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformDevice',predicate=PRED.hasDevice,
            subject=riser_id)
        self.assertEqual(len(ret[0]),0)

        # This PlatformDevice has one PlatformModel (hasModel).
        ret = self.container.resource_registry.find_objects(
            object_type='PlatformModel',predicate=PRED.hasModel,
            subject=riser_id)
        self.assertEqual(len(ret[0]),1)

        # Three instruments.
        ret = self.container.resource_registry.find_objects(
            object_type='InstrumentDevice',predicate=PRED.hasDevice,
            subject=riser_id)
        self.assertEqual(len(ret[0]),3)

        # One org has this PlatformDevice (hasResource).
        ret = self.container.resource_registry.find_subjects(
            subject_type='Org',predicate=PRED.hasResource,
            object=riser_id)
        self.assertEqual(len(ret[0]),1)

        # No platform site.
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformSite',predicate=PRED.hasDevice,
            object=riser_id)
        self.assertEqual(len(ret[0]),0)

        # One PlatformDevice has this PlatformDevice (hasDevice).
        ret = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice',predicate=PRED.hasDevice,
            object=riser_id)
        self.assertEqual(len(ret[0]),1)


    def _get_rsn_resources(self):
        """
        Return RSN resources for activation tests.
        @return: device and deployment resource ids.
        """

        ret = self.container.resource_registry.find_resources_ext(
                                            name=RSN_INSTRUMENT_01['name'])
        dev_obj = ret[0][0]
        dev_id = dev_obj['_id']

        # Deployment.
        assoc = self.container.resource_registry.find_associations(
                    subject=dev_id, predicate=PRED.hasDeployment)
        dep_id = assoc[0]['o']

        ainsts = self.container.resource_registry.find_objects(
            object_type='InstrumentAgentInstance',
            predicate=PRED.hasAgentInstance, subject=dev_id)
        ainst_id = None
        agt_id = None
        if len(ainsts[0]):
            ainst_id = ainsts[0][0]['_id']
            agt = self.container.resource_registry.find_objects(
                object_type='InstrumentAgent', predicate=PRED.hasAgentDefinition,
                subject=ainst_id)
            agt_id = agt[0][0]['_id']

        pdevs = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice', predicate=PRED.hasDevice,
            object=dev_id)
        plat_id = None
        if len(pdevs[0]):
            plat_id = pdevs[0][0]['_id']

        mods = self.container.resource_registry.find_objects(
            object_type='InstrumentModel',predicate=PRED.hasModel,
            subject=dev_id)
        mod_id = None
        if len(mods[0]):
            mod_id = mods[0][0]['_id']

        sites = self.container.resource_registry.find_subjects(
            subject_type='InstrumentSite',predicate=PRED.hasModel,
            object=mod_id)
        site_id = None
        if len(sites[0]):
            site_id = sites[0][0]['_id']

        return dev_id, dep_id, ainst_id, plat_id, mod_id, agt_id, site_id


    def _cycle_rsn_instrument(self):
        """
        Perform deactivation - activation cycle on an RSN instruemnt,
        disintegrating an existing instrument to come back to the lab while
        activating and deploying a replacement.
        @return:
        """
        dev_id, dep_id, old_ainst_id, plat_id, mod_id, agt_id, site_id = self._get_rsn_resources()

        self.oms.activate_deployment(dep_id)

        # Verify initial deployed state.
        # Normal running state of the system, e.g. upon preload.
        self._verify_rsn_inst_deployed(dev_id, dep_id)

        # OPERATIONS SOFTWARE STEP: Deactivate deployemnt.
        # A user clicks to deactivate a deployment.
        # Results:
        # 1. Deployment is retired*.
        # 2. Device is transitioned to integrated and is accessible to
        #   operations team for testing and finalization.
        # 3. Site -> Device is removed.
        # *Note: retire functionality in the obejct store is not yet correct,
        # so the deployment is parked in integrated for now.
        self.oms.deactivate_deployment(dep_id)
        self._verify_rsn_inst_deactivated_initial(dev_id, dep_id)

        # OPERATIONS PHYSICAL STEP: Post deployment testing.
        # The deactivated device is surveyed for predisintegration status,
        # e.g. calibration drift is computed, deactivation tests conducted,
        # and so on. Any finalization required at the end of a deployment.
        # Results:
        # 1. Postdeployment test results attached to the device resource.

        # OPERATIONS SOFTWARE STEP: Prepare for disintegration.
        # Shut down running instrument agent if necessary.
        # Power down instrument port.

        # OPERATIONS SOFTWARE STEP: Disintegrate the old device.
        # User clicks to transition the device from integrated to developed
        # so it may be refurbished. This takes the device completely out of
        # operations with the infrastructure.
        # Preconditions:
        # 1. Check instrument agent is not running.
        # 2. Check port power is down.
        # Results:
        # 1. Device LCS/SA goes to DEVELOPED - PRIVATE
        # 2. Agent Instance -> Device link removed.
        # 3. Platform Device -> Device link removed.
        self.container.resource_registry.set_lifecycle_state(dev_id, AS.PRIVATE)
        self.container.resource_registry.set_lifecycle_state(dev_id,
                                                    LCS.DEVELOPED)
        self.ims.unassign_instrument_agent_instance_from_instrument_device(
            old_ainst_id, dev_id)
        self.ims.unassign_instrument_device_from_platform_device(dev_id,
                                                            plat_id)
        self._verify_rsn_inst_deactivated_final(dev_id, dep_id)

        # OPERATIONS PHYSICAL STEP: Disconnect and recovery.
        # The device is subsequently disconnected from the infrastructure and
        # hauled aboard the service vessel for return to the operations lab.
        # Results:
        # 1. Device headed home, no longer in service or operations. May
        # be refurbished for another deployment or retired.

        # OPERATIONS SOFTWARE STEP: Develop new device.
        # Note: Plan and develop can occur anytime prior to integration
        # and deployment. Probably even before the maintenence cruise.
        # The new device is in the developed state and associated with
        # a model and platform.
        # Results:
        # 1. New device object created and registered.
        # 2. New device transitioned to DEVELOPED and PRIVATE.
        # 3. Device -> Model link established.
        # 4. Agent instance created.
        # 5. Device -> Agent instance link established.
        # 6. Agent Instance -> Agent link established.
        new_dev_obj = IonObject('InstrumentDevice', RSN_INSTRUMENT_02)
        new_dev_id = self.ims.create_instrument_device(new_dev_obj)
        self.oms.assign_resource_to_observatory_org(new_dev_id,
                                                    self._orgs['rsn']['_id'])
        self.ims.assign_instrument_model_to_instrument_device(mod_id, new_dev_id)
        self.container.resource_registry.set_lifecycle_state(new_dev_id,
                                                             LCS.DEVELOPED)
        agt_instance = IonObject('InstrumentAgentInstance', RSN_AGENT_02)
        new_agt_inst_id = self.ims.create_instrument_agent_instance(
                                            agt_instance, agt_id, new_dev_id)
        self.oms.assign_resource_to_observatory_org(
                                    new_agt_inst_id, self._orgs['rsn']['_id'])
        self.ims.assign_instrument_agent_to_instrument_agent_instance(
                                                        agt_id, new_agt_inst_id)
        self.ims.assign_instrument_agent_instance_to_instrument_device(
                                                    new_agt_inst_id, new_dev_id)
        self._verify_rsn_inst_developed(new_dev_id)

        # OPERATIONS PHYSICAL STEP: Submerge, connect and integrate new device.
        # The device is physically connected to the infrastructure.

        # OPERATIONS SOFTWARE STEP: Integrate device.
        # Results:
        # 1. Platform -> Device link established.
        # 2. LCS transitioned to INTEGRATED
        self.ims.assign_instrument_device_to_platform_device(new_dev_id,
                                                             plat_id)
        self.container.resource_registry.set_lifecycle_state(new_dev_id,
                                                             LCS.INTEGRATED)
        self._verify_rsn_inst_integrated(new_dev_id)

        # OPERATIONS SOFTWARE STEP: Test integrated device and attach results.
        # Results:
        # 1. Predeployment test results attached to the device resource.

        # OPERATIONS SOFTWARE STEP: Deploy new device.
        # Results:
        # 1. Deployment created.
        # 2. Site -> Deployment established.
        # 3. Device -> Deployment established.
        # 4. Device LCS transitioned to DEPLOYED - AVAILABLE.
        # 5. Deployment activated.
        new_dep = IonObject('Deployment', RSN_INST_DEPLOYMENT_2)
        new_dep_id = self.oms.create_deployment(new_dep)
        self.oms.assign_resource_to_observatory_org(new_dep_id,
                                                    self._orgs['rsn']['_id'])

        self.oms.deploy_instrument_site(site_id, new_dep_id)
        self.ims.deploy_instrument_device(new_dev_id, new_dep_id)
        self.container.resource_registry.set_lifecycle_state(new_dev_id,
                                                             LCS.DEPLOYED)
        self.container.resource_registry.set_lifecycle_state(new_dev_id,
                                                             AS.AVAILABLE)
        self.container.resource_registry.set_lifecycle_state(new_dep_id,
                                                             LCS.DEPLOYED)
        self.oms.activate_deployment(new_dep_id)
        self._verify_rsn_inst_deployed(new_dev_id, new_dep_id)


    def _verify_rsn_inst_deployed(self, dev_id, dep_id):
        """
        Verify RSN instrument in deployed state.
        @param dev_id: Resource ID of the instrument device.
        @param dep_id: Resource ID of the deployment associated with the device.
        @return:
        """

        # Read objects.
        dev_obj = self.container.resource_registry.read(dev_id)
        dep_obj = self.container.resource_registry.read(dep_id)

        ########################################################
        # Verify device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(dev_obj['lcstate'], LCS.DEPLOYED)
        self.assertEqual(dev_obj['availability'], AS.AVAILABLE)

        # This InstrumentDevice has one InstrumentModel (hasModel).
        mods = self.container.resource_registry.find_objects(
            object_type='InstrumentModel',predicate=PRED.hasModel,
            subject=dev_id)
        self.assertEqual(len(mods[0]),1)

        # This InstrumentDevice has one InstrumentAgentInstance (hasAgentInstance).
        ainsts = self.container.resource_registry.find_objects(
            object_type='InstrumentAgentInstance',
            predicate=PRED.hasAgentInstance, subject=dev_id)
        self.assertEqual(len(ainsts[0]),1)

        # This InstrumentDevice has one Deployment (hasDeployment).
        deps = self.container.resource_registry.find_objects(
            object_type='Deployment',
            predicate=PRED.hasDeployment, subject=dev_id)
        self.assertEqual(len(deps[0]),1)

        # This InstrumentDevice has one DataProducer (hasDataProducer).
        dprods = self.container.resource_registry.find_objects(
            object_type='DataProducer', predicate=PRED.hasDataProducer,
            subject=dev_id)
        self.assertEqual(len(dprods[0]),1)

        # One Org has this InstrumentDevice (hasResource).
        orgs = self.container.resource_registry.find_subjects(
            subject_type='Org', predicate=PRED.hasResource, object=dev_id)
        self.assertEqual(len(orgs[0]),1)

        # One InstrumentSite has this InstrumentDevice (hasDevice).
        sites = self.container.resource_registry.find_subjects(
            subject_type='InstrumentSite', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(sites[0]),1)

        # One PlatformDevice has this InstrumentDevice (hasDevice).
        pdevs = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(pdevs[0]),1)

        ########################################################
        # Verify deployment state and associations.
        ########################################################

        # Verify deployment state and visibility.
        self.assertEqual(dep_obj['lcstate'], LCS.DEPLOYED)
        self.assertEqual(dep_obj['availability'], AS.AVAILABLE)

        # One Org has this deployment (hasResource).
        orgs = self.container.resource_registry.find_subjects(
            subject_type='Org', predicate=PRED.hasResource, object=dep_id)
        self.assertEqual(len(orgs[0]),1)

        # One InstrumentDevice has this deployment (hasDeployment).
        devs = self.container.resource_registry.find_subjects(
            subject_type='InstrumentDevice', predicate=PRED.hasDeployment,
            object=dep_id)
        self.assertEqual(len(devs[0]),1)

        # One InstrumentSite has this deployment (hasDeployment).
        sites = self.container.resource_registry.find_subjects(
            subject_type='InstrumentSite', predicate=PRED.hasDeployment,
            object=dep_id)
        self.assertEqual(len(sites[0]),1)


    def _verify_rsn_inst_deactivated_initial(self, dev_id, dep_id):
        """
        Verify RSN instrument with deactivated deployment. This is the
        initial phase of deactivation.
        @param dev_id: Resource ID of the instrument device.
        @param dep_id: Resource ID of the deployment associated with the device.
        @return:
        """
        # Read objects.
        dev_obj = self.container.resource_registry.read(dev_id)
        dep_obj = self.container.resource_registry.read(dep_id)

        ########################################################
        # Verify device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(dev_obj['lcstate'], LCS.INTEGRATED)
        self.assertEqual(dev_obj['availability'], AS.AVAILABLE)

        # This InstrumentDevice has one InstrumentModel (hasModel).
        mods = self.container.resource_registry.find_objects(
            object_type='InstrumentModel',predicate=PRED.hasModel,
            subject=dev_id)
        self.assertEqual(len(mods[0]),1)

        # This InstrumentDevice has one InstrumentAgentInstance (hasAgentInstance).
        ainsts = self.container.resource_registry.find_objects(
            object_type='InstrumentAgentInstance',
            predicate=PRED.hasAgentInstance, subject=dev_id)
        self.assertEqual(len(ainsts[0]),1)

        # This InstrumentDevice has one Deployment (hasDeployment).
        deps = self.container.resource_registry.find_objects(
            object_type='Deployment',
            predicate=PRED.hasDeployment, subject=dev_id)
        self.assertEqual(len(deps[0]),1)

        # This InstrumentDevice has one DataProducer (hasDataProducer).
        dprods = self.container.resource_registry.find_objects(
            object_type='DataProducer', predicate=PRED.hasDataProducer,
            subject=dev_id)
        self.assertEqual(len(dprods[0]),1)

        # One Org has this InstrumentDevice (hasResource).
        orgs = self.container.resource_registry.find_subjects(
            subject_type='Org', predicate=PRED.hasResource, object=dev_id)
        self.assertEqual(len(orgs[0]),1)

        # Change:
        # InstrumentSite has been removed.
        sites = self.container.resource_registry.find_subjects(
            subject_type='InstrumentSite', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(sites[0]),0)

        # One PlatformDevice has this InstrumentDevice (hasDevice).
        pdevs = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(pdevs[0]),1)

        ########################################################
        # Verify deployment state and associations.
        ########################################################

        # Verify deployment state and visibility.
        # NOTE: deployment will be changed to retired with fix to RR and OMS.
        # This is a parking spot until that change.
        self.assertEqual(dep_obj['lcstate'], LCS.INTEGRATED)
        self.assertEqual(dep_obj['availability'], AS.AVAILABLE)

        # One Org has this deployment (hasResource).
        orgs = self.container.resource_registry.find_subjects(
            subject_type='Org', predicate=PRED.hasResource, object=dep_id)
        self.assertEqual(len(orgs[0]),1)

        # One InstrumentDevice has this deployment (hasDeployment).
        devs = self.container.resource_registry.find_subjects(
            subject_type='InstrumentDevice', predicate=PRED.hasDeployment,
            object=dep_id)
        self.assertEqual(len(devs[0]),1)

        # One InstrumentSite has this deployment (hasDeployment).
        sites = self.container.resource_registry.find_subjects(
            subject_type='InstrumentSite', predicate=PRED.hasDeployment,
            object=dep_id)
        self.assertEqual(len(sites[0]),1)


    def _verify_rsn_inst_deactivated_final(self, dev_id, dep_id):
        """
        Verify RSN instrument in final deactivated state.
        @param dev_id: Resource ID of the instrument device.
        """

        # Read objects.
        dev_obj = self.container.resource_registry.read(dev_id)
        dep_obj = self.container.resource_registry.read(dep_id)

        ########################################################
        # Verify device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(dev_obj['lcstate'], LCS.DEVELOPED)
        self.assertEqual(dev_obj['availability'], AS.PRIVATE)

        # This InstrumentDevice has one InstrumentModel (hasModel).
        mods = self.container.resource_registry.find_objects(
            object_type='InstrumentModel',predicate=PRED.hasModel,
            subject=dev_id)
        self.assertEqual(len(mods[0]),1)

        # CHANGE:
        # This InstrumentDevice has one InstrumentAgentInstance (hasAgentInstance).
        ainsts = self.container.resource_registry.find_objects(
            object_type='InstrumentAgentInstance',
            predicate=PRED.hasAgentInstance, subject=dev_id)
        self.assertEqual(len(ainsts[0]),0)

        # This InstrumentDevice has one Deployment (hasDeployment).
        deps = self.container.resource_registry.find_objects(
            object_type='Deployment',
            predicate=PRED.hasDeployment, subject=dev_id)
        self.assertEqual(len(deps[0]),1)

        # This InstrumentDevice has one DataProducer (hasDataProducer).
        dprods = self.container.resource_registry.find_objects(
            object_type='DataProducer', predicate=PRED.hasDataProducer,
            subject=dev_id)
        self.assertEqual(len(dprods[0]),1)

        # One Org has this InstrumentDevice (hasResource).
        orgs = self.container.resource_registry.find_subjects(
            subject_type='Org', predicate=PRED.hasResource, object=dev_id)
        self.assertEqual(len(orgs[0]),1)

        # InstrumentSite has been removed.
        sites = self.container.resource_registry.find_subjects(
            subject_type='InstrumentSite', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(sites[0]),0)

        # CHANGE:
        # One PlatformDevice has this InstrumentDevice (hasDevice).
        pdevs = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(pdevs[0]),0)


    def _verify_rsn_inst_developed(self, dev_id):
        """
        Verify new RSN instrument in developed state.
        @param dev_id:
        @return:
        """

        # Read objects.
        dev_obj = self.container.resource_registry.read(dev_id)

        ########################################################
        # Verify device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(dev_obj['lcstate'], LCS.DEVELOPED)
        self.assertEqual(dev_obj['availability'], AS.PRIVATE)

        # This InstrumentDevice has one InstrumentModel (hasModel).
        mods = self.container.resource_registry.find_objects(
            object_type='InstrumentModel',predicate=PRED.hasModel,
            subject=dev_id)
        self.assertEqual(len(mods[0]),1)

        # This InstrumentDevice has one InstrumentModel (hasModel).
        mods = self.container.resource_registry.find_objects(
            object_type='InstrumentModel',predicate=PRED.hasModel,
            subject=dev_id)
        self.assertEqual(len(mods[0]),1)

        # This InstrumentDevice has one InstrumentAgentInstance (hasAgentInstance).
        ainsts = self.container.resource_registry.find_objects(
            object_type='InstrumentAgentInstance',
            predicate=PRED.hasAgentInstance, subject=dev_id)
        self.assertEqual(len(ainsts[0]),1)

        # This InstrumentDevice has one Deployment (hasDeployment).
        deps = self.container.resource_registry.find_objects(
            object_type='Deployment',
            predicate=PRED.hasDeployment, subject=dev_id)
        self.assertEqual(len(deps[0]),0)

        # This InstrumentDevice has one DataProducer (hasDataProducer).
        dprods = self.container.resource_registry.find_objects(
            object_type='DataProducer', predicate=PRED.hasDataProducer,
            subject=dev_id)
        self.assertEqual(len(dprods[0]),1)

        # One Org has this InstrumentDevice (hasResource).
        orgs = self.container.resource_registry.find_subjects(
            subject_type='Org', predicate=PRED.hasResource, object=dev_id)
        self.assertEqual(len(orgs[0]),1)

        # One InstrumentSite has this InstrumentDevice (hasDevice).
        sites = self.container.resource_registry.find_subjects(
            subject_type='InstrumentSite', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(sites[0]),0)

        # One PlatformDevice has this InstrumentDevice (hasDevice).
        pdevs = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(pdevs[0]),0)


    def _verify_rsn_inst_integrated(self, dev_id):
        """
        Verify new RSN instrument in integrated state.
        @param dev_id:
        @return:
        """
        # Read objects.
        dev_obj = self.container.resource_registry.read(dev_id)

        ########################################################
        # Verify device state and associations.
        ########################################################

        # Verify device state and visibility.
        self.assertEqual(dev_obj['lcstate'], LCS.INTEGRATED)
        self.assertEqual(dev_obj['availability'], AS.PRIVATE)

        # This InstrumentDevice has one InstrumentModel (hasModel).
        mods = self.container.resource_registry.find_objects(
            object_type='InstrumentModel',predicate=PRED.hasModel,
            subject=dev_id)
        self.assertEqual(len(mods[0]),1)

        # This InstrumentDevice has one InstrumentModel (hasModel).
        mods = self.container.resource_registry.find_objects(
            object_type='InstrumentModel',predicate=PRED.hasModel,
            subject=dev_id)
        self.assertEqual(len(mods[0]),1)

        # This InstrumentDevice has one InstrumentAgentInstance (hasAgentInstance).
        ainsts = self.container.resource_registry.find_objects(
            object_type='InstrumentAgentInstance',
            predicate=PRED.hasAgentInstance, subject=dev_id)
        self.assertEqual(len(ainsts[0]),1)

        # This InstrumentDevice has one Deployment (hasDeployment).
        deps = self.container.resource_registry.find_objects(
            object_type='Deployment',
            predicate=PRED.hasDeployment, subject=dev_id)
        self.assertEqual(len(deps[0]),0)

        # This InstrumentDevice has one DataProducer (hasDataProducer).
        dprods = self.container.resource_registry.find_objects(
            object_type='DataProducer', predicate=PRED.hasDataProducer,
            subject=dev_id)
        self.assertEqual(len(dprods[0]),1)

        # One Org has this InstrumentDevice (hasResource).
        orgs = self.container.resource_registry.find_subjects(
            subject_type='Org', predicate=PRED.hasResource, object=dev_id)
        self.assertEqual(len(orgs[0]),1)

        # One InstrumentSite has this InstrumentDevice (hasDevice).
        sites = self.container.resource_registry.find_subjects(
            subject_type='InstrumentSite', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(sites[0]),0)

        # One PlatformDevice has this InstrumentDevice (hasDevice).
        pdevs = self.container.resource_registry.find_subjects(
            subject_type='PlatformDevice', predicate=PRED.hasDevice,
            object=dev_id)
        self.assertEqual(len(pdevs[0]),1)

