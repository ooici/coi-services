#!/usr/bin/env python
# coding=utf-8

"""
@package ion.services.sa.test.test_register_and_activate
@file ion/services/sa/test/test_register_and_activate.py
@author Edward Hunter
@brief Integration test cases to confirm registration and activation services
for marine device resources.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon imports
from pyon.public import IonObject, log, RT, PRED, LCS, OT, CFG

# Pyon unittest support.
from pyon.util.int_test import IonIntegrationTestCase

from interface.objects import OrgTypeEnum

# Service clients.
from interface.services.coi.iidentity_management_service \
    import IdentityManagementServiceClient
from interface.services.sa.iobservatory_management_service \
    import ObservatoryManagementServiceClient


"""
--with-queueblame   report leftover queues
--with-pycc         run in seperate container
--with-greenletleak ewpoer leftover greenlets
bin/nosetests -s -v --nologcapture ion/services/sa/test/test_register_and_activate.py:TestRegisterAndActivate
bin/nosetests -s -v --nologcapture ion/services/sa/test/test_register_and_activate.py:TestRegisterAndActivate.test_cabled_device_activation
bin/nosetests -s -v --nologcapture ion/services/sa/test/test_register_and_activate.py:TestRegisterAndActivate.test_uncabled_device_activation
"""

class TestRegisterAndActivate(IonIntegrationTestCase):
    """
    Integration test cases to confirm registration and activation services
    for marine device resources.
    """

    def setUp(self):
        """
        Test setup.
        """

        # Resources used in the tests.
        # General resources.
        self.actor_id = None
        self.user_info_id = None
        self.org_id = None
        self.obs_id = None

        # Cabled infrastructure.
        self.cabled_platform_model_id = None
        self.cabled_platform_site_id = None
        self.cabled_platform_device_id = None
        self.cabled_platform_agent_id = None
        self.cabled_platform_agent_instance_id = None
        self.cabled_platform_deployment_id = None
        self.cabled_instrument_deployment_id = None
        self.cabled_instrument_model_id = None
        self.cabled_instrument_site_id = None
        self.cabled_instrument_device_id = None
        self.cabled_instrument_agent_id = None
        self.cabled_instrument_agent_instance_id = None
        self.cabled_instrument_deployment_id = None

        # Uncabled infrastructure.
        self.uncabled_platform_model_id = None
        self.uncabled_platform_site_id = None
        self.uncabled_platform_device_id = None
        self.uncabled_platform_agent_id = None
        self.uncabled_platform_agent_instance_id = None
        self.uncabled_instrument_model_id = None
        self.uncabled_instrument_site_id = None
        self.uncabled_instrument_device_id = None
        self.uncabled_instrument_agent_id = None
        self.uncabled_instrument_agent_instance_id = None
        self.uncabled_site_deployment_id = None

        # Start container.
        log.info('Staring capability container.')
        self._start_container()

        # Bring up services in a deploy file (no need to message)
        log.info('Staring deploy services.')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Setup service clients.
        self.idms = IdentityManagementServiceClient(node=self.container.node)
        self.oms = ObservatoryManagementServiceClient(node=self.container.node)

        # Add generic resources.
        self._load_system_actors()
        self._create_user()
        self._create_org()
        self._create_observatory()

        # Add cleanup routine.
        self.addCleanup(self._cleanup_resources)


    def _cleanup_resources(self):
        """
        Delete resources created by the tests.
        """

        # Check and clean up cabled resources.
        if self.cabled_instrument_model_id:
            self.oms.unassign_instrument_model_from_instrument_site(
                self.cabled_instrument_model_id, self.cabled_instrument_site_id)
            self.ims.delete_instrument_model(self.cabled_instrument_model_id)
            self.cabled_instrument_model_id = None

        if self.cabled_platform_model_id:
            self.oms.unassign_platform_model_from_platform_site(
                self.cabled_platform_model_id, self.cabled_platform_site_id)
            self.ims.delete_platform_model(self.cabled_platform_model_id)
            self.cabled_platform_model_id = None

        if self.cabled_instrument_site_id:
            self.oms.unassign_site_from_site(self.cabled_instrument_site_id,
                                             self.cabled_platform_site_id)
            self.oms.delete_instrument_site(self.cabled_instrument_site_id)
            self.cabled_instrument_site_id = None

        if self.cabled_platform_site_id:
            self.oms.unassign_site_from_site(self.cabled_platform_site_id,
                                             self.obs_id)
            self.oms.delete_platform_site(self.cabled_platform_site_id)
            self.cabled_platform_site_id = None

        # Check and clean up unclabled resources.
        # TODO

        # Clean up generic resources.
        if self.user_info_id:
            self.idms.delete_user_info(self.user_info_id)
            self.user_info_id = None

        if self.actor_id:
            self.idms.delete_actor_identity(self.actor_id)
            self.actor_id = None

        if self.obs_id:
            self.oms.delete_observatory(self.obs_id)
            self.obs_id = None

        if self.org_id:
            self.container.resource_registry.delete(self.org_id)
            self.org_id = None


    def _load_system_actors(self):
        """
        Retrieve system and webauth actors and headers for later use.
        """

        # Retrieve and store system actor and headers.
        system_actor, _ = self.container.resource_registry.find_resources(
            RT.ActorIdentity,
            name=CFG.system.system_actor,
            id_only=False)
        self.system_actor = system_actor[0] if system_actor else None
        self.system_actor_id = system_actor[0]._id if system_actor \
            else 'anonymous'
        self.system_actor_headers = {
            'ion-actor-id': self.system_actor_id,
            'ion-actor-roles': {'ION': ['ION_MANAGER', 'ORG_MANAGER']},
            'expiry':'0'
        }

        # Retrieve and store webauth actor and headers.
        webauth_actor, _ = self.container.resource_registry.find_resources(
            RT.ActorIdentity,
            name=CFG.get_safe("system.web_authentication_actor",
                              "web_authentication"), id_only=False)
        self.webauth_actor = webauth_actor[0] if webauth_actor else None
        self.webauth_actor_id = webauth_actor[0]._id if webauth_actor \
            else 'anonymous'
        self.webauth_actor_headers = {
            'ion-actor-id': self.webauth_actor_id,
            'ion-actor-roles': {'ION': ['ION_MANAGER', 'ORG_MANAGER']},
            'expiry':'0'
        }


    def _create_user(self):
        """
        Create user resources that serve as device owners.
        This test user does not have contact information,
        user credentials or notification preferences.
        Results in these objects:
        ActorIdentity({'_rev': '1',
            '_id': '07f92986b34e426bba0fca00b73cf4a5',
            'lcstate': 'DEPLOYED',
            'alt_ids': [],
            'description': '',
            'ts_updated': '1391542388312',
            'actor_type': 1,
            'addl': {},
            'ts_created': '1391542388312',
            'availability': 'AVAILABLE',
            'name': 'Identity for Adam Activationtest'})
        UserInfo({'_rev': '1',
            '_id': 'ac8d368e6ea247d996fd60dd0f9c7f89',
            'lcstate': 'DEPLOYED',
            'alt_ids': [],
            'description': 'Activation Test User',
            'tokens': [],
            'ts_updated': '1391542388345',
            'contact': ContactInformation({'individual_names_given': '',
                'city': '', 'roles': [], 'administrative_area': '', 'url': '',
                'country': '', 'variables': [{'name': '', 'value': ''}],
                'organization_name': '', 'postal_code': '',
                'individual_name_family': '', 'phones': [], 'position_name': '',
                'email': '', 'street_address': ''}),
            'variables': [{'name': '', 'value': ''}],
            'addl': {},
            'ts_created': '1391542388345',
            'availability': 'AVAILABLE',
            'name': 'Adam Activationtest'})
        """

        # Basic user attributes for test user.
        user_attrs = {
            'name' : 'Adam Activationtest',
            'description' : 'Activation Test User'
        }

        # Create ActorIdentity.
        actor_name = "Identity for %s" % user_attrs['name']
        actor_identity_obj = IonObject("ActorIdentity", name=actor_name)
        log.trace("creating user %s with headers: %r", user_attrs['name'],
                  self.webauth_actor_headers)
        self.actor_id = self.idms.create_actor_identity(actor_identity_obj,
                                            headers=self.webauth_actor_headers)

        # Create UserInfo.
        user_info_obj = IonObject("UserInfo", **user_attrs)
        self.user_info_id = self.idms.create_user_info(self.actor_id,
                            user_info_obj,headers=self.webauth_actor_headers)


    def _create_org(self):
        """
        Create an org that contains all test infrastructure.
        Results in this object:
        Org({'message_controllable': True,
            '_rev': '1',
            '_id': '9ff82d9f6c7b41f886c6137f54a3086c',
            'lcstate': 'DEPLOYED',
            'alt_ids': [],
            'url': '',
            'description':
            'An Org for Activation Tests',
            'contacts': [],
            'org_governance_name': 'ActiveOrg',
            'institution': Institution({'website': '', 'phone': '',
                'name': '', 'email': ''}),
            'ts_updated': '1391542388395',
            'monitorable': True,
            'org_type': 2,
            'addl': {},
            'ts_created': '1391542388395',
            'availability': 'AVAILABLE',
            'name': 'ActiveOrg'})
        """

        org_attrs = {
            'name' : 'ActiveOrg',
            'description' : 'An Org for Activation Tests',
            'org_type' : OrgTypeEnum.MARINE_FACILITY
        }
        org_obj = IonObject('Org', **org_attrs)
        self.org_id = self.oms.create_marine_facility(org_obj,
                                headers=self.system_actor_headers)


    def _create_observatory(self):
        """
        Create a top level obsevaotry site for the tests.
        Results in this object:
        Observatory({'reference_designator': '',
            'spatial_area_name': '',
            '_id': 'fdcda51901464575913858f98aaf0f41',
            '_rev': '1',
            'lcstate': 'DEPLOYED',
            'alt_ids': [],
            'url': '',
            'description': 'An Observatory for Activation Tests',
            'coordinate_reference_system': GeospatialCoordinateReferenceSystem(
                {'geospatial_latitude_units': '',
                'geospatial_vertical_crs': '',
                'geospatial_geodetic_crs': '',
                'geospatial_vertical_positive': '',
                'geospatial_vertical_units': '',
                'geospatial_longitude_units': ''}),
            'constraint_list': [],
            'environment': 1,
            'ts_updated': '1391544601340',
            'local_name': '',
            'geospatial_point_center': GeospatialIndex({'lat': 0.0, 'lon': 0.0}),
            'addl': {},
            'ts_created': '1391544601340',
            'availability': 'AVAILABLE',
            'name': 'ActiveObservatory'})
        """
        obs_attrs = {
            'name': 'ActiveObservatory',
            'description' : 'An Observatory for Activation Tests'
        }
        obs_obj = IonObject('Observatory', **obs_attrs)
        self.obs_id = self.oms.create_observatory(obs_obj, self.org_id)


    def _create_cabled_resources(self):
        """
        Create preexisting infrastructure for the cabled test environment:
        sites, deployments, models. These are resources that already
        exist in the system due to preload or incremental preload updates.
        PlatformModel
        InstrumentModel
        PlatformSite
        InstrumentSite
        """
        platform_model_attrs = {
            'name' : 'LP Jbox',
            'description' : 'Node Type: LJ',
            'manufacturer' : 'University of Washington',
            'platform_type' : 'Cable Node',
            'platform_family' : 'Low Power JBox',
            'ci_onboard' : False,
            'shore_networked' : True
        }

        instrument_model_attrs = {
            'name': 'Diffuse Vent Fluid 3-D Temperature Array (TMPSF-A)',
            'description': 'Measures temperatures of diffuse flow across the seafloor',
            'reference_designator': 'TMPSFA',
            'class_name': 'Temperature seafloor',
            'mixed_sampling_mode': True,
            'integrated_inductive_modem_available': True,
            'internal_battery': True,
            'addl': {'comments': '', 'connector': '',
                     'makemodel_description': 'XR-420',
                     'input_voltage_range': '',
                     'interface': '',
                     'output_description': '',
                     'class_long_name': 'Temperature_seafloor'},
            'ooi_make_model': 'XR-420',
            'series_name': 'TMPSF Series A',
            'inline_management': True,
            'series_id': 'TMPSFA',
            'subseries_name': 'TMPSF Series A Sub 01',
            'primary_interface': 1,
            'manufacturer': 'RBR Global',
            'family_name': 'Seafloor Properties',
            'class_description': 'Measures temperatures of diffuse flow across the seafloor',
            'class_alternate_name': 'Diffuse Vent Fluid 3-D Temperature Array',
            'subseries_id': 'TMPSFA01',
            'class_id': 'TMPSF',
            'family_id': 'SFL',
            'has_clock': True
        }

        platform_site_attrs = {
            'name' : 'Cabled LP JBOX Platform Site',
            'description' : 'Test Site for a Cabled LP JBOX Platform'
        }

        instrument_site_attrs = {
            'name' : 'Cabled TMPSF Instrument Site',
            'description' : 'Test Site for a Cabled TMPSF Instrument'
        }

        # Create the cabled model preloaded resources.
        platform_model = IonObject('PlatformModel', **platform_model_attrs)
        self.cabled_platform_model_id = self.ims.create_platform_model(platform_model)
        instrument_model = IonObject('InstrumentModel', **instrument_model_attrs)
        self.cabled_instrument_model_id = self.ims.create_instrument_model(
            instrument_model)

        # Create the cabled sites and link them appropriately.
        platform_site = IonObject('', **platform_site_attrs)
        self.cabled_platform_site_id = self.oms.create_platform_site(platform_site)
        self.oms.assign_site_to_site(self.cabled_platform_site_id, self.obs_id)
        instrument_site = IonObject('', **instrument_site_attrs)
        self.cabled_instrument_site_id = self.oms.create_instrument_site(instrument_site)
        self.oms.assign_site_to_site(self.cabled_instrument_site_id, self.cabled_platform_site_id)

        # Assign models to available sites.
        self.oms.assign_platform_model_to_platform_site(
            self.cabled_platform_model_id, self.cabled_platform_site_id)
        self.oms.assign_instrument_model_to_instrument_site(
            self.cabled_instrument_model_id, self.cabled_instrument_site_id)


    def _create_uncabled_resources(self):
        """
        Create preexisting infrastructure for the uncabled test environment:
        sites, deployments, models.
        """
        pass


    def test_cabled_device_activation(self):
        """
        Test registration and activation of cabled device infrastructure.
        """
        pass


    def test_uncabled_device_activation(self):
        """
        Test registration and activation of uncabled device infrastructure.
        """
        pass



