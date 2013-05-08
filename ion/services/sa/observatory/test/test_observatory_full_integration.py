#!/usr/bin/env python
import string
import unittest
from ion.util.enhanced_resource_registry_client import EnhancedResourceRegistryClient

from pyon.util.containers import DotDict, get_ion_ts
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.public import RT, PRED, OT
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

    def test_observatory(self):
        # Perform OOI preload for summer deployments (production mode, no debug, no bulk)


        # Check OOI preloaded resources to see if they match needs for this test and for correctness

        # Check lcstates for select OOI resources: Some PLANNED, some INTEGRATED, some DEPLOYED

        # See if primary nodes are already deployed, in particular CE04OSHY-PN01C


        # Check existing RSN node CE04OSHY-LV01C Deployment (PLANNED lcstate)

        # Set CE04OSHY-LV01C Deployment to DEPLOYED state

        # Activate Deployment for CE04OSHY-LV01C

        # (optional) Start CE04OSHY-LV01C platform agent with simulator


        # Check existing CE04OSBP-LJ01C Deployment (PLANNED lcstate)

        # Set CE04OSBP-LJ01C Deployment to DEPLOYED state

        # Activate Deployment for CE04OSBP-LJ01C

        # (optional) Add/register CE04OSBP-LJ01C platform agent to parent agent

        # (optional) Start CE04OSBP-LJ01C platform agent


        # Check existing RSN instrument CE04OSBP-LJ01C-06-CTDBPO108 Deployment (PLANNED lcstate)

        # Set CE04OSBP-LJ01C-06-CTDBPO108 Deployment to DEPLOYED state

        # Activate Deployment for CE04OSBP-LJ01C-06-CTDBPO108 instrument

        # (optional) Add/register CE04OSBP-LJ01C-06-CTDBPO108 instrument agent to parent agent

        # (optional) Start CE04OSBP-LJ01C-06-CTDBPO108 instrument agent with simulator


        # (optional) Create a substitute Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with a comparable device

        # (optional) Activate this second deployment - check first deployment is deactivated

        # (optional) Set first CE04OSBP-LJ01C-06-CTDBPO108 Deployment to INTEGRATED state


        # (optional) Create a third Deployment for site CE04OSBP-LJ01C-06-CTDBPO108 with a same device from first deployment

        # (optional) Activate this third deployment - check second deployment is deactivated


        # Check that glider GP05MOAS-GL001 assembly is defined by OOI preload (3 instruments)

        # Set GP05MOAS-GL001 Deployment to DEPLOYED

        # Activate Deployment for GP05MOAS-GL001

        # Deactivate Deployment for GP05MOAS-GL001


        # Create a Deployment for same glider device same site GP05MOAS-GL001

        # Set second GP05MOAS-GL001 Deployment to DEPLOYED

        # Activate second Deployment for GP05MOAS-GL001

        # Deactivate second Deployment for GP05MOAS-GL001


        # Assemble several CE01ISSM-RI002-* instruments to a CG CE01ISSM-RI002 component platform

        # Assemble CE01ISSM-RI002 platform to CG CE01ISSM-LM001 station platform

        # Set CE01ISSM-LM001 Deployment to DEPLOYED

        # Activate CE01ISSM-LM001 platform assembly deployment


        # Add a new instrument agent

        # Add a new instrument agent instance

        # Check DataProducts

        # Check provenance

        pass
