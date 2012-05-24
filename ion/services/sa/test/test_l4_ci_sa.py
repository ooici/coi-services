#from interface.services.icontainer_agent import ContainerAgentClient
#from pyon.ion.endpoint import ProcessRPCClient
from pyon.public import Container, IonObject
#from pyon.util.log import log
from pyon.util.containers import DotDict
from pyon.util.int_test import IonIntegrationTestCase

from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceClient
from interface.services.sa.iobservatory_management_service import ObservatoryManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

from pyon.core.exception import BadRequest, NotFound, Conflict, Inconsistent
from pyon.public import RT, LCS, PRED
from nose.plugins.attrib import attr
import unittest

from ion.services.sa.test.helpers import any_old
from ion.services.sa.observatory.instrument_site_impl import InstrumentSiteImpl
from ion.services.sa.observatory.platform_site_impl import PlatformSiteImpl
from ion.services.sa.instrument.platform_agent_impl import PlatformAgentImpl
from ion.services.sa.instrument.instrument_device_impl import InstrumentDeviceImpl
from ion.services.sa.instrument.sensor_device_impl import SensorDeviceImpl

# some stuff for logging info to the console
import sys
log = DotDict()
printout = sys.stderr.write
printout = lambda x: None

log.debug = lambda x: printout("DEBUG: %s\n" % x)
log.info = lambda x: printout("INFO: %s\n" % x)
log.warn = lambda x: printout("WARNING: %s\n" % x)



@attr('INT', group='sa')
class TestAssembly(IonIntegrationTestCase):
    """
    assembly integration tests at the service level
    """

    def setUp(self):
        # Start container
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Now create client to DataProductManagementService
        self.client = DotDict()
        self.client.DAMS = DataAcquisitionManagementServiceClient(node=self.container.node)
        self.client.DPMS = DataProductManagementServiceClient(node=self.container.node)
        self.client.IMS  = InstrumentManagementServiceClient(node=self.container.node)
        self.client.OMS = ObservatoryManagementServiceClient(node=self.container.node)
        self.client.PSMS = PubsubManagementServiceClient(node=self.container.node)

        self.client.RR   = ResourceRegistryServiceClient(node=self.container.node)

    @unittest.skip('this test just for debugging setup')
    def test_just_the_setup(self):
        return
    

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_221(self):
        """
        Monitoring services to oversee specified physical resource attributes shall be provided. 

        Instrument management shall implement monitoring of instruments and platforms

        Status information provided by the instrument via its agent. This includes state change.

        note from maurice 2012-05-18: This is a request via inst agent for the current 'state' of a running device.  you can only show whatever that agent/driver provides
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_135(self):
        """
        The monitoring services shall publish monitored attributes. 

        Instrument management shall publish monitored attributes

        note from maurice 2012-05-18: agent/drivers will submit events for certain attrs, need to make an event listener for an instrumetn then force it. talk with Bill F. for options.
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_320(self):
        """
        Instrument management shall implement the acquire life cycle activity

        The predecessor to the actual submission of a command.

        note from maurice 2012-05-18: coord with stephen
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_321(self):
        """
        Instrument management shall implement the release life cycle activity

        This releases the instrument to other users.

        note from maurice 2012-05-18: coord with stephen
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_143(self):
        """
        Instrument management shall implement notification of physical resource events

        note from maurice 2012-05-18: These are just capturing events that are published by Bills drivers.  Create a subscription to the event,s move the device into a different state then show the event is published.
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_145(self):
        """
        Instrument management shall implement notification of physical resource state change
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_371(self):
        """
        Instrument management shall implement control of physical resources

        note from maurice 2012-05-18: we already have tests that show this, it is just show that a cmd sent to a device is executed
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_138(self):
        """
        Physical resource control shall be subject to policy

        Instrument management control capabilities shall be subject to policy

        The actor accessing the control capabilities must be authorized to send commands.

        note from maurice 2012-05-18: Talk to tim M to verify that this is policy.  If it is then talk with Stephen to get an example of a policy test and use that to create a test stub that will be completed when we have instrument policies.  
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_314(self):
        """
        Iinstrument management control capabilities shall submit requests to control a physical resource as required by policy
        
        By sending a request to the marine operator or instrument provider actor
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_315(self):
        """
        Instrument management shall implement physical resource command queing

        For use on intermittently-connected platforms. The batch file is executed the next time the platform is connected.
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_139(self):
        """
        The control services shall support resource control by another resource

        Instrument management shall implement resource control by another resource

        Subject to policy.

        note from maurice 2012-05-18: Talk to tim M to verify that this is policy.  If it is then talk with Stephen to get an example of a policy test and use that to create a test stub that will be completed when we have instrument policies.  
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_323(self):
        """
        Instrument management shall update physical resource metadata when change occurs
        
        For example, when there is a change of state.

        note from maurice 2012-05-18: consider this to mean a change of stored RR data
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_380(self):
        """
        Instrument management shall implement the synoptic notion of time

        Software to utilize NTP or PTP synoptic time for instrument and platform management purposes

        note from maurice 2012-05-18: will need to talk to tim m / alan about this, until then just write a test shell with outline of how this can be generated
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_226(self):
        """
        Test services to apply test procedures to physical resources shall be provided. 

        Instrument management shall implement qualification of physical resources on ION

        This includes testing of the instrument and its Instrument Agent for compatibility with ION. This could be carried out using the Instrument Test Kit, or online for a newly installed instrument on the marine infrastructure. The details of the qualification tests are instrument specific at the discretion of the marine operator.

        note from maurice 2012-05-18: Bill may already have this or perhaps this is just an inspection test.  Talk with Tim M.

        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_148(self):
        """
        The test services shall ensure that test results are incorporated into physical resource metadata. 

        Instrument management shall log physical resource qualifications

        These are available to the resource provider and marine operator, and should be captured in instrument metadata.

        note from maurice 2012-05-18: these are the test attachments that Bill is sending when he calls register_driver.  Int test to show that after register_driver completes, you can retrieve the attachment.
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_234(self):
        """
        The instrument repository services shall support physical resource configuration workflows
        
        Instrument management shall support instrument configuration workflows

        This sets up an instrument and instrument agent on the system, and includes the requirement to manage the workflow in a repository

        note from maurice 2012-05-18: Int test which spins up an instrument and calls the configure command.  Just add this to an existing int test, actually.
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_235(self):
        """
        The instrument repository services shall support the persistence of physical resource documentation

        Instrument management shall support the persistence of instrument documentation

        Including association with instrument metadata


        note from maurice 2012-05-18: Create an instrument resource, call add_attachment to add some doc to taht instrumetn, save then retrieve the doc to demonstrate that it was persisted.
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_161(self):
        """
        The instrument repository services shall associate the instrument repository contents with physical resource metadata

        Instrument management shall associate instrument repository content with instrument metadata

        note from maurice 2012-05-18: No idea what this means, talk with tim m.  From the use cases it seems it would also be about saving additional instrument metadata via attachments.
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_225(self):
        """
        The instrument activation services shall support the deactivation of physical resources. 

        Instrument activation shall support transition to the deactivated state of instruments

        Deactivation means the instrument is no longer available for use. This takes the instrument off-line, powers it down and shuts down its Instrument Agent.
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_335(self):
        """
        Instrument activation shall support transition to the retired state of instruments
        """
        pass

    @unittest.skip('to be completed')
    def test_l4_ci_sa_rq_336(self):
        """
        Instrument activation shall support the registration of Instrument Agents
        """
        pass
