#!/usr/bin/env python
# coding=utf-8

"""
@package ion.services.sa.observatory.test.activation_test_resources
@file ion/services/sa/observatory/test/activation_test_resources.py
@author Edward Hunter
@brief Object configurations for activation tests. Unclutters test file.
"""

__author__ = 'Edward Hunter'
__license__ = 'Apache 2.0'

# Pyon object and resource imports.
from pyon.public import IonObject, log, RT, PRED, LCS, OT, CFG

from interface.objects import PlatformSite
from interface.objects import InstrumentSite
from interface.objects import Deployment
from interface.objects import CabledInstrumentDeploymentContext
from interface.objects import CabledNodeDeploymentContext
from interface.objects import RemotePlatformDeploymentContext
from interface.objects import GeospatialBounds
from interface.objects import GeospatialCoordinateReferenceSystem
from interface.objects import GeospatialIndex
from interface.objects import TemporalBounds
from interface.objects import PlatformPort
from interface.objects import SiteEnvironmentType
from interface.objects import CommissionedStatusType
from interface.objects import ContactInformation
from interface.objects import DriverTypeEnum
from interface.objects import DeploymentTypeEnum

# Following define resources load atop preload until
# preload is augmented to include them. We assume the system
# exists will all sites and deployments existing.

RSN_FACILITY_NAME = 'RSN Facility'
RSN_FACILITY_ALT_ID = 'MF_RSN'

RSN_PLATFORM_SITE = 'Medium Power JBox 01A - Regional Continental Margin Base'
RSN_INSTRUMENT_SITE = 'Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base'
RSN_INSTRUMENT_01 = 'Instrument RS01SLBS-MJ01A-02-PRESTA999 device #01'
RSN_INSTRUMENT_02 = 'Instrument RS01SLBS-MJ01A-02-PRESTA999 device #02'

# 3-Wavelength Fluorometer on Mooring Riser 003 - Coastal Pioneer Central
# Instrument CP01CNSM-RI003-05-FLORTD999 device #01
EXAMPLE_DEVICE_ALT_ID = 'CP01CNSM-RI003-05-FLORTD999_ID'


RSN_INSTRUMENT_01 = dict(
        org='rsn',
        instrument_model = 'PRESTA',
        platform_device = 'RS01SLBS-MJ01A_PD',
        site = 'RS01SLBS-MJ01A-02-PRESTA999',
        name='Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base',
        description='Instrument RS01SLBS-MJ01A-02-PRESTA999 device #01',
        alt_ids=["PRE:RS01SLBS-MJ01A-02-PRESTA999_ID"],
        serial_number='',
        monitorable=True,
        controllable=True,
        message_controllable=True,
        custom_attributes={},
        contacts=[ContactInformation()],
        reference_urls=[],
        commissioned=CommissionedStatusType.COMMISSIONED,
        last_calibration_datetime='',
        hardware_version='',
        firmware_version='',
        #lcstate='DEPLOYED',
        #availability='AVAILABLE',
        #ts_created='',
        #ts_updated='',
        #addl={},
)


RSN_INSTRUMENT_02 = dict (
    name='Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base',
    description='Instrument RS01SLBS-MJ01A-02-PRESTA999 device #02',
    alt_ids=[],
    serial_number='',
    monitorable=True,
    controllable=True,
    message_controllable=True,
    custom_attributes={},
    contacts=[ContactInformation()],
    reference_urls=[],
    commissioned=CommissionedStatusType.COMMISSIONED,
    last_calibration_datetime='',
    hardware_version='',
    firmware_version='',
    #lcstate='DEPLOYED',
    #availability='AVAILABLE',
    #ts_created='',
    #ts_updated='',
    #addl={},
)


RSN_AGENT_01 = dict(
        org='rsn',
        agent='Agent-0.1-PRESTA',
        device = 'RS01SLBS-MJ01A-02-PRESTA999_ID',
        name='PRESTA Agent Instance',
        description='Instrument Agent Instance for PRESTA Device',
        alt_ids=['PRE:AgentInstance-PRESTA'],
        agent_config={},
        startup_config={},
        agent_spawn_config={},
        saved_agent_state={},
        driver_config={},
        port_agent_config={},
        alerts=[],
        agent_process_id='',
        deployment_type=DeploymentTypeEnum.PROCESS,
        #lcstate='DEPLOYED',
        #availability='AVAILABLE',
        #type_='InstrumentAgentInstance',
        #ts_created='',
        #ts_updated='',
        #addl={},
    )


RSN_AGENT_02 = dict(
        #org='rsn',
        #agent='Agent-0.1-PRESTA',
        #device = 'RS01SLBS-MJ01A-02-PRESTA999_ID',
        name='PRESTA Agent Instance',
        description='Instrument Agent Instance for PRESTA Device',
        alt_ids=['PRE:AgentInstance-PRESTA'],
        agent_config={},
        startup_config={},
        agent_spawn_config={},
        saved_agent_state={},
        driver_config={},
        port_agent_config={},
        alerts=[],
        agent_process_id='',
        deployment_type=DeploymentTypeEnum.PROCESS,
        #lcstate='DEPLOYED',
        #availability='AVAILABLE',
        #type_='InstrumentAgentInstance',
        #ts_created='',
        #ts_updated='',
        #addl={},
    )

AUGMENT_INSTRUMENT_SITES = [
    dict(
        org='rsn',
        parent_site = 'RS01SLBS-MJ01A',
        instrument_models = ['PRESTA'],
        name='Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base',
        description='Instrument: RS01SLBS-MJ01A-02-PRESTA999',
        alt_ids=["OOI:RS01SLBS-MJ01A-02-PRESTA999", "PRE:RS01SLBS-MJ01A-02-PRESTA999"],
        local_name='Tidal Seafloor Pressure (PRESTA)',
        reference_designator='RS01SLBS-MJ01A-02-PRESTA999',
        environment=SiteEnvironmentType.FIELD,
        constraint_list=[GeospatialBounds(), TemporalBounds()],
        coordinate_reference_system=GeospatialCoordinateReferenceSystem(),
        geospatial_point_center=GeospatialIndex(),
        planned_uplink_port=PlatformPort(),
        #lcstate='DEPLOYED',
        #availability='AVAILABLE',
        #ts_created='',
        #ts_updated='',
        #addl=None,
        #alt_resource_type=''
    )
]

AUGMENT_PLATFORM_DEVICES =[
    dict(
        org='rsn',
        platform_model='MJ_PM',
        parent_device='RS01SLOP-PN01A_PD',
        network_parent='RS01SLOP-PN01A_PD',
        name='Medium Power JBox 01A - Regional Continental Margin Base device #01',
        description='Platform RS01SLBS-MJ01A device #01',
        alt_ids=["PRE:RS01SLBS-MJ01A_PD"],
        serial_number='',
        monitorable=True,
        controllable=True,
        message_controllable=True,
        platform_monitor_attributes=[],
        custom_attributes={},
        ports=[],
        contacts=[ContactInformation()],
        index_location=GeospatialIndex(),
        reference_urls=[],
        commissioned=CommissionedStatusType.COMMISSIONED,
        #lcstate='DEPLOYED',
        #availability='AVAILABLE',
        #ts_created='',
        #ts_updated='',
        #addl={},
    )
]

AUGMENT_PLATFORM_AGENTS =[
    dict(
        org='rsn',
        models = ['MJ_PM'],
        name='MJ_PM Agent 0.1',
        description='Instrument Agent for Medium Power JBox Device',
        alt_ids=['PRE:Agent-0.1-MJ_PM'],
        agent_module='path.to.agent.mod',
        agent_class='agent_class',
        agent_uri='agent_uri',
        agent_version='0.1',
        agent_default_config={},
        stream_configurations=[],
        driver_module='path.to.driver.mod',
        driver_class='driver_class',
        driver_uri='driver_uri',
        driver_version='0.1',
        driver_type=DriverTypeEnum.CLASS,
        commissioned=CommissionedStatusType.COMMISSIONED,
        #lcstate='DEPLOYED',
        #availability='AVAILABLE',
        #type_=InstrumentAgent,
        #ts_created='',
        #ts_updated='',
        #addl={},
    )
]

AUGMENT_PLATFORM_AGENT_INSTANCES =[
    dict(
        org='rsn',
        agent='Agent-0.1-MJ_PM',
        device = 'RS01SLBS-MJ01A_PD',
        name='MJ_PM Agent Instance',
        description='Instrument Agent Instance for Medium Power JBox Device',
        alt_ids=['PRE:AgentInstance-MJ_PM'],
        agent_config={},
        agent_spawn_config={},
        saved_agent_state={},
        driver_config={},
        alerts=[],
        agent_process_id='',
        deployment_type=DeploymentTypeEnum.PROCESS,
        #lcstate='DEPLOYED',
        #availability='AVAILABLE',
        #type_='InstrumentAgentInstance',
        #ts_created='',
        #ts_updated='',
        #addl={},
    )
]

AUGMENT_INSTRUMENT_DEVICES = [
    RSN_INSTRUMENT_01
]

AUGMENT_INSTRUMENT_AGENTS =[
    dict(
        org='rsn',
        models = ['PRESTA'],
        name='PRESTA Agent 0.1',
        description='Instrument Agent for PRESTA Device',
        alt_ids=['PRE:Agent-0.1-PRESTA'],
        agent_module='path.to.agent.mod',
        agent_class='agent_class',
        agent_uri='agent_uri',
        agent_version='0.1',
        agent_default_config={},
        stream_configurations=[],
        driver_module='path.to.driver.mod',
        driver_class='driver_class',
        driver_uri='driver_uri',
        driver_version='0.1',
        driver_type=DriverTypeEnum.EXT_PROCESS,
        commissioned=CommissionedStatusType.COMMISSIONED,
        lcstate='DEPLOYED',
        availability='AVAILABLE',
        #type_=InstrumentAgent,
        #ts_created='',
        #ts_updated='',
        #addl={},
    )
]

AUGMENT_INSTRUMENT_AGENT_INSTANCES =[
    RSN_AGENT_01
]

AUGMENT_DATASET_AGENTS =[

]

AUGMENT_DATASET_AGENT_INSTANCES =[

]

AUGMENT_PLATFORM_DEPLOYMENTS = [
    dict(
        org='rsn',
        platform_site='RS01SLBS-MJ01A',
        platform_device='RS01SLBS-MJ01A_PD',
        name='Deployment of platform RS01SLBS-MJ01A',
        description='Deployment: RS01SLBS-MJ01A_DEP',
        alt_ids=["PRE:RS01SLBS-MJ01A_DEP"],
        constraint_list=[TemporalBounds()],
        coordinate_reference_system=GeospatialCoordinateReferenceSystem(),
        geospatial_point_center=GeospatialIndex(),
        port_assignments={},
        context=CabledNodeDeploymentContext(),
        auxiliary_name=None,
        auxiliary_identifier=None,
        #lcstate='DEPLOYED',
        #availability='AVAILABLE',
        #ts_created='',
        #ts_updated='',
        #addl=None,
    )
]

RSN_INST_DEPLOYMENT_1 = dict(
    org='rsn',
    instrument_site='RS01SLBS-MJ01A-02-PRESTA999',
    instrument_device='RS01SLBS-MJ01A-02-PRESTA999_ID',
    name='Deployment of instrument RS01SLBS-MJ01A-02-PRESTA999_ID',
    description='Deployment: RS01SLBS-MJ01A-02-PRESTA999_DEP',
    alt_ids=['PRE:RS01SLBS-MJ01A-02-PRESTA999_DEP'],
    constraint_list=[TemporalBounds()],
    coordinate_reference_system=GeospatialCoordinateReferenceSystem(),
    geospatial_point_center=GeospatialIndex(),
    port_assignments={},
    context=CabledNodeDeploymentContext(),
    auxiliary_name=None,
    auxiliary_identifier=None,
    #lcstate='DEPLOYED',
    #availability='AVAILABLE',
    #ts_created='',
    #ts_updated='',
    #addl=None,
)

RSN_INST_DEPLOYMENT_2 = dict(
    #org='rsn',
    #instrument_site='RS01SLBS-MJ01A-02-PRESTA999',
    #instrument_device='RS01SLBS-MJ01A-02-PRESTA999_ID',
    name='Deployment of instrument RS01SLBS-MJ01A-02-PRESTA999_ID',
    description='Deployment: RS01SLBS-MJ01A-02-PRESTA999_DEP',
    alt_ids=['PRE:RS01SLBS-MJ01A-02-PRESTA999_DEP'],
    constraint_list=[TemporalBounds()],
    coordinate_reference_system=GeospatialCoordinateReferenceSystem(),
    geospatial_point_center=GeospatialIndex(),
    port_assignments={},
    context=CabledNodeDeploymentContext(),
    auxiliary_name=None,
    auxiliary_identifier=None,
    #lcstate='DEPLOYED',
    #availability='AVAILABLE',
    #ts_created='',
    #ts_updated='',
    #addl=None,
)

AUGMENT_INSTRUMENT_DEPLOYMENTS = [
    RSN_INST_DEPLOYMENT_1
    ]





"""
INITIAL DEPLOYED:
========================================================================================================================================================================================================
lcstate                                            DEPLOYED
_rev                                               3
firmware_version
availability                                       AVAILABLE
controllable                                       True
uuid
contacts                                           [ContactInformation({'individual_names_given': '', 'city': '', 'roles': [], 'administrative_area': '', 'url': '', 'country': '', 'variables': [{'name': '', 'value': ''}], 'organization_name': '', 'postal_code': '', 'individual_name_family': '', 'phones': [], 'position_name': '', 'email': '', 'street_address': ''})]
custom_attributes                                  {}
monitorable                                        True
serial_number
addl                                               {}
message_controllable                               True
description                                        Instrument RS01SLBS-MJ01A-02-PRESTA999 device #01
reference_urls                                     []
ts_updated                                         1393345380674
commissioned                                       2
ts_created                                         1393345380539
last_calibration_datetime
name                                               Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
alt_ids                                            ['PRE:RS01SLBS-MJ01A-02-PRESTA999_ID']
hardware_version
type_                                              InstrumentDevice
_id                                                62d3fbcbeb4f4e09886e36de0608abe0
========================================================================================================================================================================================================
InstrumentSite                 Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base                                      hasDevice                      this InstrumentDevice
PlatformDevice                 Medium Power JBox 01A - Regional Continental Margin Base device #01                                                      hasDevice                      this InstrumentDevice
Org                            RSN Facility                                                                                                             hasResource                    this InstrumentDevice
========================================================================================================================================================================================================
this InstrumentDevice               hasAgentInstance               InstrumentAgentInstance        PRESTA Agent Instance
this InstrumentDevice               hasDataProducer                DataProducer                   Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
this InstrumentDevice               hasDeployment                  Deployment                     Deployment of instrument RS01SLBS-MJ01A-02-PRESTA999_ID
this InstrumentDevice               hasModel                       InstrumentModel                Tidal Seafloor Pressure (PREST-A)
========================================================================================================================================================================================================


INITIAL DEACTIVATED:
========================================================================================================================================================================================================
lcstate                                            INTEGRATED
_rev                                               4
firmware_version
availability                                       AVAILABLE
controllable                                       True
uuid
contacts                                           [ContactInformation({'individual_names_given': '', 'city': '', 'roles': [], 'administrative_area': '', 'url': '', 'country': '', 'variables': [{'name': '', 'value': ''}], 'organization_name': '', 'postal_code': '', 'individual_name_family': '', 'phones': [], 'position_name': '', 'email': '', 'street_address': ''})]
custom_attributes                                  {}
monitorable                                        True
serial_number
addl                                               {}
message_controllable                               True
description                                        Instrument RS01SLBS-MJ01A-02-PRESTA999 device #01
reference_urls                                     []
ts_updated                                         1393345384693
commissioned                                       2
ts_created                                         1393345380539
last_calibration_datetime
name                                               Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
alt_ids                                            ['PRE:RS01SLBS-MJ01A-02-PRESTA999_ID']
hardware_version
type_                                              InstrumentDevice
_id                                                62d3fbcbeb4f4e09886e36de0608abe0
========================================================================================================================================================================================================
PlatformDevice                 Medium Power JBox 01A - Regional Continental Margin Base device #01                                                      hasDevice                      this InstrumentDevice
Org                            RSN Facility                                                                                                             hasResource                    this InstrumentDevice
========================================================================================================================================================================================================
this InstrumentDevice               hasAgentInstance               InstrumentAgentInstance        PRESTA Agent Instance
this InstrumentDevice               hasDataProducer                DataProducer                   Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
this InstrumentDevice               hasDeployment                  Deployment                     Deployment of instrument RS01SLBS-MJ01A-02-PRESTA999_ID
this InstrumentDevice               hasModel                       InstrumentModel                Tidal Seafloor Pressure (PREST-A)
========================================================================================================================================================================================================


FINAL DEACTIVATED:
========================================================================================================================================================================================================
lcstate                                            DEVELOPED
_rev                                               6
firmware_version
availability                                       PRIVATE
controllable                                       True
uuid
contacts                                           [ContactInformation({'individual_names_given': '', 'city': '', 'roles': [], 'administrative_area': '', 'url': '', 'country': '', 'variables': [{'name': '', 'value': ''}], 'organization_name': '', 'postal_code': '', 'individual_name_family': '', 'phones': [], 'position_name': '', 'email': '', 'street_address': ''})]
custom_attributes                                  {}
monitorable                                        True
serial_number
addl                                               {}
message_controllable                               True
description                                        Instrument RS01SLBS-MJ01A-02-PRESTA999 device #01
reference_urls                                     []
ts_updated                                         1393345917044
commissioned                                       2
ts_created                                         1393345912849
last_calibration_datetime
name                                               Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
alt_ids                                            ['PRE:RS01SLBS-MJ01A-02-PRESTA999_ID']
hardware_version
type_                                              InstrumentDevice
_id                                                ec36a44227a04668bef3a509f276f22e
========================================================================================================================================================================================================
Org                            RSN Facility                                                                                                             hasResource                    this InstrumentDevice
========================================================================================================================================================================================================
this InstrumentDevice               hasDataProducer                DataProducer                   Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
this InstrumentDevice               hasDeployment                  Deployment                     Deployment of instrument RS01SLBS-MJ01A-02-PRESTA999_ID
this InstrumentDevice               hasModel                       InstrumentModel                Tidal Seafloor Pressure (PREST-A)
========================================================================================================================================================================================================



DEVELOPED:
========================================================================================================================================================================================================
lcstate                                            DEVELOPED
_rev                                               2
firmware_version
availability                                       PRIVATE
controllable                                       True
uuid
contacts                                           [ContactInformation({'individual_names_given': '', 'city': '', 'roles': [], 'administrative_area': '', 'url': '', 'country': '', 'variables': [{'name': '', 'value': ''}], 'organization_name': '', 'postal_code': '', 'individual_name_family': '', 'phones': [], 'position_name': '', 'email': '', 'street_address': ''})]
custom_attributes                                  {}
monitorable                                        True
serial_number
addl                                               {}
message_controllable                               True
description                                        Instrument RS01SLBS-MJ01A-02-PRESTA999 device #02
reference_urls                                     []
ts_updated                                         1393346414677
commissioned                                       2
ts_created                                         1393346414367
last_calibration_datetime
name                                               Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
alt_ids                                            []
hardware_version
type_                                              InstrumentDevice
_id                                                9fdcaa1b73054bf795b113988f5d73d8
========================================================================================================================================================================================================
Org                            RSN Facility                                                                                                             hasResource                    this InstrumentDevice
========================================================================================================================================================================================================
this InstrumentDevice               hasAgentInstance               InstrumentAgentInstance        PRESTA Agent Instance
this InstrumentDevice               hasDataProducer                DataProducer                   Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
this InstrumentDevice               hasModel                       InstrumentModel                Tidal Seafloor Pressure (PREST-A)
========================================================================================================================================================================================================



INTEGRATED:
========================================================================================================================================================================================================
lcstate                                            INTEGRATED
_rev                                               3
firmware_version
availability                                       PRIVATE
controllable                                       True
uuid
contacts                                           [ContactInformation({'individual_names_given': '', 'city': '', 'roles': [], 'administrative_area': '', 'url': '', 'country': '', 'variables': [{'name': '', 'value': ''}], 'organization_name': '', 'postal_code': '', 'individual_name_family': '', 'phones': [], 'position_name': '', 'email': '', 'street_address': ''})]
custom_attributes                                  {}
monitorable                                        True
serial_number
addl                                               {}
message_controllable                               True
description                                        Instrument RS01SLBS-MJ01A-02-PRESTA999 device #02
reference_urls                                     []
ts_updated                                         1393346415054
commissioned                                       2
ts_created                                         1393346414367
last_calibration_datetime
name                                               Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
alt_ids                                            []
hardware_version
type_                                              InstrumentDevice
_id                                                9fdcaa1b73054bf795b113988f5d73d8
========================================================================================================================================================================================================
PlatformDevice                 Medium Power JBox 01A - Regional Continental Margin Base device #01                                                      hasDevice                      this InstrumentDevice
Org                            RSN Facility                                                                                                             hasResource                    this InstrumentDevice
========================================================================================================================================================================================================
this InstrumentDevice               hasAgentInstance               InstrumentAgentInstance        PRESTA Agent Instance
this InstrumentDevice               hasDataProducer                DataProducer                   Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
this InstrumentDevice               hasModel                       InstrumentModel                Tidal Seafloor Pressure (PREST-A)
========================================================================================================================================================================================================


DEPLOYED
========================================================================================================================================================================================================
lcstate                                            DEPLOYED
_rev                                               5
firmware_version
availability                                       AVAILABLE
controllable                                       True
uuid
contacts                                           [ContactInformation({'individual_names_given': '', 'city': '', 'roles': [], 'administrative_area': '', 'url': '', 'country': '', 'variables': [{'name': '', 'value': ''}], 'organization_name': '', 'postal_code': '', 'individual_name_family': '', 'phones': [], 'position_name': '', 'email': '', 'street_address': ''})]
custom_attributes                                  {}
monitorable                                        True
serial_number
addl                                               {}
message_controllable                               True
description                                        Instrument RS01SLBS-MJ01A-02-PRESTA999 device #02
reference_urls                                     []
ts_updated                                         1393346415223
commissioned                                       2
ts_created                                         1393346414367
last_calibration_datetime
name                                               Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
alt_ids                                            []
hardware_version
type_                                              InstrumentDevice
_id                                                9fdcaa1b73054bf795b113988f5d73d8
========================================================================================================================================================================================================
InstrumentSite                 Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base                                      hasDevice                      this InstrumentDevice
PlatformDevice                 Medium Power JBox 01A - Regional Continental Margin Base device #01                                                      hasDevice                      this InstrumentDevice
Org                            RSN Facility                                                                                                             hasResource                    this InstrumentDevice
========================================================================================================================================================================================================
this InstrumentDevice               hasAgentInstance               InstrumentAgentInstance        PRESTA Agent Instance
this InstrumentDevice               hasDataProducer                DataProducer                   Tidal Seafloor Pressure on Medium Power JBox 01A - Regional Continental Margin Base
this InstrumentDevice               hasDeployment                  Deployment                     Deployment of instrument RS01SLBS-MJ01A-02-PRESTA999_ID
this InstrumentDevice               hasModel                       InstrumentModel                Tidal Seafloor Pressure (PREST-A)
========================================================================================================================================================================================================


"""