from unittest import skip

from ion.services.dm.test.dm_test_case import DMTestCase

from pyon.public import PRED, OT, RT
from pyon.util.log import log

from ion.services.dm.test.test_dm_end_2_end import DatasetMonitor
from ion.services.dm.utility.granule import RecordDictionaryTool
from nose.plugins.attrib import attr

import numpy as np

import calendar
from datetime import datetime


@attr(group='dm')
class TestSiteDataProducts(DMTestCase):

    def create_device_site_deployment(self, dep_name="Deployment", starting=''):
        from interface.objects import StreamConfiguration, StreamConfigurationType, InstrumentDevice
        from interface.objects import InstrumentModel, PlatformAgent, InstrumentSite, TemporalBounds, Deployment
        from interface.objects import RemotePlatformDeploymentContext

        stream_conf     = StreamConfiguration(stream_name="CTD 1 Parsed Stream", parameter_dictionary_name='ctd_parsed_param_dict',  stream_type=StreamConfigurationType.PARSED)
        pdict_id        = self.dataset_management.read_parameter_dictionary_by_name(name='ctd_parsed_param_dict')
        stream_def_id   = self.create_stream_definition(name='CTD 1', parameter_dictionary_id=pdict_id)
        data_product_id = self.create_data_product(name="DDP_1", stream_def_id=stream_def_id, stream_configuration=stream_conf)
        self.activate_data_product(data_product_id)

        dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(data_product_id)
        stream_def = self.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition)[0][0]
        param_dict = self.resource_registry.find_objects(stream_def._id,  PRED.hasParameterDictionary)[0][0]
        # Add data to the DataProduct
        dataset_monitor = DatasetMonitor(dataset_id)
        self.addCleanup(dataset_monitor.stop)
        rdt  = self.ph.get_rdt(stream_def._id)
        rdt_ = self.ph.rdt_for_data_product(data_product_id)
        self.assertEquals(rdt, rdt_)
        rdt['time'] = [0,   1,  2,  3]
        rdt['temp'] = [10, 11, 12, 13]
        self.ph.publish_rdt_to_data_product(data_product_id, rdt)
        self.assertTrue(dataset_monitor.wait())

        # Create Device
        device = InstrumentDevice(name='Device 1')
        device_id = self.instrument_management.create_instrument_device(device)
        self.data_acquisition_management.register_instrument(device_id)
        self.data_acquisition_management.assign_data_product(device_id, data_product_id)

        # Create Model
        model = InstrumentModel(name='Model 1')
        model_id = self.instrument_management.create_instrument_model(model)
        self.instrument_management.assign_instrument_model_to_instrument_device(model_id, device_id)

        # Create AgentDefinition
        ad       = PlatformAgent(stream_configurations=[stream_conf])
        ad_id, _ = self.resource_registry.create(ad)

        # Create Site
        site = InstrumentSite(name='Site 1', stream_configurations=[stream_conf])
        site_id, _ = self.resource_registry.create(site)
        self.resource_registry.create_association(site_id, PRED.hasModel,           model_id)
        self.resource_registry.create_association(site_id, PRED.hasAgentDefinition, ad_id)

        # TemporalBounds of the Deployment
        temp_bounds = TemporalBounds(start_datetime=starting, end_datetime='')
        # Create Deployment
        deployment     = Deployment(name=dep_name, type="RemotePlatform", context=RemotePlatformDeploymentContext(),
                                    constraint_list=[temp_bounds])
        deployment_id  = self.observatory_management.create_deployment(deployment=deployment, site_id=site_id, device_id=device_id)

        return site_id, device_id, dataset_id, deployment_id, param_dict, data_product_id

    @attr('PRELOAD')
    def test_preload_creation(self):
        from interface.objects import DataProductTypeEnum

        self.preload_alpha()

        # IDs from Preload sheets
        deployment_id = "DEP_BTST_1"
        site_id       = "IS_BTST_SBE37"
        device_id     = "ID_BTST_SBE37"
        #deployment_id = "DEP_BTST_2"
        #site_id       = "IS_BTST_CTDSIM0"
        #device_id     = "ID_BTST_CTDSIM0"

        deployment_obj = self.container.resource_registry.find_resources_ext(alt_id=deployment_id, alt_id_ns='PRE')[0][0]
        site_obj       = self.container.resource_registry.find_resources_ext(alt_id=site_id,       alt_id_ns='PRE')[0][0]
        device_obj     = self.container.resource_registry.find_resources_ext(alt_id=device_id,     alt_id_ns='PRE')[0][0]

        # Check associations
        self.assertEquals(self.resource_registry.find_objects(site_obj._id,   PRED.hasDevice,            id_only=True)[0][0], device_obj._id)
        self.assertEquals(self.resource_registry.find_objects(site_obj._id,   PRED.hasPrimaryDeployment, id_only=True)[0][0], deployment_obj._id)
        self.assertEquals(self.resource_registry.find_objects(site_obj._id,   PRED.hasDeployment,        id_only=True)[0][0], deployment_obj._id)
        self.assertEquals(self.resource_registry.find_objects(device_obj._id, PRED.hasPrimaryDeployment, id_only=True)[0][0], deployment_obj._id)
        self.assertEquals(self.resource_registry.find_objects(device_obj._id, PRED.withinDeployment,     id_only=True)[0][0], deployment_obj._id)
        self.assertEquals(self.resource_registry.find_objects(device_obj._id, PRED.hasDeployment,        id_only=True)[0][0], deployment_obj._id)

        # stream_name to dataset_id, for lookup later
        device_stream_names = {}
        device_data_products, _ = self.resource_registry.find_objects(device_obj._id, PRED.hasOutputProduct)
        for ddp in device_data_products:
            stream_def = self.resource_registry.find_objects(ddp._id,        PRED.hasStreamDefinition)[0][0]
            dataset_id = self.RR2.find_dataset_id_of_data_product_using_has_dataset(ddp._id)
            device_stream_names[stream_def.name] = dataset_id

        site_data_products, _   = self.resource_registry.find_objects(site_obj._id,   PRED.hasOutputProduct)
        for sdp in site_data_products:
            self.assertEquals(sdp.category, DataProductTypeEnum.SITE)
            self.assertEquals(len(sdp.dataset_windows), 1)
            stream_def = self.resource_registry.find_objects(sdp._id, PRED.hasStreamDefinition)[0][0]
            assert sdp.dataset_windows[0].dataset_id            == device_stream_names.get(stream_def.name)
            assert sdp.dataset_windows[0].bounds.start_datetime == deployment_obj.start_datetime
            assert sdp.dataset_windows[0].bounds.end_datetime   == deployment_obj.end_datetime

        self.observatory_management.deactivate_deployment(deployment_id=deployment_obj._id)
        deployment_obj = self.resource_registry.read(deployment_obj._id)

        for sdp in site_data_products:
            self.assertEquals(sdp.category, DataProductTypeEnum.SITE)
            self.assertEquals(len(sdp.dataset_windows), 1)
            stream_def = self.resource_registry.find_objects(sdp._id, PRED.hasStreamDefinition)[0][0]
            assert sdp.dataset_windows[0].dataset_id            == device_stream_names.get(stream_def.name)
            assert sdp.dataset_windows[0].bounds.start_datetime == deployment_obj.start_datetime
            assert sdp.dataset_windows[0].bounds.end_datetime   == deployment_obj.end_datetime

    @attr('INT')
    def test_primary_deployment(self):
        # First deployment
        starting = str(calendar.timegm(datetime(2014, 1, 1, 0).timetuple()))
        site_1_id, device_1_id, dataset_1_id, deployment_1_id, param_dict_a, data_product_1_id = self.create_device_site_deployment(dep_name="Site 1 - Device 1",
                                                                                                                                    starting=starting)

        self.assertEquals([], self.resource_registry.find_objects(device_1_id, PRED.hasPrimaryDeployment, id_only=True)[0])
        self.assertEquals([], self.resource_registry.find_objects(site_1_id,   PRED.hasPrimaryDeployment, id_only=True)[0])
        self.assertEquals([], self.resource_registry.find_objects(device_1_id, PRED.withinDeployment,     id_only=True)[0])

        self.observatory_management.activate_deployment(deployment_id=deployment_1_id)

        self.assertEquals(deployment_1_id, self.resource_registry.find_objects(device_1_id, PRED.hasPrimaryDeployment, id_only=True)[0][0])
        self.assertEquals(deployment_1_id, self.resource_registry.find_objects(site_1_id,   PRED.hasPrimaryDeployment, id_only=True)[0][0])
        self.assertEquals(deployment_1_id, self.resource_registry.find_objects(device_1_id, PRED.withinDeployment,     id_only=True)[0][0])

        self.observatory_management.deactivate_deployment(deployment_id=deployment_1_id)

        self.assertEquals([],              self.resource_registry.find_objects(device_1_id, PRED.hasPrimaryDeployment, id_only=True)[0])
        self.assertEquals([],              self.resource_registry.find_objects(site_1_id,   PRED.hasPrimaryDeployment, id_only=True)[0])
        self.assertEquals(deployment_1_id, self.resource_registry.find_objects(device_1_id, PRED.withinDeployment,     id_only=True)[0][0])

    @attr('INT')
    @skip("Multiple deployments of the same device are not functional.  State transitions need to be looked at.")
    def test_multiple_deployments(self):
        from interface.objects import DataProductTypeEnum, TemporalBounds, Deployment, RemotePlatformDeploymentContext

        # First deployment
        starting = str(calendar.timegm(datetime(2014, 1, 1, 0).timetuple()))
        site_1_id, device_1_id, dataset_1_id, deployment_1_id, param_dict_a, data_product_1_id = self.create_device_site_deployment(dep_name="Site 1 - Device 1",
                                                                                                                                    starting=starting)
        site = self.resource_registry.read(site_1_id)

        # Create SDPs
        # This logis is also in preload, but testing preload is painful.
        # Testing it manually here for now.
        for i, scfg in enumerate(site.stream_configurations):
            pdict = self.container.resource_registry.find_resources(name=scfg.parameter_dictionary_name,
                                                                    restype=RT.ParameterDictionary, id_only=False)[0][0]
            # Clone/Create the new ParameterDictionary
            del pdict._id
            del pdict._rev
            sdp_pdict_id, _ = self.container.resource_registry.create(pdict)
            stream_def_id   = self.create_stream_definition(name='CTD 1 - SDP', parameter_dictionary_id=sdp_pdict_id)
            sdp_id          = self.create_data_product(name="SDP_%d" % i, stream_def_id=stream_def_id, stream_configuration=scfg)
            self.activate_data_product(sdp_id)
            self.container.resource_registry.create_association(subject=site_1_id,
                                                                predicate=PRED.hasOutputProduct,
                                                                object=sdp_id,
                                                                assoc_type=RT.DataProduct)
            sdp = self.resource_registry.read(sdp_id)
            sdp.category = DataProductTypeEnum.SITE
            self.resource_registry.update(sdp)

        self.observatory_management.activate_deployment(deployment_id=deployment_1_id)

        self.assertEquals(deployment_1_id, self.resource_registry.find_objects(device_1_id, PRED.hasPrimaryDeployment, id_only=True)[0][0])
        self.assertEquals(deployment_1_id, self.resource_registry.find_objects(site_1_id,   PRED.hasPrimaryDeployment, id_only=True)[0][0])
        self.assertEquals(deployment_1_id, self.resource_registry.find_objects(device_1_id, PRED.withinDeployment, id_only=True)[0][0])

        self.observatory_management.deactivate_deployment(deployment_id=deployment_1_id)

        self.assertEquals([], self.resource_registry.find_objects(device_1_id, PRED.hasPrimaryDeployment, id_only=True)[0])
        self.assertEquals([], self.resource_registry.find_objects(site_1_id,   PRED.hasPrimaryDeployment, id_only=True)[0])
        self.assertEquals(deployment_1_id, self.resource_registry.find_objects(device_1_id, PRED.withinDeployment, id_only=True)[0][0])

        sdps, _ = self.resource_registry.find_objects(site_1_id, PRED.hasOutputProduct)
        for sdp in sdps:
            self.assertEquals(sdp.category, DataProductTypeEnum.SITE)
            self.assertEquals(len(sdp.dataset_windows), 1)
            assert sdp.dataset_windows[0].dataset_id            == dataset_1_id
            assert sdp.dataset_windows[0].bounds.start_datetime == starting
            assert int(sdp.dataset_windows[0].bounds.end_datetime) - calendar.timegm(datetime.utcnow().timetuple()) < 10

        # Second deployment (same site and device)
        starting2    = str(calendar.timegm(datetime(2014, 1, 5, 0).timetuple()))
        temp_bounds2 = TemporalBounds(start_datetime=starting2, end_datetime='')
        deployment_2 = Deployment(name="Site 1 - Device 1 - v2", type="RemotePlatform", context=RemotePlatformDeploymentContext(),
                                  constraint_list=[temp_bounds2])
        deployment_2_id  = self.observatory_management.create_deployment(deployment=deployment_2, site_id=site_1_id, device_id=device_1_id)

        self.observatory_management.activate_deployment(deployment_id=deployment_2_id)

        self.assertEquals(deployment_2_id, self.resource_registry.find_objects(device_1_id, PRED.hasPrimaryDeployment, id_only=True)[0][0])
        self.assertEquals(deployment_2_id, self.resource_registry.find_objects(site_1_id,   PRED.hasPrimaryDeployment, id_only=True)[0][0])
        self.assertItemsEqual([deployment_1_id, deployment_2_id], self.resource_registry.find_objects(device_1_id, PRED.withinDeployment, id_only=True)[0])

        self.observatory_management.deactivate_deployment(deployment_id=deployment_1_id)

        self.assertEquals([], self.resource_registry.find_objects(device_1_id, PRED.hasPrimaryDeployment, id_only=True)[0])
        self.assertEquals([], self.resource_registry.find_objects(site_1_id,   PRED.hasPrimaryDeployment, id_only=True)[0])
        self.assertItemsEqual([deployment_1_id, deployment_2_id], self.resource_registry.find_objects(device_1_id, PRED.withinDeployment, id_only=True)[0])

        sdps, _ = self.resource_registry.find_objects(site_1_id, PRED.hasOutputProduct)
        for sdp in sdps:
            self.assertEquals(sdp.category, DataProductTypeEnum.SITE)
            self.assertEquals(len(sdp.dataset_windows), 2)
            assert sdp.dataset_windows[0].dataset_id            == dataset_1_id
            assert sdp.dataset_windows[0].bounds.start_datetime == starting
            assert int(sdp.dataset_windows[0].bounds.end_datetime) - calendar.timegm(datetime.utcnow().timetuple()) < 10

    @attr('INT')
    def test_single_device_single_site(self):
        from interface.objects import DataProductTypeEnum

        starting = str(calendar.timegm(datetime(2014, 1, 1, 0).timetuple()))
        site_1_id, device_1_id, dataset_1_id, deployment_1_id, param_dict_a, data_product_1_id = self.create_device_site_deployment(dep_name="Site 1 - Device 1",
                                                                                                                                    starting=starting)
        site = self.resource_registry.read(site_1_id)

        # Create SDPs
        # This logis is also in preload, but testing preload is painful.
        # Testing it manually here for now.
        for i, scfg in enumerate(site.stream_configurations):
            pdict = self.container.resource_registry.find_resources(name=scfg.parameter_dictionary_name,
                                                                    restype=RT.ParameterDictionary, id_only=False)[0][0]
            # Clone/Create the new ParameterDictionary
            del pdict._id
            del pdict._rev
            sdp_pdict_id, _ = self.container.resource_registry.create(pdict)
            stream_def_id   = self.create_stream_definition(name='CTD 1 - SDP', parameter_dictionary_id=sdp_pdict_id)
            sdp_id          = self.create_data_product(name="SDP_%d" % i, stream_def_id=stream_def_id, stream_configuration=scfg)
            self.activate_data_product(sdp_id)
            self.container.resource_registry.create_association(subject=site_1_id,
                                                                predicate=PRED.hasOutputProduct,
                                                                object=sdp_id,
                                                                assoc_type=RT.DataProduct)
            sdp = self.resource_registry.read(sdp_id)
            sdp.category = DataProductTypeEnum.SITE
            self.resource_registry.update(sdp)

        self.observatory_management.activate_deployment(deployment_id=deployment_1_id)

        # Get Deployment start time
        deployment_obj = self.resource_registry.read(deployment_1_id)
        for constraint in deployment_obj.constraint_list:
            if constraint.type_ == OT.TemporalBounds:
                assert constraint.start_datetime == starting

        # Get information about the new SiteDataProduct that should have been created
        site_data_product_1_id = self.resource_registry.find_objects(site_1_id,              PRED.hasOutputProduct,    id_only=True)[0][0]
        stream_def_2_id        = self.resource_registry.find_objects(site_data_product_1_id, PRED.hasStreamDefinition, id_only=True)[0][0]
        param_dict_b           = self.resource_registry.find_objects(stream_def_2_id,        PRED.hasParameterDictionary)[0][0]

        # Check associations
        self.assertEquals(self.resource_registry.find_objects(site_1_id,   PRED.hasDevice,        id_only=True)[0][0], device_1_id)
        self.assertEquals(self.resource_registry.find_objects(site_1_id,   PRED.hasDeployment,    id_only=True)[0][0], deployment_1_id)
        self.assertEquals(self.resource_registry.find_objects(device_1_id, PRED.hasDeployment,    id_only=True)[0][0], deployment_1_id)
        self.assertEquals(self.resource_registry.find_objects(device_1_id, PRED.hasOutputProduct, id_only=True)[0][0], data_product_1_id)
        self.assertEquals(self.resource_registry.find_objects(site_1_id,   PRED.hasOutputProduct, id_only=True)[0][0], site_data_product_1_id)

        site_data_product_1 = self.resource_registry.find_objects(site_1_id, PRED.hasOutputProduct)[0][0]
        self.assertEquals(site_data_product_1.category, DataProductTypeEnum.SITE)
        self.assertEquals(len(site_data_product_1.dataset_windows), 1)
        assert site_data_product_1.dataset_windows[0].dataset_id            == dataset_1_id
        assert site_data_product_1.dataset_windows[0].bounds.start_datetime == starting
        assert site_data_product_1.dataset_windows[0].bounds.end_datetime   == ''

        # Check that param dicts have equal members
        self.assertEquals(param_dict_a.name, param_dict_b.name)

        self.observatory_management.deactivate_deployment(deployment_id=deployment_1_id)

        # Verify the window has an ending time
        site_data_product_1 = self.resource_registry.find_objects(site_1_id, PRED.hasOutputProduct)[0][0]
        self.assertEquals(site_data_product_1.category, DataProductTypeEnum.SITE)
        self.assertEquals(len(site_data_product_1.dataset_windows), 1)
        assert site_data_product_1.dataset_windows[0].dataset_id            == dataset_1_id
        assert site_data_product_1.dataset_windows[0].bounds.start_datetime == starting
        assert int(site_data_product_1.dataset_windows[0].bounds.end_datetime) - calendar.timegm(datetime.utcnow().timetuple()) < 10

        # Verify that data is there
        granule = self.data_retriever.retrieve(dataset_1_id)
        rdt = RecordDictionaryTool.load_from_granule(granule)
        np.testing.assert_allclose(rdt['time'], np.arange(4))
        np.testing.assert_allclose(rdt['temp'], np.arange(10, 14))
