#!/usr/bin/env python

"""
@package ion.agents.data.handlers.test.test_netcdf_data_handler
@file ion/agents/data/handlers/test/test_netcdf_data_handler
@author Christopher Mueller
@brief Test cases for netcdf_data_handler
"""

from pyon.public import log
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from mock import Mock, sentinel, MagicMock, patch

from ion.services.dm.utility.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.netcdf_data_handler import NetcdfDataHandler
from interface.objects import ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Granule
from netCDF4 import Dataset
from pyon.core.interceptor.encode import encode_ion
import msgpack


@attr('UNIT', group='eoi')
class TestNetcdfDataHandlerUnit(PyonTestCase):

    def setUp(self):
        self.__rr_cli = Mock()
        pass

    def test__init_acquisition_cycle_no_ext_ds_res(self):
        config = {}
        NetcdfDataHandler._init_acquisition_cycle(config)
        self.assertEqual(config, config)

    def test__init_acquisition_cycle_ext_ds_res(self):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['dataset_path'] = 'test_data/usgs.nc'
        config = {'external_dataset_res': edres}
        NetcdfDataHandler._init_acquisition_cycle(config)
        self.assertIn('dataset_object', config)
        self.assertTrue(isinstance(config['dataset_object'], Dataset))

    def test__constraints_for_new_request_no_ext_ds(self):
        config = {}
        retval = NetcdfDataHandler._constraints_for_new_request(config=config)
        self.assertIsNone(retval)

    def test__constraints_for_new_request(self):
        old_list = [1302393600, 1302480000, 1302566400, 1302652800, 1302739200,
                    1302825600, 1302912000, 1302998400, 1303084800, 1303171200, 1303257600,
                    1303344000, 1303430400, 1303516800, 1303603200, 1303689600, 1303776000,
                    1303862400, 1303948800, 1304035200, 1304121600, 1304208000, 1304294400,
                    1304380800, 1304467200, 1304553600, 1304640000, 1304726400, 1304812800,
                    1304899200, 1304985600, 1305072000, 1305158400, 1305244800, 1305331200,
                    1305417600, 1305504000, 1305590400, 1305676800, 1305763200, 1305849600,
                    1305936000, 1306022400, 1306108800, 1306195200, 1306281600, 1306368000,
                    1306454400, 1306540800, 1306627200, 1306713600, 1306800000, 1306886400,
                    1306972800, 1307059200, 1307145600, 1307232000, 1307318400, 1307404800,
                    1307491200, 1307577600, 1307664000, 1307750400, 1307836800, 1307923200,
                    1308009600, 1308096000, 1308182400, 1308268800, 1308355200, 1308441600,
                    1308528000, 1308614400, 1308700800, 1308787200, 1308873600, 1308960000,
                    1309046400, 1309132800, 1309219200, 1309305600, 1309392000, 1309478400,
                    1309564800, 1309651200, 1309737600, 1309824000, 1309910400, 1309996800,
                    1310083200, 1310169600, 1310256000, 1310342400, 1310428800, 1310515200,
                    1310601600, 1310688000, 1310774400, 1310860800, 1310947200, 1311033600,
                    1311120000, 1311206400, 1311292800, 1311379200, 1311465600, 1311552000,
                    1311638400, 1311724800, 1311811200, 1311897600, 1311984000, 1312070400,
                    1312156800, 1312243200, 1312329600, 1312416000, 1312502400, 1312588800,
                    1312675200, 1312761600, 1312848000, 1312934400, 1313020800, 1313107200,
                    1313193600, 1313280000, 1313366400, 1313452800, 1313539200, 1313625600,
                    1313712000, 1313798400, 1313884800, 1313971200, 1314057600, 1314144000,
                    1314230400, 1314316800, 1314403200, 1314489600, 1314576000, 1314662400,
                    1314748800, 1314835200, 1314921600, 1315008000, 1315094400, 1315180800,
                    1315267200, 1315353600, 1315440000, 1315526400, 1315612800, 1315699200,
                    1315785600, 1315872000, 1315958400, 1316044800, 1316131200, 1316217600,
                    1316304000, 1316390400, 1316476800, 1316563200, 1316649600, 1316736000,
                    1316822400, 1316908800, 1316995200, 1317081600, 1317168000, 1317254400,
                    1317340800, 1317427200, 1317513600, 1317600000, 1317686400, 1317772800,
                    1317859200, 1317945600, 1318032000, 1318118400, 1318204800, 1318291200,
                    1318377600, 1318464000, 1318550400, 1318636800, 1318723200, 1318809600,
                    1318896000, 1318982400, 1319068800, 1319155200, 1319241600, 1319328000,
                    1319414400, 1319500800, 1319587200, 1319673600, 1319760000, 1319846400,
                    1319932800, 1320019200, 1320105600, 1320192000, 1320278400, 1320364800,
                    1320451200, 1320537600, 1320624000, 1320710400, 1320796800, 1320883200,
                    1320969600, 1321056000, 1321142400, 1321228800, 1321315200, 1321401600,
                    1321488000, 1321574400, 1321660800, 1321747200, 1321833600, 1321920000,
                    1322006400, 1322092800, 1322179200, 1322265600, 1322352000, 1322438400,
                    1322524800, 1322611200, 1322697600, 1322784000, 1322870400, 1322956800,
                    1323043200, 1323129600, 1323216000, 1323302400, 1323388800, 1323475200,
                    1323561600, 1323648000, 1323734400, 1323820800, 1323907200, 1323993600,
                    1324080000, 1324166400, 1324252800, 1324339200, 1324425600, 1324512000,
                    1324598400, 1324684800, 1324771200, 1324857600, 1324944000, 1325030400,
                    1325116800, 1325203200, 1325289600, 1325376000, 1325462400, 1325548800,
                    1325635200, 1325721600, 1325808000, 1325894400, 1325980800, 1326067200,
                    1326153600, 1326240000, 1326326400, 1326412800, 1326499200, 1326585600]
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['dataset_path'] = 'test_data/usgs.nc'
        edres.dataset_description.parameters['temporal_dimension'] = 'time'
        edres.update_description.parameters['new_data_check'] = msgpack.packb(old_list, default=encode_ion)
        config = {'external_dataset_res': edres, 'dataset_object': Dataset(edres.dataset_description.parameters['dataset_path'])}

        ret = NetcdfDataHandler._constraints_for_new_request(config)
        #log.debug('test__constraints_for_new_request: {0}'.format(ret['temporal_slice']))
        self.assertEqual(ret['temporal_slice'], slice(281, 295, None))

    def test__constrainst_for_historical_request(self):
        config = {}
        retval = NetcdfDataHandler._constraints_for_historical_request(config=config)
        self.assertEqual(retval, {})

    @patch('ion.agents.data.handlers.netcdf_data_handler.RecordDictionaryTool')
    def test__get_data(self, RecordDictionaryTool_mock):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['dataset_path'] = 'test_data/usgs.nc'
        edres.dataset_description.parameters['temporal_dimension'] = 'time'
        edres.dataset_description.parameters['zonal_dimension'] = 'lat'
        edres.dataset_description.parameters['meridional_dimension'] = 'lon'
        edres.dataset_description.parameters['vertical_dimension'] = 'z'
        edres.dataset_description.parameters['variables'] = ['water_temperature']
        config = {'external_dataset_res': edres,
                  'dataset_object': Dataset(edres.dataset_description.parameters['dataset_path']),
                  'constraints': {'count': 6},
                  'max_records': 4,
                  'data_producer_id': sentinel.dprod_id,
                  'param_dictionary': sentinel.pdict,
                  'stream_def': sentinel.stream_def_id}

        my_dict = {}

        def setitem(name, val):
            my_dict[name] = val

        retval = MagicMock(spec=RecordDictionaryTool)
        retval.__setitem__ = Mock(side_effect=setitem)
        retval.to_granule.return_value = MagicMock(spec=Granule)
        RecordDictionaryTool_mock.return_value = retval

        for x in NetcdfDataHandler._get_data(config):
            self.assertTrue(isinstance(x, Granule))
            retval.to_granule.assert_any_call()

    def test__get_fingerprint(self):
        ds = Dataset('test_data/ncom.nc')
        retval = NetcdfDataHandler._get_fingerprint(ds)
        log.debug(retval)

    def test__compare_equal(self):
        base_fingerprint = ('9a2dcda4a8b8823881f14708c3328047cde6c176', {'dims': ('82aef4d2eb3355675c368f05b028a97b4d076974', {u'time': 'ac442c96b516911fdbf67d49e077645496cf4bb7'}), 'gbl_atts': ('614d96bff45c969c9cf08f9636bc67955985247f', {u'ion_geospatial_vertical_positive': '77346d0447daff959358a0ecbeec83bfd9ec86bb', u'ion_geospatial_lat_min': '48559c14f388bfa389e3bddd3f86fbfc55472b3e', u'source': 'b21cefcd86db619dc52f824849e8751f85d7847a', u'CF:featureType': '2da36ff30a38777c28fe7c9fd17f2a4982050cdc', u'ion_geospatial_lat_max': 'ac15063961ed183c6c7ba66c90483201602b4895', u'ion_geospatial_vertical_max': '38f6d7875e3195bdaee448d2cb6917f3ae4994af', u'Conventions': 'a1c3406199f676d4b90c7273b495fe00c47a5e5f', u'ion_time_coverage_start': '4bd0ba8e3cdacf91109d4b1b977ff9d3cbbc1d9c', u'references': '1e981ae63056fd89a5c93429779f42daeee25f53', u'ion_geospatial_lon_min': '588fb99431cb37aa5444328af1be09c4c8c94f61', u'NCO': '559001a22d744138e76553a04e52ead00c38650c', u'ion_time_coverage_end': 'b8cd51caffd68b595a77bf839bce8ae3ec37c6aa', u'title': '2a011784e88ce5332085af7fb2abaeede89aa721', u'ion_geospatial_lon_max': '588fb99431cb37aa5444328af1be09c4c8c94f61', u'institution': '1112e986b9cbe43dbd758f75c65a2c6fe9c50640', u'ion_geospatial_vertical_min': '38f6d7875e3195bdaee448d2cb6917f3ae4994af'}), 'vars': ('941d3ff0bdfb4d1fb9991f2f2363b66f0564c463', {u'streamflow': ('6710202fcfc74ebd64f9a979abe6a22b1c9dee54', {u'units': 'c329772c90300e531ee80bb19b18c50deb55dbcb', u'long_name': 'bbf1ade4a8b825901c20e884b815d1d684147c5f', u'standard_name': '906f870f9d69c17a115554a4f369e807eb2b73b7', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'}), u'lon': ('9139b25efd691deec60d870606c43efd5d109369', {u'units': 'f3333f58f05198b1ce9622350d52c1b6be65fedf', u'long_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0', u'standard_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0', u'_CoordinateAxisType': 'eb9297283a5a34ca7d2cff274e9ff4c0db8ddbc5'}), u'data_qualifier': ('2d1b57081def53fbd187a1aa97cbf9627e4055c4', {u'_FillValue': 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410c', u'flag_meanings': '457c49c797bc533a404955ae1842e237345a5247', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf', u'valid_range': 'aad1409b889ef360dad475dc32649f26d9df142a', u'long_name': '39ad280b35a4716efa8e46f597c355826d97757b', u'flag_values': 'aad1409b889ef360dad475dc32649f26d9df142a'}), u'specific_conductance': ('fff3ceebe90a2c95cc71335c6149f3ffde6408c7', {u'units': 'f177d83132385aba3b671086fead28e76eb775e0', u'long_name': '0646bfc3c2a9692e8df107f40b6c25bf72b29673', u'standard_name': 'd58675e27ed4039cd2385df6c9272eb8af8a3d9e', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'}), u'water_temperature_bottom': ('c90b36e64f40a3a1c08a95ca139e7769e1c6132d', {u'units': '86385633c95f56f924236571319fc39a4ab157bf', u'long_name': '46ec4f2f9bcb7e58d95a24dc034fe75de2630e73', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'}), u'time': ('5d4eb6e75f3990319bcd6aec0c8e445c05d506ef', {u'units': 'd779896565174fdacafb4c96fc70455a2ac7d826', u'long_name': '714eea0f4c980736bde0065fe73f573487f08e3a', u'standard_name': '714eea0f4c980736bde0065fe73f573487f08e3a', u'_CoordinateAxisType': '6c82e6dd86807ee3db07e3c82bec1ae1ce00b08b'}), u'stnId': ('2badd3674c0e6937f5ea45786f67b1b5b0e7bcde', {u'long_name': '3ec72d178ffa0ff9e78f8f645e55d1af43cbeec2', u'cf_role': '7e8d422307b3765fe973cfd567009274b02d3756'}), u'lat': ('a806cf5409ed3201819e5beca52bb04e1e7449e8', {u'units': '0f64995555efe141f90225e843501790654ae08c', u'long_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed', u'standard_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed', u'_CoordinateAxisType': '4b5152274022e4a3e476ccee4ce6ae0e0dfb1c9f'}), u'z': ('4f0c43b6d2fa144f2a28c5b0853ff6c0363676e7', {u'positive': '77346d0447daff959358a0ecbeec83bfd9ec86bb', u'long_name': '1ae7667dfa9dafd04883d07989e99c9da613bae8', u'standard_name': 'f82a8e8dd311d353948062cb1a0b67c9e9850be1', u'_CoordinateZisPositive': '77346d0447daff959358a0ecbeec83bfd9ec86bb', u'units': '6b0d31c0d563223024da45691584643ac78c96e8', u'_CoordinateAxisType': '3f608b4935ead643d43b2642dc4ec863d170aa1d', u'missing_value': 'e23fb30f847fda4fabf293091a78216f980e4c8e'}), u'water_temperature_middle': ('1d5bc255639aea10ba273827d354f1fc3b50233f', {u'units': '86385633c95f56f924236571319fc39a4ab157bf', u'long_name': '0132619eed74bdcfdf9e70920e97feff2d3b7317', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'}), u'water_temperature': ('1b1c792453f5ed44378e6ed4b813b0cf851bc232', {u'units': '86385633c95f56f924236571319fc39a4ab157bf', u'long_name': '4e470f31ee04784da9ca08953eeb1bd2a10152c9', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'})})})
        dh_config = {}
        nch = NetcdfDataHandler(dh_config=dh_config)
        retval = nch._compare(base_fingerprint=base_fingerprint, new_fingerprint=base_fingerprint)
        log.debug(retval)

    def test__compare_unequal(self):
        base_fingerprint = ('9a2dcda4a8b8823881f14708c3328047cde6c176', {'dims': ('82aef4d2eb3355675c368f05b028a97b4d076974', {u'time': 'ac442c96b516911fdbf67d49e077645496cf4bb7'}), 'gbl_atts': ('614d96bff45c969c9cf08f9636bc67955985247f', {u'ion_geospatial_vertical_positive': '77346d0447daff959358a0ecbeec83bfd9ec86bb', u'ion_geospatial_lat_min': '48559c14f388bfa389e3bddd3f86fbfc55472b3e', u'source': 'b21cefcd86db619dc52f824849e8751f85d7847a', u'CF:featureType': '2da36ff30a38777c28fe7c9fd17f2a4982050cdc', u'ion_geospatial_lat_max': 'ac15063961ed183c6c7ba66c90483201602b4895', u'ion_geospatial_vertical_max': '38f6d7875e3195bdaee448d2cb6917f3ae4994af', u'Conventions': 'a1c3406199f676d4b90c7273b495fe00c47a5e5f', u'ion_time_coverage_start': '4bd0ba8e3cdacf91109d4b1b977ff9d3cbbc1d9c', u'references': '1e981ae63056fd89a5c93429779f42daeee25f53', u'ion_geospatial_lon_min': '588fb99431cb37aa5444328af1be09c4c8c94f61', u'NCO': '559001a22d744138e76553a04e52ead00c38650c', u'ion_time_coverage_end': 'b8cd51caffd68b595a77bf839bce8ae3ec37c6aa', u'title': '2a011784e88ce5332085af7fb2abaeede89aa721', u'ion_geospatial_lon_max': '588fb99431cb37aa5444328af1be09c4c8c94f61', u'institution': '1112e986b9cbe43dbd758f75c65a2c6fe9c50640', u'ion_geospatial_vertical_min': '38f6d7875e3195bdaee448d2cb6917f3ae4994af'}), 'vars': ('941d3ff0bdfb4d1fb9991f2f2363b66f0564c463', {u'streamflow': ('6710202fcfc74ebd64f9a979abe6a22b1c9dee54', {u'units': 'c329772c90300e531ee80bb19b18c50deb55dbcb', u'long_name': 'bbf1ade4a8b825901c20e884b815d1d684147c5f', u'standard_name': '906f870f9d69c17a115554a4f369e807eb2b73b7', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'}), u'lon': ('9139b25efd691deec60d870606c43efd5d109369', {u'units': 'f3333f58f05198b1ce9622350d52c1b6be65fedf', u'long_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0', u'standard_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0', u'_CoordinateAxisType': 'eb9297283a5a34ca7d2cff274e9ff4c0db8ddbc5'}), u'data_qualifier': ('2d1b57081def53fbd187a1aa97cbf9627e4055c4', {u'_FillValue': 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410c', u'flag_meanings': '457c49c797bc533a404955ae1842e237345a5247', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf', u'valid_range': 'aad1409b889ef360dad475dc32649f26d9df142a', u'long_name': '39ad280b35a4716efa8e46f597c355826d97757b', u'flag_values': 'aad1409b889ef360dad475dc32649f26d9df142a'}), u'specific_conductance': ('fff3ceebe90a2c95cc71335c6149f3ffde6408c7', {u'units': 'f177d83132385aba3b671086fead28e76eb775e0', u'long_name': '0646bfc3c2a9692e8df107f40b6c25bf72b29673', u'standard_name': 'd58675e27ed4039cd2385df6c9272eb8af8a3d9e', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'}), u'water_temperature_bottom': ('c90b36e64f40a3a1c08a95ca139e7769e1c6132d', {u'units': '86385633c95f56f924236571319fc39a4ab157bf', u'long_name': '46ec4f2f9bcb7e58d95a24dc034fe75de2630e73', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'}), u'time': ('5d4eb6e75f3990319bcd6aec0c8e445c05d506ef', {u'units': 'd779896565174fdacafb4c96fc70455a2ac7d826', u'long_name': '714eea0f4c980736bde0065fe73f573487f08e3a', u'standard_name': '714eea0f4c980736bde0065fe73f573487f08e3a', u'_CoordinateAxisType': '6c82e6dd86807ee3db07e3c82bec1ae1ce00b08b'}), u'stnId': ('2badd3674c0e6937f5ea45786f67b1b5b0e7bcde', {u'long_name': '3ec72d178ffa0ff9e78f8f645e55d1af43cbeec2', u'cf_role': '7e8d422307b3765fe973cfd567009274b02d3756'}), u'lat': ('a806cf5409ed3201819e5beca52bb04e1e7449e8', {u'units': '0f64995555efe141f90225e843501790654ae08c', u'long_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed', u'standard_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed', u'_CoordinateAxisType': '4b5152274022e4a3e476ccee4ce6ae0e0dfb1c9f'}), u'z': ('4f0c43b6d2fa144f2a28c5b0853ff6c0363676e7', {u'positive': '77346d0447daff959358a0ecbeec83bfd9ec86bb', u'long_name': '1ae7667dfa9dafd04883d07989e99c9da613bae8', u'standard_name': 'f82a8e8dd311d353948062cb1a0b67c9e9850be1', u'_CoordinateZisPositive': '77346d0447daff959358a0ecbeec83bfd9ec86bb', u'units': '6b0d31c0d563223024da45691584643ac78c96e8', u'_CoordinateAxisType': '3f608b4935ead643d43b2642dc4ec863d170aa1d', u'missing_value': 'e23fb30f847fda4fabf293091a78216f980e4c8e'}), u'water_temperature_middle': ('1d5bc255639aea10ba273827d354f1fc3b50233f', {u'units': '86385633c95f56f924236571319fc39a4ab157bf', u'long_name': '0132619eed74bdcfdf9e70920e97feff2d3b7317', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'}), u'water_temperature': ('1b1c792453f5ed44378e6ed4b813b0cf851bc232', {u'units': '86385633c95f56f924236571319fc39a4ab157bf', u'long_name': '4e470f31ee04784da9ca08953eeb1bd2a10152c9', u'coordinates': '72b91e45da579f5802cb2759d3929e2310b9f8bf'})})})
        new_fingerprint = ('908aca9bfe5bb11083f4489d77fb1aaebbdf3f8f', {'dims': ('9dbcadf3ee61ea8e0e6f4f7e94ad24493ca1c12b', {u'lat': '4595b256c4603ab08605948c059b9937febb9e93', u'depth': '3ea9f5ba070adbca161ac02625c014d00e3f0b8d', u'lon': 'ef382d9aa648d2e66fd2cc0472a7a3838cad65f5', u'time': 'eb5de98efca8f825488c8152b81d8229825676ce'}), 'gbl_atts': ('9448ebb076ab953bc6972dd44e1434174143bdc0', {u'comment': '6eae3a5b062c6d0d79f070c26e6d62486b40cb46', u'distribution_statement': 'e85d8ab7d12ea6a73aa07fee0e4595e3b691f0df', u'operational_status': '05134426bcd175050b08193c02f14ccdafcca088', u'reference': '1238d1dd8d35b52d01e3f28c4720c349ed7aec47', u'time_origin': 'ba766656eb2e8b8ff02541697f7fb81281ecc2b6', u'message': 'fb6601061625e984f14f07c4d3ff232338d4c383', u'classification_level': 'fb6601061625e984f14f07c4d3ff232338d4c383', u'input_data_source': '95ee9e7d064e84c6fead1c577e5a92725f3cefbd', u'downgrade_date': 'b4d1e1bdf71f516f2f62b1fac060d67c732aa3c9', u'Conventions': '144593033f88263452d3418df81dd51506d03839', u'contact': '220cff7a17d28cf91a17c35b845a108fa9e7fb5c', u'NCO': '559001a22d744138e76553a04e52ead00c38650c', u'model_type': 'ec77db8e3ffae4f2ed8d254532c6ee9674c8f37c', u'classification_authority': 'b4d1e1bdf71f516f2f62b1fac060d67c732aa3c9', u'institution': '5bbc583db6912872ff381dd8a2327cccc5bfe542', u'generating_model': '0ce53ba33397949ec92f62c1f17a8db85b762466', u'history': '2497493ae227405dd55cdf7a093c1e287ae5b813'}), 'vars': ('7376ab4c4a0cfb9d2cda3765b9ea0ecba6d86811', {u'tau': ('3151f20d74e33a0772eaab1a4434f01393b3c094', {u'units': '2cf4bd4271f9c0b3c469c0dd761d9d44f97d95e0', u'long_name': '6fab4aaa30b8fad352e9db3a6ca333b4dc82fe8d', u'time_origin': 'f24a58c0da873a17fdd1cd5a0e0d4d2202994b11', u'NAVO_code': '54ceb91256e8190e474aa752a6e0650a2df5ba37'}), u'water_temp': ('5155a7d4a3ec9da3ad9397f99721715d4185a2c3', {u'_FillValue': 'fe8c28793485e0fe597030c1f757b10d88716f41', u'scale_factor': '7905963a484552d87c6ede4a2af6b5dcf3bc49af', u'add_offset': 'b8fc90fd7ade4838fc2405e4321d6b394d59f336', u'long_name': 'aab7b3cc7e90951ca444ce4e093e45905e797553', u'NAVO_code': 'f1abd670358e036c31296e66b3b66c382ac00812', u'units': '3fda22cc8a2ba51b65ca5ee8410023a11d3511e6', u'missing_value': 'fe8c28793485e0fe597030c1f757b10d88716f41'}), u'lon': ('23bca14cc1dde96fdb24c65c3c01ac4f574408dd', {u'units': 'ff371c19d6872ff49d09490ecbdffbe3e7af2be5', u'long_name': '6f9f011c5d36df6d431a68143636a75fbaa508bd', u'NAVO_code': 'da4b9237bacccdf19c0760cab7aec4a8359010b0'}), u'water_u': ('16846917a5e6dba7b0ce62c396817b3cfde6b4fa', {u'_FillValue': 'fe8c28793485e0fe597030c1f757b10d88716f41', u'scale_factor': '7905963a484552d87c6ede4a2af6b5dcf3bc49af', u'add_offset': '38f6d7875e3195bdaee448d2cb6917f3ae4994af', u'long_name': '48b3ebde2873bec968d6f1c6a9aece1906fa09bb', u'NAVO_code': '0716d9708d321ffb6a00818614779e779925365c', u'units': 'eade4f406fc4e29c65de29a5404e14296e962da0', u'missing_value': 'fe8c28793485e0fe597030c1f757b10d88716f41'}), u'salinity': ('7ea78cd2872ca2e3b75f3b2de58ea3f72a2135f8', {u'_FillValue': 'fe8c28793485e0fe597030c1f757b10d88716f41', u'scale_factor': '7905963a484552d87c6ede4a2af6b5dcf3bc49af', u'add_offset': 'b8fc90fd7ade4838fc2405e4321d6b394d59f336', u'long_name': 'ba8b96b506e59595922f3e7e6a900a9929ba5a7b', u'NAVO_code': '1574bddb75c78a6fd2251d61e2993b5146201319', u'units': '867230eae0f2ad45b63b9bba390b3e231ce0202f', u'missing_value': 'fe8c28793485e0fe597030c1f757b10d88716f41'}), u'water_v': ('58c66f0d777958e373ce35f20e7e66ff3e019dee', {u'_FillValue': 'fe8c28793485e0fe597030c1f757b10d88716f41', u'scale_factor': '7905963a484552d87c6ede4a2af6b5dcf3bc49af', u'add_offset': '38f6d7875e3195bdaee448d2cb6917f3ae4994af', u'long_name': '20e9b6ca2a56570d0b1d4d6f647e26619cc85902', u'NAVO_code': '9e6a55b6b4563e652a23be9d623ca5055c356940', u'units': 'eade4f406fc4e29c65de29a5404e14296e962da0', u'missing_value': 'fe8c28793485e0fe597030c1f757b10d88716f41'}), u'depth': ('ed31179dcd786fde972275d19794fbf1ca34dc14', {u'units': '17272b9d25ea510ed0220773d423b3b29935dfe1', u'long_name': 'df0e298e356261439795d0d435ce0ce576604c65', u'positive': '77346d0447daff959358a0ecbeec83bfd9ec86bb', u'NAVO_code': 'ac3478d69a3c81fa62e60f5c3696165a4e5e6ac4'}), u'time': ('ed374ac30a2c02a82d53aa7aabaf4096b06194aa', {u'units': 'd2158b57a120696c0ec6946b1a5944e91397d5d3', u'long_name': 'c0fb336660e4dafc5fd132087813b901d200fff9', u'time_origin': '4e96ed6a4b3727d11bc643526f43625c1b00a792', u'NAVO_code': 'bd307a3ec329e10a2cff8fb87480823da114f8f4'}), u'lat': ('8976bd1ea070825de42cd27a232e5d363159f153', {u'units': '84f9eb7a658bfac37e07152c3ea75548fee6f512', u'long_name': 'ced3905ec8220a51e0d47124fc2d0549ce7c8472', u'NAVO_code': '356a192b7913b04c54574d18c28d46e6395428ab'}), u'surf_el': ('0ab0703890ad7f457e5f4a91541a0829ffa6f874', {u'_FillValue': 'fe8c28793485e0fe597030c1f757b10d88716f41', u'scale_factor': '7905963a484552d87c6ede4a2af6b5dcf3bc49af', u'positive': '7c0a25c06ea30bae50e39a37a5997e31a1a96e20', u'add_offset': '38f6d7875e3195bdaee448d2cb6917f3ae4994af', u'long_name': '819f27b921fe544916844888bdb92ee00d4fae51', u'NAVO_code': 'cb4e5208b4cd87268b208e49452ed6e89a68e0b8', u'units': '17272b9d25ea510ed0220773d423b3b29935dfe1', u'missing_value': 'fe8c28793485e0fe597030c1f757b10d88716f41'})})})
        dh_config = {}
        nch = NetcdfDataHandler(dh_config=dh_config)
        retval = nch._compare(base_fingerprint=base_fingerprint, new_fingerprint=new_fingerprint)
        log.debug(retval)
