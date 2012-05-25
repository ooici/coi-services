__author__ = 'timgiguere'
__author__ = 'cmueller'

import tempfile
import os
from pyon.util.int_test import IonIntegrationTestCase
from ion.agents.eoi.data_acquisition_management_service_Placeholder import *
from netCDF4 import Dataset
from nose.plugins.attrib import attr
from ion.agents.eoi.handler.dap_external_data_handler import DapExternalDataHandler
from ion.agents.eoi.handler.base_external_data_handler import DataAcquisitionError, InstantiationError
from pyon.util.unit_test import PyonTestCase
from interface.objects import ExternalDataset, DataSource, DatasetDescription, UpdateDescription, ContactInformation, ExternalDataRequest, DatasetDescriptionDataSamplingEnum, CompareResult, CompareResultEnum
import unittest
import numpy
from numpy import array


@attr('UNIT_EOI_DEPRECATED', group='eoi')
class TestDapExternalDataHandler(PyonTestCase):

    #_dsh_list = {}
    #_ds_base_sig = None
    _ten_x_ten_x_ten = None
    _twelve_x_ten_x_ten = None

    def setUp(self):
        self._ds_base_sig = ('89fc34cc894ccde2ae83b71ed60d1f8bf7705b75',
                                 {'dims': ('b1c1a3fbba162ac0ccfd0af05fd9673adfd41ea9',
                                    {u'lat': 'ce1d497b71e7ef0c4b43eaffbade5073dbc450ee',
                                     u'lon': '2a4e0de4e2c037a94aa75cdf59dfb5b1f2fddcc5',
                                     u'time': '7d0730bff4ef5dcf6034f5b7b5285b6fa9422816'}),
                                  'gbl_atts': ('09a13128e45c6e1909c4b762bdd0d84b9736cfbe',
                                    {u'history': '70d1ff82dbc7cc7a0972f67ab75ce93a184f160f',
                                     u'creator': '3c5b4941b7b4594facec9eb943e4d3c5401d882a'}),
                                  'vars': ('cfd99a46e8d2bb7d290beb79808d995337b2265f',
                                    {u'doubledata': ('e30671e4f23c46537916f211c8c03d5021239197',
                                        {u'units': '4defa67e8d027b1d564101917f3af00f0ba4fcde',
                                         u'long_name': '9666f26ef1d4133286ac1720c23ab6d79e732a57',
                                         u'standard_name': '6915b14cc28d7c49e0226fb79d98502e97f6b97f'}),
                                     u'bytedata': ('c4efaeabdf390223c2ab090a6ed4228d56d21705',
                                        {u'units': '8cf1783fa99f62ca581f6fe8f3cd66b0f9ab9fc3',
                                         u'long_name': '43b5048d58f03a15ba415ae6bb986c592ac21d8f',
                                         u'standard_name': 'daf529a73101c2be626b99fc6938163e7a27620b'}),
                                     u'floatdata': ('3d5d26c312e0a0f18c07ba48780cede9fe2a2124',
                                        {u'units': 'ee51183217c223eb1db757b4d9c3a411579b61c0',
                                         u'long_name': '003fa881fcb9241c326210094f6295c837309470',
                                         u'standard_name': 'c37c8fd2a2acf45d254c0aaf2fe984e5d0f25341'}),
                                     u'longitude': ('4812c8a8a7814777a5bc9d8bcaa04a83e929deae',
                                        {u'units': 'ff371c19d6872ff49d09490ecbdffbe3e7af2be5',
                                         u'long_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0',
                                         u'standard_name': 'd2a773ae817d7d07c19d9e37be4e792cec37aff0'}),
                                     u'time': ('036112bc95c35ba8ccff3ae54d77e76c881ad1af',
                                        {u'comment': 'd4bd716c721dd666ab529575d2ccaea4f0867dd9',
                                         u'_FillValue': '7984b0a0e139cabadb5afc7756d473fb34d23819',
                                         u'long_name': '714eea0f4c980736bde0065fe73f573487f08e3a',
                                         u'standard_name': '714eea0f4c980736bde0065fe73f573487f08e3a',
                                         u'units': 'd779896565174fdacafb4c96fc70455a2ac7d826',
                                         u'calendar': '4a78d3cb314a4e97cfe37eda5781f60b87f6145e'}),
                                     u'latitude': ('fa639066e2cbff9006c7abffce0b2f2d4be74a5f',
                                        {u'units': '84f9eb7a658bfac37e07152c3ea75548fee6f512',
                                         u'long_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed',
                                         u'standard_name': '5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed'}),
                                     u'intdata': ('c3b88b83ca2d88d1a920c446bde84b6c4ea432cb',
                                        {u'units': '46f8ab7c0cff9df7cd124852e26022a6bf89e315',
                                         u'long_name': '789e7e9f23bda5cc8fcd5f68778f0a91baf44b6c',
                                         u'standard_name': '9e35a694eb0354c94b54f2d71f780e0007fa0a31'}),
                                     u'shortdata': ('8233872bc0219895b8e4357eefd1b9b76e553e1f',
                                        {u'units': 'a0f4ea7d91495df92bbac2e2149dfb850fe81396',
                                         u'long_name': '8de90e7c61efae549eeffabb0a7dc7ec55ca2c14',
                                         u'standard_name': '9e32d15b246e504af33cd10eb2ad2222215746fc'})})})

        self._ds1_repr = "\n>> ExternalDataProvider:\n" \
                         "None\n>> DataSource:\nNone\n***\n" \
                         "dataset:\n<type 'netCDF4.Dataset'>\n" \
                         "root group (NETCDF4 file format):\n" \
                         "    creator: ocean observing initiative\n" \
                         "    history: first history entry\n" \
                         "    dimensions = (u'time', u'lat', u'lon')\n" \
                         "    variables = (u'time', u'latitude', u'longitude', u'bytedata'," \
                         " u'shortdata', u'intdata', u'floatdata', u'doubledata')\n" \
                         "    groups = ()\n\ntime_var: <type 'netCDF4.Variable'>\n" \
                         "int64 time(u'time',)\n    _FillValue: -1\n" \
                         "    standard_name: time\n" \
                         "    long_name: time\n" \
                         "    units: seconds since 1970-01-01 00:00:00\n" \
                         "    calendar: gregorian\n" \
                         "    comment: estimated time of observation\n" \
                         "unlimited dimensions = (u'time',)\n" \
                         "current size = (10,)\n\n" \
                         "dataset_signature(sha1): 0e0bd865d4cd42c840b55f273499f62e5c32d4ff\n" \
                         "\tdims: 9e86329cc35148d33f94da5d9a45c4996b8ff756\n" \
                         "\t\tlat: 17b0bff9fd5da05a17e27478f72d39216546eafa\n" \
                         "\t\tlon: f115170f91587fb723e33ac1564eb021615acfaa\n" \
                         "\t\ttime: 165058f1f201a8e9e455687940ce890121dd7853\n" \
                         "\tgbl_atts: 09a13128e45c6e1909c4b762bdd0d84b9736cfbe\n" \
                         "\t\thistory: 70d1ff82dbc7cc7a0972f67ab75ce93a184f160f\n" \
                         "\t\tcreator: 3c5b4941b7b4594facec9eb943e4d3c5401d882a\n" \
                         "\tvars: cfd99a46e8d2bb7d290beb79808d995337b2265f\n" \
                         "\t\tdoubledata: e30671e4f23c46537916f211c8c03d5021239197\n" \
                         "\t\t\tunits: 4defa67e8d027b1d564101917f3af00f0ba4fcde\n" \
                         "\t\t\tlong_name: 9666f26ef1d4133286ac1720c23ab6d79e732a57\n" \
                         "\t\t\tstandard_name: 6915b14cc28d7c49e0226fb79d98502e97f6b97f\n" \
                         "\t\tbytedata: c4efaeabdf390223c2ab090a6ed4228d56d21705\n" \
                         "\t\t\tunits: 8cf1783fa99f62ca581f6fe8f3cd66b0f9ab9fc3\n" \
                         "\t\t\tlong_name: 43b5048d58f03a15ba415ae6bb986c592ac21d8f\n" \
                         "\t\t\tstandard_name: daf529a73101c2be626b99fc6938163e7a27620b\n" \
                         "\t\tfloatdata: 3d5d26c312e0a0f18c07ba48780cede9fe2a2124\n" \
                         "\t\t\tunits: ee51183217c223eb1db757b4d9c3a411579b61c0\n" \
                         "\t\t\tlong_name: 003fa881fcb9241c326210094f6295c837309470\n" \
                         "\t\t\tstandard_name: c37c8fd2a2acf45d254c0aaf2fe984e5d0f25341\n" \
                         "\t\tlongitude: 4812c8a8a7814777a5bc9d8bcaa04a83e929deae\n" \
                         "\t\t\tunits: ff371c19d6872ff49d09490ecbdffbe3e7af2be5\n" \
                         "\t\t\tlong_name: d2a773ae817d7d07c19d9e37be4e792cec37aff0\n" \
                         "\t\t\tstandard_name: d2a773ae817d7d07c19d9e37be4e792cec37aff0\n" \
                         "\t\ttime: 036112bc95c35ba8ccff3ae54d77e76c881ad1af\n" \
                         "\t\t\tcomment: d4bd716c721dd666ab529575d2ccaea4f0867dd9\n" \
                         "\t\t\t_FillValue: 7984b0a0e139cabadb5afc7756d473fb34d23819\n" \
                         "\t\t\tlong_name: 714eea0f4c980736bde0065fe73f573487f08e3a\n" \
                         "\t\t\tstandard_name: 714eea0f4c980736bde0065fe73f573487f08e3a\n" \
                         "\t\t\tunits: d779896565174fdacafb4c96fc70455a2ac7d826\n" \
                         "\t\t\tcalendar: 4a78d3cb314a4e97cfe37eda5781f60b87f6145e\n" \
                         "\t\tlatitude: fa639066e2cbff9006c7abffce0b2f2d4be74a5f\n" \
                         "\t\t\tunits: 84f9eb7a658bfac37e07152c3ea75548fee6f512\n" \
                         "\t\t\tlong_name: 5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed\n" \
                         "\t\t\tstandard_name: 5fcccdcf1d079c4a85c92c6fe7c8d29a27e49bed\n" \
                         "\t\tintdata: c3b88b83ca2d88d1a920c446bde84b6c4ea432cb\n" \
                         "\t\t\tunits: 46f8ab7c0cff9df7cd124852e26022a6bf89e315\n" \
                         "\t\t\tlong_name: 789e7e9f23bda5cc8fcd5f68778f0a91baf44b6c\n" \
                         "\t\t\tstandard_name: 9e35a694eb0354c94b54f2d71f780e0007fa0a31\n" \
                         "\t\tshortdata: 8233872bc0219895b8e4357eefd1b9b76e553e1f\n" \
                         "\t\t\tunits: a0f4ea7d91495df92bbac2e2149dfb850fe81396\n" \
                         "\t\t\tlong_name: 8de90e7c61efae549eeffabb0a7dc7ec55ca2c14\n" \
                         "\t\t\tstandard_name: 9e32d15b246e504af33cd10eb2ad2222215746fc\n"

        lst = ["DS_BASE", "DS_BASE_DUP", "DS_DIM_SIZE_CHANGED", "DS_VAR_ATT_CHANGED", "DS_GLOBAL_ATT_CHANGED", "DS_ADDITIONAL_TIMES", "DS_TIME_DIM_VAR_DIFFER"]
        self._dsh_list = {}
        for key in lst:
            self._create_tst_data_set_handler(key=key)

        self.timedataname = "time"
        self.londataname = "longitude"
        self.latdataname = "latitude"
        self.bytedataname = "bytedata"
        self.shortdataname = "shortdata"
        self.intdataname = "intdata"
        self.floatdataname = "floatdata"
        self.doubledataname = "doubledata"
        self.vlist = [self.timedataname, self.londataname, self.latdataname, self.bytedataname, self.shortdataname,
                      self.intdataname, self.floatdataname, self.doubledataname]

    def tearDown(self):
        for key in self._dsh_list:
            os.remove(self._dsh_list[key][1])
        pass

    def _create_tst_data_set(self, tmp_handle, tmp_name, key):
        ds = Dataset(tmp_name, 'w')
        time = ds.createDimension('time', None)
        lat = ds.createDimension('lat', 80)
        lon_len = 60
        if key is "DS_DIM_SIZE_CHANGED":
            lon_len = 70

        time_len = 10
        if key is "DS_ADDITIONAL_TIMES":
            time_len = 12

        lon = ds.createDimension('lon', lon_len)

        ds.creator = "ocean observing initiative"
        if key is "DS_GLOBAL_ATT_CHANGED":
            ds.history = "first history entry; second history entry"
        else:
            ds.history = "first history entry"

        if key is "DS_TIME_DIM_VAR_DIFFER":
            time = ds.createVariable('record', 'i8', ('time',), fill_value=-1)
        else:
            time = ds.createVariable('time', 'i8', ('time',), fill_value=-1)
        time.standard_name = "time"
        time.long_name = "time"
        time.units = "seconds since 1970-01-01 00:00:00"
        time.calendar = "gregorian"
        time.comment = "estimated time of observation"

        latitudes = ds.createVariable('latitude', 'f4', ('lat',))
        latitudes.standard_name = "latitude"
        if key is "DS_VAR_ATT_CHANGED":
            latitudes.long_name = "latitude has changed"
        else:
            latitudes.long_name = "latitude"
        latitudes.units = "degrees_north"
        latitudes[:] = numpy.arange(10, 50, 0.5)

        longitudes = ds.createVariable('longitude', 'f4', ('lon',))
        longitudes.standard_name = "longitude"
        longitudes.long_name = "longitude"
        longitudes.units = "degrees_east"
        if key is "DS_DIM_SIZE_CHANGED":
            longitudes[:] = numpy.arange(-90, -55, 0.5)
        else:
            longitudes[:] = numpy.arange(-90, -60, 0.5)

        byte_data = ds.createVariable('bytedata', 'i1', ('time', 'lat', 'lon',))
        byte_data.standard_name = "bytes"
        byte_data.long_name = "byte_data"
        byte_data.units = "byte"

        array_data = numpy.array(numpy.arange(0, time_len * len(ds.dimensions["lat"]) * len(ds.dimensions["lon"])))
        byte_data[0:time_len, :, :] = numpy.reshape(array_data, (time_len, len(ds.dimensions["lat"]), len(ds.dimensions["lon"])))

        short_data = ds.createVariable('shortdata', 'i2', ('time', 'lat', 'lon',))
        short_data.standard_name = "shorts"
        short_data.long_name = "short_data"
        short_data.units = "short"
        short_data[0:time_len, :, :] = numpy.reshape(array_data, (time_len, len(ds.dimensions["lat"]), len(ds.dimensions["lon"])))

        int_data = ds.createVariable('intdata', 'i4', ('time', 'lat', 'lon',))
        int_data.standard_name = "integers"
        int_data.long_name = "integer_data"
        int_data.units = "int"
        int_data[0:time_len, :, :] = numpy.reshape(array_data, (time_len, len(ds.dimensions["lat"]), len(ds.dimensions["lon"])))

        array_data = numpy.linspace(start=0, stop=10000, num=time_len * len(ds.dimensions["lat"]) * len(ds.dimensions["lon"]))

        float_data = ds.createVariable('floatdata', 'f4', ('time', 'lat', 'lon',))
        float_data.standard_name = "floats"
        float_data.long_name = "float_data"
        float_data.units = "flt"
        float_data[0:time_len, :, :] = numpy.reshape(array_data, (time_len, len(ds.dimensions["lat"]), len(ds.dimensions["lon"])))

        double_data = ds.createVariable('doubledata', 'f8', ('time', 'lat', 'lon',))
        double_data.standard_name = "doubles"
        double_data.long_name = "double_data"
        double_data.units = "dbl"
        double_data[0:time_len, :, :] = numpy.reshape(array_data, (time_len, len(ds.dimensions["lat"]), len(ds.dimensions["lon"])))

        # fill in the temporal data
        from datetime import datetime, timedelta
        from netCDF4 import  date2num
        dates = [datetime(2011, 12, 1) + n * timedelta(hours=1) for n in range(float_data.shape[0])]
        time[:] = date2num(dates, units=time.units, calendar=time.calendar)

        ds.close()

        os.close(tmp_handle)

    def _create_tst_data_set_handler(self, key="DS_BASE"):
        th, tn = tempfile.mkstemp(prefix=(key + "_"), suffix='.nc')
        self._create_tst_data_set(th, tn, key)

        ext_ds = ExternalDataset(name="test", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = tn
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'

        dsh = DapExternalDataHandler(ext_dataset=ext_ds)

        self._dsh_list[key] = dsh, tn

    #### Test Methods

    def test_constructor(self):
        ext_ds = ExternalDataset(name="test", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = self._dsh_list["DS_BASE"][1]
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'

        dsh = DapExternalDataHandler(ext_dataset=ext_ds)
        self.assertTrue(type(dsh), DapExternalDataHandler)

        dsrc = DataSource(name="test")
        dsrc.connection_params["base_data_url"] = ""

        dsh1 = DapExternalDataHandler(data_source=dsrc, ext_dataset=ext_ds)
        self.assertTrue(type(dsh1), DapExternalDataHandler)

        dsh2 = DapExternalDataHandler(data_source=dsrc, ext_dataset=ext_ds, BLOCK_SIZE=10)
        self.assertEqual(dsh2._block_size, 10)

    def test_constructor_exception(self):
        with self.assertRaises(InstantiationError) as cm:
            obj = DapExternalDataHandler(ext_dataset=None)

        ex = cm.exception
        self.assertEqual(ex.message, "Invalid DatasetHandler: ExternalDataset resource cannot be 'None'")

    @unittest.skip("differs on buildbot")
    def test_get_fingerprint(self):
        dsh = self._dsh_list["DS_BASE"][0]
        ## Tests the case where the fingerprint is force-recalculated
        fingerprint = dsh.get_fingerprint(recalculate=True)
        ## Uncomment this line when the guts of "get_fingerprint" has changed to print the new "correct" value - replace "self._ds_base_sig" with the output
        self.assertEqual(fingerprint, self._ds_base_sig)

        ## Tests the case where the fingerprint has already been calculated
        fingerprint = dsh.get_fingerprint()

        dsh._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.NONE
        fingerprint = dsh.get_fingerprint(recalculate=True)
        self.assertEqual(fingerprint, self._ds_base_sig)

    def test_compare_equal_no_data(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        dsh_2 = self._dsh_list["DS_BASE_DUP"][0]

        dcr = dsh_1.compare(dsh_2.get_fingerprint())
        for x in dcr:
            self.assertEqual(x.difference, CompareResultEnum.EQUAL)

    @unittest.skip("Nothing to test yet")
    def test_compare_data_equal_first_last(self):
        pass

    @unittest.skip("Nothing to test yet")
    def test_compare_data_equal_full(self):
        pass

    def test_compare_data_different_first_last(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST

        dsh_2 = self._dsh_list["DS_BASE_DUP"][0]
        dsh_2._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST

        dcr = dsh_1.compare(dsh_2.get_fingerprint())
        for x in dcr:
            self.assertEqual(x.difference, CompareResultEnum.EQUAL)

    def test_compare_data_different_full(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL

        dsh_2 = self._dsh_list["DS_BASE_DUP"][0]
        dsh_2._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL

        dcr = dsh_1.compare(dsh_2.get_fingerprint())
        for x in dcr:
            self.assertEqual(x.difference, CompareResultEnum.EQUAL)

    #TODO: Make test for shotgun sampling (must implement support for shotgun sampling first)

    def test_compare_global_attribute_changed(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        dsh_2 = self._dsh_list["DS_GLOBAL_ATT_CHANGED"][0]

        dcr = dsh_1.compare(dsh_2.get_fingerprint())
        for x in dcr:
            self.assertEqual(x.difference, CompareResultEnum.MOD_GATT)

    def test_compare_dim_size_changed(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        dsh_2 = self._dsh_list["DS_DIM_SIZE_CHANGED"][0]

        dcr = dsh_1.compare(dsh_2.get_fingerprint())
        for x in dcr:
            self.assertEqual(x.difference, CompareResultEnum.MOD_DIM)

    def test_get_attributes_global(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        atts = dsh_1.get_attributes()
        self.assertEqual(atts, {"creator": "ocean observing initiative", "history": "first history entry"})

    def test_get_attributes_variable(self, scope="floatdata"):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        atts = dsh_1.get_attributes(scope=scope)
        self.assertEqual(atts, {"standard_name": "floats", "long_name": "float_data", "units": "flt"})

    @unittest.skip("")
    def test_base_handler_repr(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        self.assertEqual(str(dsh_1), self._ds1_repr)

    @unittest.skip("differs on buildbot")
    def test_has_data_changed_false(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        self.assertFalse(dsh_1.has_data_changed(self._ds_base_sig))

    def test_has_data_changed_true(self):
        dsh_1 = self._dsh_list["DS_ADDITIONAL_TIMES"][0]

        self.assertTrue(dsh_1.has_data_changed(self._ds_base_sig))

    def test_has_data_changed_initial(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        # Tests when last_fingerprint is an empty string
        res = dsh_1.has_data_changed('')
        self.assertTrue(res)

        res = dsh_1.has_data_changed(None)
        self.assertTrue(res)

    def test_has_new_data_false(self):
        # 'Old' times are the same as the current set
        timesteps = array([1322697600, 1322701199, 1322704800, 1322708400, 1322711999, 1322715600,
                           1322719200, 1322722799, 1322726400, 1322730000])
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._ext_dataset_res.update_description.parameters['new_data_check'] = timesteps
        res = dsh_1.has_new_data()
        self.assertFalse(res)

    def test_has_new_data_false_windowed(self):
        # Extra time at the beginning of the 'old' array
        timesteps = array([1322694001, 1322697600, 1322701199, 1322704800, 1322708400, 1322711999, 1322715600,
                           1322719200, 1322722799, 1322726400, 1322730000])
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._ext_dataset_res.update_description.parameters['new_data_check'] = timesteps
        res = dsh_1.has_new_data()
        self.assertFalse(res)

    def test_has_new_data_true(self):
        # Fewer times in 'old' array than current
        timesteps = array([1322697600, 1322701199, 1322704800, 1322708400, 1322711999, 1322715600])
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._ext_dataset_res.update_description.parameters['new_data_check'] = timesteps
        res = dsh_1.has_new_data()
        self.assertTrue(res)

    def test_has_new_data_true_windowed(self):
        # Fewer times in 'old' array than current and additional time at the beginning of 'old' array
        timesteps = array([1322694001, 1322697600, 1322701199, 1322704800, 1322708400, 1322711999, 1322715600])
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._ext_dataset_res.update_description.parameters['new_data_check'] = timesteps
        res = dsh_1.has_new_data()
        self.assertTrue(res)

    @unittest.skip("Needs refactoring -> ExternalDataRequest properties are wrong")
    def test_acquire_data_by_request_multidim_byte(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = ExternalDataRequest()
        req.name="bytedata"
        req.slice=(slice(0), slice(0, 10), slice(0, 10))

        name, data, typecode, dims, attrs = dsh_1.acquire_data_by_request(request=req)
        #        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))

        self.assertEqual(name, "bytedata")
        self.assertEqual(typecode, 'b')
        self.assertEqual(dims, (u'time', u'lat', u'lon'))
        self.assertEqual(attrs, {u'units': u'byte', u'long_name': u'byte_data', u'standard_name': u'bytes'})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        lt_1 = arr < 129
        gt_0 = arr > -129
        self.assertTrue(lt_1.min(), lt_1.max())
        self.assertTrue(gt_0.min(), gt_0.max())

    @unittest.skip("Needs refactoring -> ExternalDataRequest properties are wrong")
    def test_acquire_data_by_request_multidim_float(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = ExternalDataRequest()
        req.name="floatdata"
        req.slice=(slice(0), slice(0, 10), slice(0, 10))

        name, data, typecode, dims, attrs = dsh_1.acquire_data_by_request(request=req)
#        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))

        self.assertEqual(name, "floatdata")
        self.assertEqual(typecode, 'f')
        self.assertEqual(dims, (u'time', u'lat', u'lon'))
        self.assertEqual(attrs, {u'units': u'flt', u'long_name': u'float_data', u'standard_name': u'floats'})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        lt_1 = arr <= 10000
        gt_0 = arr >= 0
        self.assertTrue(lt_1.min(), lt_1.max())
        self.assertTrue(gt_0.min(), gt_0.max())

    @unittest.skip("Needs refactoring -> ExternalDataRequest properties are wrong")
    def test_acquire_data_by_request_multidim_double(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = ExternalDataRequest()
        req.name="doubledata"
        req.slice=(slice(0), slice(0, 10), slice(0, 10))

        name, data, typecode, dims, attrs = dsh_1.acquire_data_by_request(request=req)

        self.assertEqual(name, "doubledata")
        self.assertEqual(typecode, 'd')
        self.assertEqual(dims, (u'time', u'lat', u'lon'))
        self.assertEqual(attrs, {u'units': u'dbl', u'long_name': u'double_data', u'standard_name': u'doubles'})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        print arr
        lt_1 = arr <= 10000
        gt_0 = arr >= 0
        self.assertTrue(lt_1.min(), lt_1.max())
        self.assertTrue(gt_0.min(), gt_0.max())

    @unittest.skip("Needs refactoring -> ExternalDataRequest properties are wrong")
    def test_acquire_data_by_request_multidim_int(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = ExternalDataRequest()
        req.name="intdata"
        req.slice=(slice(0), slice(0, 10), slice(0, 10))

        name, data, typecode, dims, attrs = dsh_1.acquire_data_by_request(request=req)
        #        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))

        self.assertEqual(name, "intdata")
        self.assertEqual(typecode, 'i')
        self.assertEqual(dims, (u'time', u'lat', u'lon'))
        self.assertEqual(attrs, {u'units': u'int', u'long_name': u'integer_data', u'standard_name': u'integers'})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        lt_1 = arr < 2147483647
        gt_0 = arr > -2147483647
        self.assertTrue(lt_1.min(), lt_1.max())
        self.assertTrue(gt_0.min(), gt_0.max())

    @unittest.skip("Needs refactoring -> ExternalDataRequest properties are wrong")
    def test_acquire_data_by_request_multidim_short(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = ExternalDataRequest()
        req.name="shortdata"
        req.slice=(slice(0), slice(0, 10), slice(0, 10))

        name, data, typecode, dims, attrs = dsh_1.acquire_data_by_request(request=req)
        #        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))

        self.assertEqual(name, "shortdata")
        self.assertEqual(typecode, 'h')
        self.assertEqual(dims, (u'time', u'lat', u'lon'))
        self.assertEqual(attrs, {u'units': u'short', u'long_name': u'short_data', u'standard_name': u'shorts'})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        lt_1 = arr < 32767
        gt_0 = arr > -32767
        self.assertTrue(lt_1.min(), lt_1.max())
        self.assertTrue(gt_0.min(), gt_0.max())

    @unittest.skip("Needs refactoring -> ExternalDataRequest properties are wrong")
    def test_acquire_data_by_request_onedim(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = ExternalDataRequest()
        req.name="longitude"
        req.slice=(slice(0, 10))

        name, data, typecode, dims, attrs = dsh_1.acquire_data_by_request(request=req)
#        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))
        self.assertEqual(name, "longitude")
        self.assertEqual(typecode, 'f')
        self.assertEqual(dims, (u'lon',))
        self.assertEqual(attrs, {u'units': u'degrees_east', u'long_name': u'longitude', u'standard_name': u'longitude'})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        self.assertTrue(numpy.array_equiv(arr, numpy.arange(-90, -85, 0.5, dtype='float32')))

    @unittest.skip("Needs refactoring -> ExternalDataRequest properties are wrong")
    def test_acquire_data_by_request_with_dim_no_var(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = ExternalDataRequest()
        req.name="lon"
        req.slice=(slice(0, 10))

        name, data, typecode, dims, attrs = dsh_1.acquire_data_by_request(request=req)
#        raise StandardError(str(name) + "\n" + str(data) + "\n" + str(typecode) + "\n" + str(dims) + "\n" + str(attrs))
        self.assertEqual(name, "lon")
        self.assertEqual(typecode, 'l')
        self.assertEqual(dims, ('lon',))
        self.assertEqual(attrs, {})
        import numpy
        arr = numpy.asarray(data).flat.copy()
        self.assertTrue(numpy.array_equiv(arr, numpy.arange(0, 10)))

    @unittest.skip("Needs refactoring -> ExternalDataRequest properties are wrong")
    def test_acquire_data_by_request_with_no_dim_or_var(self):
        dsh_1 = self._dsh_list["DS_BASE"][0]

        req = ExternalDataRequest()
        req.name="no_var_or_dim_with_this_name"
        req.slice=(slice(0, 10))

        with self.assertRaises(DataAcquisitionError) as cm:
            dsh_1.acquire_data_by_request(request=req)

        ex = cm.exception
        self.assertEqual(ex.message, "The name 'no_var_or_dim_with_this_name' does not appear to be available in the dataset, cannot acquire data")

    def test_acquire_data_full(self):
        import numpy
        dsh_1 = self._dsh_list["DS_BASE"][0]
        # set the block size such that all of the floatdata var can be returned at once (max == 80*60*10)
        dsh_1._block_size = 48000

        locallist = self.vlist
        data_iter = dsh_1.acquire_data()
        for vn, slice_, rng, data in data_iter:
            self.assertTrue(vn in locallist)
            locallist.pop(locallist.index(vn))
            self.assertTrue(isinstance(slice_, tuple))
            self.assertTrue(isinstance(rng, tuple))
            self.assertTrue(isinstance(data, numpy.ndarray))

        self.assertTrue(len(locallist) == 0)

    def test_acquire_data_float_by_9600(self):
        import numpy
        dsh_1 = self._dsh_list["DS_BASE"][0]
        # set the block size such that the temp var data is returned in 4 chunks (80*60*10 / 4)
        dsh_1._block_size = 9600
        data_iter = dsh_1.acquire_data("floatdata")
#        for vn, slice_, data in enumerate(data_iter):
        for count, ret in enumerate(data_iter):
            self.assertTrue(ret[0] is "floatdata")
            self.assertEqual(len(ret[1]), 3)
            self.assertEqual(len(ret[2]), 2)
            self.assertTrue(isinstance(ret[3], numpy.ndarray))

        self.assertEqual(count, 4)

    #TODO: Need many more acquire_data tests

    def test_acquire_new_data(self):
        import numpy
        dsh_1 = self._dsh_list["DS_BASE"][0]
        # set the block size such that all of the floatdata var can be returned at once (max == 80*60*10)
        dsh_1._block_size = 48000

        locallist = self.vlist
        data_iter = dsh_1.acquire_new_data()
        for vn, slice_, rng, data in data_iter:
            self.assertTrue(vn in locallist)
            locallist.pop(locallist.index(vn))
            self.assertTrue(isinstance(slice_, tuple))
            self.assertTrue(isinstance(rng, tuple))
            self.assertTrue(isinstance(data, numpy.ndarray))

        self.assertTrue(len(locallist) == 0)

    def test_find_time_axis_specified(self):
        import netCDF4
        dsh_1 = self._dsh_list["DS_BASE"][0]

        tvar = dsh_1.find_time_axis()
        self.assertTrue(isinstance(tvar, netCDF4.Variable))

    def test_find_time_axis_specified_var_dim_differ(self):
        import netCDF4
        dsh_1 = self._dsh_list["DS_TIME_DIM_VAR_DIFFER"][0]

        tvar = dsh_1.find_time_axis()
        self.assertTrue(isinstance(tvar, netCDF4.Variable))

    def test_find_time_axis_unknown(self):
        import netCDF4
        dsh_1 = self._dsh_list["DS_BASE"][0]
        dsh_1._ext_dataset_res.dataset_description.parameters["temporal_dimension"] = ""

        tvar = dsh_1.find_time_axis()
        self.assertTrue(isinstance(tvar, netCDF4.Variable))

    def test_pprint_fingerprint(self):
        fingerprint = self._dsh_list['DS_BASE'][0]._pprint_fingerprint()
        print type(fingerprint)
        self.assertTrue(type(fingerprint) in [str, unicode])

    def test_scan(self):
        print 'test_scan'
        scan_results = self._dsh_list["DS_BASE"][0].scan()
        self.assertTrue('variables' in scan_results)
        self.assertTrue('attributes' in scan_results)
        self.assertTrue('dimensions' in scan_results)
        self.assertFalse(scan_results['variables'] is None)
        self.assertFalse(scan_results['attributes'] is None)
        self.assertFalse(scan_results['dimensions'] is None)
