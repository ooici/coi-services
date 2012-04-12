__author__ = 'timgiguere'

import os
from nose.plugins.attrib import attr
from ion.agents.eoi.handler.hfr_radial_data_handler import HfrRadialDataHandler
from ion.agents.eoi.handler.base_external_data_handler import DataAcquisitionError, InstantiationError
from pyon.util.unit_test import PyonTestCase
from interface.objects import ExternalDataRequest, DatasetDescriptionDataSamplingEnum, ExternalDataset, DatasetDescription, UpdateDescription, ContactInformation, CompareResultEnum
import unittest
import numpy
import tempfile


@attr('UNIT', group='eoi')
class TestHfrRadialDataHandler(PyonTestCase):

    def setUp(self):

        self._datasets = {}

        file = self._create_tst_data_set('base')

        ext_ds = ExternalDataset(name="base", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = file
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'

        self._datasets['base'] = HfrRadialDataHandler(data_source=file, ext_dataset=ext_ds)

        file = self._create_tst_data_set('mod_gatt')

        ext_ds = ExternalDataset(name="mod_gatt", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = file
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'
        self._datasets['mod_gatt'] = HfrRadialDataHandler(data_source=file, ext_dataset=ext_ds)

        file = self._create_tst_data_set('new_gatt')

        ext_ds = ExternalDataset(name="new_gatt", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = file
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'

        self._datasets['new_gatt'] = HfrRadialDataHandler(data_source=file, ext_dataset=ext_ds)

        file = self._create_tst_data_set('mod_var')

        ext_ds = ExternalDataset(name="mod_var", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = file
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'
        self._datasets['mod_var'] = HfrRadialDataHandler(data_source=file, ext_dataset=ext_ds)

        file = self._create_tst_data_set('new_var')

        ext_ds = ExternalDataset(name="new_var", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = file
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'
        self._datasets['new_var'] = HfrRadialDataHandler(data_source=file, ext_dataset=ext_ds)

        file = self._create_tst_data_set('mod_varatt')

        ext_ds = ExternalDataset(name="mod_varatt", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        ext_ds.dataset_description.parameters["dataset_path"] = file
        ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'
        self._datasets['mod_varatt'] = HfrRadialDataHandler(data_source=file, ext_dataset=ext_ds)

        #file = 'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/ASSA/RDLi_ASSA_2012_02_09_1100.ruv    '
        #ext_ds = ExternalDataset(name="remote", dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        #ext_ds.dataset_description.parameters["dataset_path"] = file
        #ext_ds.dataset_description.parameters["temporal_dimension"] = 'time'
        #self._datasets['remote'] = HfrRadialDataHandler(data_source=file, ext_dataset=ext_ds)

        pass

    def tearDown(self):
        self._datasets['base'] = None
        self._datasets['mod_gatt'] = None
        self._datasets['new_gatt'] = None
        self._datasets['mod_var'] = None
        self._datasets['new_var'] = None
        self._datasets['mod_varatt'] = None
        pass

    def _create_tst_data_set(self, key):
        #attributes contents 1, data contents 1
        temp_file_base =  '%CTF: 1.00 \n'\
                          '%FileType: LLUV rdls "RadialMap" \n'\
                          '%LLUVSpec: 1.14  2010 07 18 \n'\
                          '%UUID: 9D847DC7-BCD4-4624-85D3-7F00A1294F71 \n'\
                          '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                          '%Site: SEAB "" \n'\
                          '%TimeStamp: 2011 08 24  16 00 00 \n'\
                          '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                          '%TableType: LLUV RDL9 \n'\
                          '%TableColumns: 18 \n'\
                          '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                          '%TableRows: 595 \n'\
                          '%TableStart: \n'\
                          '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                          '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                          '-73.9711577  40.3898353    0.261    7.464          0      11.842      11.842      -7.469     -33.607       3        3       0.1055      3.0206    3.0224     2.0     -7.469     182.0         1 \n'\
                          '-73.9680618  40.3896489    3.832   31.195          0     999.000       1.862     -31.429     -31.429       1        4       0.3683      2.9999    3.0224     7.0    -31.429     187.0         1 \n'\
                          '-73.9649991  40.3892569    3.932   18.491          0      18.515      10.270      -9.646     -28.162       2        6       0.6284      2.9564    3.0224    12.0    -18.904     192.0         1 \n'\
                          '-73.9619927  40.3886621    3.459   11.308          0      16.966       8.378      12.135     -29.251       3        7       0.8837      2.8903    3.0224    17.0    -11.825     197.0         1 \n'\
                          '-73.9590656  40.3878692    6.574   16.265          0       4.485       8.910     -11.825     -24.894       5        5       1.1322      2.8023    3.0224    22.0    -17.543     202.0         1 \n'\
                          '-73.9562401  40.3868841    9.204   18.055          0       1.903       6.439     -18.359     -22.716       2        6       1.3721      2.6930    3.0224    27.0    -20.266     207.0         1 \n'\
                          '%TableEnd: \n'

        temp_file_mod_gatt = '%CTF: 1.00 \n'\
                             '%FileType: LLUV rdls "RadialMap" \n'\
                             '%LLUVSpec: 1.14  2010 07 18 \n'\
                             '%UUID: E88CB18F-6DB6-4656-8596-5E2EF58F2116 \n'\
                             '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                             '%Site: WILD "" \n'\
                             '%TimeStamp: 2011 12 19  14 00 00 \n'\
                             '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                             '%TableType: LLUV RDL9 \n'\
                             '%TableColumns: 18 \n'\
                             '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                             '%TableRows: 595 \n'\
                             '%TableStart: \n'\
                             '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                             '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                             '-73.9711577  40.3898353    0.261    7.464          0      11.842      11.842      -7.469     -33.607       3        3       0.1055      3.0206    3.0224     2.0     -7.469     182.0         1 \n'\
                             '-73.9680618  40.3896489    3.832   31.195          0     999.000       1.862     -31.429     -31.429       1        4       0.3683      2.9999    3.0224     7.0    -31.429     187.0         1 \n'\
                             '-73.9649991  40.3892569    3.932   18.491          0      18.515      10.270      -9.646     -28.162       2        6       0.6284      2.9564    3.0224    12.0    -18.904     192.0         1 \n'\
                             '-73.9619927  40.3886621    3.459   11.308          0      16.966       8.378      12.135     -29.251       3        7       0.8837      2.8903    3.0224    17.0    -11.825     197.0         1 \n'\
                             '-73.9590656  40.3878692    6.574   16.265          0       4.485       8.910     -11.825     -24.894       5        5       1.1322      2.8023    3.0224    22.0    -17.543     202.0         1 \n'\
                             '-73.9562401  40.3868841    9.204   18.055          0       1.903       6.439     -18.359     -22.716       2        6       1.3721      2.6930    3.0224    27.0    -20.266     207.0         1 \n'\
                             '%TableEnd: \n'

        #attributes contents 1, data contents 2
        temp_file_new_gatt = '%CTF: 1.00 \n'\
                             '%FileType: LLUV rdls "RadialMap" \n'\
                             '%LLUVSpec: 1.14  2010 07 18 \n'\
                             '%UUID: 9D847DC7-BCD4-4624-85D3-7F00A1294F71 \n'\
                             '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                             '%TimeCoverage: 150.000 Minutes \n'\
                             '%Origin:  38.9421667  -74.8861333'\
                             '%GreatCircle: "WGS84" 6378137.000  298.257223562997 \n'\
                             '%GeodVersion: "CGEO" 1.57  2009 03 10 \n'\
                             '%LLUVTrustData: all %% all lluv xyuv rbvd \n'\
                             '%RangeStart: 1 \n'\
                             '%RangeEnd: 36 \n'\
                             '%TableType: LLUV RDL9 \n'\
                             '%TableColumns: 18 \n'\
                             '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                             '%TableRows: 595 \n'\
                             '%TableStart: \n'\
                             '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                             '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                             '-73.9711577  40.3898353    0.261    7.464          0      11.842      11.842      -7.469     -33.607       3        3       0.1055      3.0206    3.0224     2.0     -7.469     182.0         1 \n'\
                             '-73.9680618  40.3896489    3.832   31.195          0     999.000       1.862     -31.429     -31.429       1        4       0.3683      2.9999    3.0224     7.0    -31.429     187.0         1 \n'\
                             '-73.9649991  40.3892569    3.932   18.491          0      18.515      10.270      -9.646     -28.162       2        6       0.6284      2.9564    3.0224    12.0    -18.904     192.0         1 \n'\
                             '-73.9619927  40.3886621    3.459   11.308          0      16.966       8.378      12.135     -29.251       3        7       0.8837      2.8903    3.0224    17.0    -11.825     197.0         1 \n'\
                             '-73.9590656  40.3878692    6.574   16.265          0       4.485       8.910     -11.825     -24.894       5        5       1.1322      2.8023    3.0224    22.0    -17.543     202.0         1 \n'\
                             '-73.9562401  40.3868841    9.204   18.055          0       1.903       6.439     -18.359     -22.716       2        6       1.3721      2.6930    3.0224    27.0    -20.266     207.0         1 \n'\
                             '%TableEnd: \n'

        #attributes contents 2, data contents 2
        temp_file_mod_var = '%CTF: 1.00 \n'\
                            '%FileType: LLUV rdls "RadialMap" \n'\
                            '%LLUVSpec: 1.14  2010 07 18 \n'\
                            '%UUID: 9D847DC7-BCD4-4624-85D3-7F00A1294F71 \n'\
                            '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                            '%Site: SEAB "" \n'\
                            '%TimeStamp: 2011 08 24  16 00 00 \n'\
                            '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                            '%TableType: LLUV RDL9 \n'\
                            '%TableColumns: 18 \n'\
                            '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                            '%TableRows: 595 \n'\
                            '%TableStart: \n'\
                            '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                            '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                            '-74.8203594  38.9530647   18.856    3.994          0     999.000     999.000     -19.274     -19.274       1        2       5.7015      1.2119    5.8289    78.0    -19.274     258.0         1 \n'\
                            '-74.8193954  38.9485464   12.484    1.524          0       7.871       6.174      -3.447     -25.766       3        3       5.7855      0.7104    5.8289    83.0    -12.577     263.0         1 \n'\
                            '-74.8189395  38.9439798   22.507    0.769          0       2.434      18.800     -17.651     -22.520       2        5       5.8253      0.2034    5.8289    88.0    -22.520     268.0         1 \n'\
                            '-74.8189950  38.9393994   24.109   -1.281          0     999.000       9.770     -24.143     -24.143       1        5       5.8209     -0.3051    5.8289    93.0    -24.143     273.0         1 \n'\
                            '-74.8195614  38.9348403   13.458   -1.901          0      19.713       6.498       1.422     -43.618       5        5       5.7722     -0.8112    5.8289    98.0    -13.592     278.0         1 \n'\
                            '-74.8206343  38.9303370   12.452   -2.884          0       5.776      11.331     -12.782     -25.766       3        5       5.6795     -1.3112    5.8289   103.0    -12.782     283.0         1 \n'\
                            '%TableEnd: \n'

        temp_file_new_var = '%CTF: 1.00 \n'\
                            '%FileType: LLUV rdls "RadialMap" \n'\
                            '%LLUVSpec: 1.14  2010 07 18 \n'\
                            '%UUID: 9D847DC7-BCD4-4624-85D3-7F00A1294F71 \n'\
                            '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                            '%Site: SEAB "" \n'\
                            '%TimeStamp: 2011 08 24  16 00 00 \n'\
                            '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                            '%TableType: LLUV RDL9 \n'\
                            '%TableColumns: 18 \n'\
                            '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD \n'\
                            '%TableRows: 595 \n'\
                            '%TableStart: \n'\
                            '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction \n'\
                            '%%     (deg)       (deg)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True) \n'\
                            '-73.9711577  40.3898353    0.261    7.464          0      11.842      11.842      -7.469     -33.607       3        3       0.1055      3.0206    3.0224     2.0     -7.469     182.0 \n'\
                            '-73.9680618  40.3896489    3.832   31.195          0     999.000       1.862     -31.429     -31.429       1        4       0.3683      2.9999    3.0224     7.0    -31.429     187.0 \n'\
                            '-73.9649991  40.3892569    3.932   18.491          0      18.515      10.270      -9.646     -28.162       2        6       0.6284      2.9564    3.0224    12.0    -18.904     192.0 \n'\
                            '-73.9619927  40.3886621    3.459   11.308          0      16.966       8.378      12.135     -29.251       3        7       0.8837      2.8903    3.0224    17.0    -11.825     197.0 \n'\
                            '-73.9590656  40.3878692    6.574   16.265          0       4.485       8.910     -11.825     -24.894       5        5       1.1322      2.8023    3.0224    22.0    -17.543     202.0 \n'\
                            '-73.9562401  40.3868841    9.204   18.055          0       1.903       6.439     -18.359     -22.716       2        6       1.3721      2.6930    3.0224    27.0    -20.266     207.0 \n'\
                            '%TableEnd: \n'

        temp_file_mod_varatt = '%CTF: 1.00 \n'\
                               '%FileType: LLUV rdls "RadialMap" \n'\
                               '%LLUVSpec: 1.14  2010 07 18 \n'\
                               '%UUID: 9D847DC7-BCD4-4624-85D3-7F00A1294F71 \n'\
                               '%Manufacturer: CODAR Ocean Sensors. SeaSonde \n'\
                               '%Site: SEAB "" \n'\
                               '%TimeStamp: 2011 08 24  16 00 00 \n'\
                               '%TimeZone: "UTC" +0.000 0 "Atlantic/Reykjavik" \n'\
                               '%TableType: LLUV RDL9 \n'\
                               '%TableColumns: 18 \n'\
                               '%TableColumnTypes: LOND LATD VELU VELV VFLG ESPC ETMP MAXV MINV ERSC ERTC XDST YDST RNGE BEAR VELO HEAD SPRC \n'\
                               '%TableRows: 595 \n'\
                               '%TableStart: \n'\
                               '%%   Longitude   Latitude    U comp   V comp  VectorFlag    Spatial    Temporal     Velocity    Velocity  Spatial  Temporal X Distance  Y Distance   Range   Bearing   Velocity  Direction   Spectra \n'\
                               '%%     (rad)       (rad)     (cm/s)   (cm/s)  (GridCode)    Quality     Quality     Maximum     Minimum    Count    Count      (km)        (km)       (km)    (True)    (cm/s)     (True)    RngCell \n'\
                               '-73.9711577  40.3898353    0.261    7.464          0      11.842      11.842      -7.469     -33.607       3        3       0.1055      3.0206    3.0224     2.0     -7.469     182.0         1 \n'\
                               '-73.9680618  40.3896489    3.832   31.195          0     999.000       1.862     -31.429     -31.429       1        4       0.3683      2.9999    3.0224     7.0    -31.429     187.0         1 \n'\
                               '-73.9649991  40.3892569    3.932   18.491          0      18.515      10.270      -9.646     -28.162       2        6       0.6284      2.9564    3.0224    12.0    -18.904     192.0         1 \n'\
                               '-73.9619927  40.3886621    3.459   11.308          0      16.966       8.378      12.135     -29.251       3        7       0.8837      2.8903    3.0224    17.0    -11.825     197.0         1 \n'\
                               '-73.9590656  40.3878692    6.574   16.265          0       4.485       8.910     -11.825     -24.894       5        5       1.1322      2.8023    3.0224    22.0    -17.543     202.0         1 \n'\
                               '-73.9562401  40.3868841    9.204   18.055          0       1.903       6.439     -18.359     -22.716       2        6       1.3721      2.6930    3.0224    27.0    -20.266     207.0         1 \n'\
                               '%TableEnd: \n'

        th, tn = tempfile.mkstemp(prefix=(key + "_"), suffix='.ruv')
        result = tn
        tempobj = os.fdopen(th, 'w')

        if key == 'base':
            tempobj.write(temp_file_base)
        elif key == 'mod_gatt':
            tempobj.write(temp_file_mod_gatt)
        elif key == 'new_gatt':
            tempobj.write(temp_file_new_gatt)
        elif key == 'mod_var':
            tempobj.write(temp_file_mod_var)
        elif key == 'new_var':
            tempobj.write(temp_file_new_var)
        elif key == 'mod_varatt':
            tempobj.write(temp_file_mod_varatt)

        tempobj.close()
        return result

        pass

    def test_get_attributes(self):
        attributes = self._datasets['base'].get_attributes()
        #make sure attributes match up here?

    def test_acquire_data(self):
        data_iter = self._datasets['base'].acquire_data()
        for vn, slice_, rng, data in data_iter:
            self.assertTrue(isinstance(slice_, tuple))
            self.assertTrue(isinstance(rng, tuple))
            self.assertTrue(isinstance(data, numpy.ndarray))

    def test_acquire_data_one_variable(self):
        data_iter = self._datasets['base'].acquire_data(var_name='LOND')
        for vn, slice_, rng, data in data_iter:
            self.assertTrue(isinstance(slice_, tuple))
            self.assertTrue(isinstance(rng, tuple))
            self.assertTrue(isinstance(data, numpy.ndarray))

    def test_has_data_changed_true(self):
        fingerprint = self._datasets['base'].get_fingerprint()
        self.assertFalse(self._datasets['base'].has_data_changed(fingerprint))

    def test_has_data_changed_false(self):
        fingerprint = self._datasets['base'].get_fingerprint()
        self.assertTrue(self._datasets['mod_gatt'].has_data_changed(fingerprint))

    def test_has_new_data_true(self):
        url = 'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/ASSA/'
        # Obtain the current list of files from the source
        from ion.agents.eoi.handler.hfr_radial_data_handler import AnchorParser
        import urllib
        parser = AnchorParser()
        data = urllib.urlopen(url).read()
        parser.feed(data)
        previous_files = parser._link_names
        # Remove the last item in the list to simulate a 'new' file being present
        previous_files.remove(previous_files[-1])

        # Assign the url (temporary measure) and file list to the update_description object
        self._datasets['base']._ext_dataset_res.update_description.parameters['url'] = url
        self._datasets['base']._ext_dataset_res.update_description.parameters['new_data_check'] = previous_files

        # Perform the has_new_data test
        res = self._datasets['base'].has_new_data()
        self.assertTrue(res)

    def test_has_new_data_true_windowed(self):
        url = 'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/ASSA/'
        # Obtain the current list of files from the source
        from ion.agents.eoi.handler.hfr_radial_data_handler import AnchorParser
        import urllib
        parser = AnchorParser()
        data = urllib.urlopen(url).read()
        parser.feed(data)
        previous_files = parser._link_names
        # Add a 'dummy' item in the first spot in the list to simulate the file being removed from the server
        previous_files.insert(0, 'I_was_removed')
        # Remove the last item in the list to simulate a 'new' file being present
        previous_files.remove(previous_files[-1])

        # Assign the url (temporary measure) and file list to the update_description object
        self._datasets['base']._ext_dataset_res.update_description.parameters['url'] = url
        self._datasets['base']._ext_dataset_res.update_description.parameters['new_data_check'] = previous_files

        # Perform the has_new_data test
        res = self._datasets['base'].has_new_data()
        self.assertTrue(res)

#    @unittest.skip("")
    def test_has_new_data_false(self):
        url = 'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/ASSA/'
        # Obtain the current list of files from the source
        from ion.agents.eoi.handler.hfr_radial_data_handler import AnchorParser
        import urllib
        parser = AnchorParser()
        data = urllib.urlopen(url).read()
        parser.feed(data)
        previous_files = parser._link_names

        # Assign the url (temporary measure) and file list to the update_description object
        self._datasets['base']._ext_dataset_res.update_description.parameters['url'] = url
        self._datasets['base']._ext_dataset_res.update_description.parameters['new_data_check'] = previous_files

        # Perform the has_new_data test
        res = self._datasets['base'].has_new_data()
        self.assertFalse(res)

#    @unittest.skip("")
    def test_has_new_data_false_windowed(self):
        url = 'http://marine.rutgers.edu/cool/maracoos/codar/ooi/radials/ASSA/'
        # Obtain the current list of files from the source
        from ion.agents.eoi.handler.hfr_radial_data_handler import AnchorParser
        import urllib
        parser = AnchorParser()
        data = urllib.urlopen(url).read()
        parser.feed(data)
        previous_files = parser._link_names
        # Add a 'dummy' item in the first spot in the list to simulate the file being removed from the server
        previous_files.insert(0, 'I_was_removed')

        # Assign the url (temporary measure) and file list to the update_description object
        self._datasets['base']._ext_dataset_res.update_description.parameters['url'] = url
        self._datasets['base']._ext_dataset_res.update_description.parameters['new_data_check'] = previous_files

        # Perform the has_new_data test
        res = self._datasets['base'].has_new_data()
        self.assertFalse(res)

    def test_get_fingerprint(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.NONE
        fingerprint = self._datasets['base'].get_fingerprint(True)
        self.assertTrue(not len(fingerprint) == 0)

    def test_get_fingerprint_no_recalculate(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.NONE
        fingerprint1 = self._datasets['base'].get_fingerprint(True) #initialize the fingerprint
        fingerprint2 = self._datasets['base'].get_fingerprint(False) # make sure you get the same one back
        self.assertEqual(fingerprint1, fingerprint2)

    def test_get_fingerprint_full(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint = self._datasets['base'].get_fingerprint(True)
        self.assertTrue(not len(fingerprint) == 0)

    def test_get_fingerprint_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        fingerprint = self._datasets['base'].get_fingerprint(True)
        self.assertTrue(not len(fingerprint) == 0)

    def test_compare_equal_full(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint = self._datasets['base'].get_fingerprint(True)
        for x in self._datasets['base'].compare(fingerprint):
            self.assertEqual(x.difference, CompareResultEnum.EQUAL)

    def test_compare_equal_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint = self._datasets['base'].get_fingerprint(True)
        for x in self._datasets['base'].compare(fingerprint):
            self.assertEqual(x.difference, CompareResultEnum.EQUAL)

    def test_compare_mod_gatt_full(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['mod_gatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        for x in self._datasets['mod_gatt'].compare(fingerprint1):
            self.assertEqual(x.difference, CompareResultEnum.MOD_GATT)

    def test_compare_mod_gatt_full_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['mod_gatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._datasets['mod_gatt'].compare(fingerprint1):
            self.assertTrue(x.difference == CompareResultEnum.MOD_GATT or x.difference == CompareResultEnum.MOD_VAR)

    def test_compare_mod_gatt_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['mod_gatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._datasets['mod_gatt'].compare(fingerprint1):
            self.assertEqual(x.difference, CompareResultEnum.MOD_GATT)

    def test_compare_new_gatt_full(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['new_gatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        for x in self._datasets['new_gatt'].compare(fingerprint1):
            self.assertEqual(x.difference, CompareResultEnum.NEW_GATT)

    def test_compare_new_gatt_full_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['new_gatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._datasets['new_gatt'].compare(fingerprint1):
            self.assertTrue(x.difference == CompareResultEnum.NEW_GATT or x.difference == CompareResultEnum.MOD_VAR)

    def test_compare_new_gatt_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['new_gatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._datasets['new_gatt'].compare(fingerprint1):
            self.assertEqual(x.difference, CompareResultEnum.NEW_GATT)

    def test_compare_mod_varatt_full(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['mod_varatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        for x in self._datasets['mod_varatt'].compare(fingerprint1):
            self.assertTrue(x.difference == CompareResultEnum.MOD_VARATT or x.difference == CompareResultEnum.MOD_VAR)

    def test_compare_mod_varatt_full_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['mod_varatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._datasets['mod_varatt'].compare(fingerprint1):
            self.assertTrue(x.difference == CompareResultEnum.MOD_VARATT or x.difference == CompareResultEnum.MOD_VAR)

    def test_compare_mod_varatt_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['mod_varatt']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._datasets['mod_varatt'].compare(fingerprint1):
            self.assertTrue(x.difference == CompareResultEnum.MOD_VARATT or x.difference == CompareResultEnum.MOD_VAR)

    def test_compare_new_var_full(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['new_var']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        for x in self._datasets['new_var'].compare(fingerprint1):
            self.assertEqual(x.difference, CompareResultEnum.NEW_VAR)

    def test_compare_new_var_full_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FULL
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['new_var']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._datasets['new_var'].compare(fingerprint1):
            self.assertTrue(x.difference == CompareResultEnum.NEW_VAR or x.difference == CompareResultEnum.MOD_VAR)

    def test_compare_new_var_first_last(self):
        self._datasets['base']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        fingerprint1 = self._datasets['base'].get_fingerprint(True)
        self._datasets['new_var']._ext_dataset_res.dataset_description.data_sampling = DatasetDescriptionDataSamplingEnum.FIRST_LAST
        for x in self._datasets['new_var'].compare(fingerprint1):
            self.assertEqual(x.difference, CompareResultEnum.NEW_VAR)

    def test_scan(self):
        scan_results = self._datasets['base'].scan()
        self.assertTrue('variables' in scan_results)
        self.assertTrue('attributes' in scan_results)
        self.assertTrue('dimensions' in scan_results)
        self.assertFalse(scan_results['variables'] is None)
        self.assertFalse(scan_results['attributes'] is None)
        self.assertTrue(scan_results['dimensions'] is None)
