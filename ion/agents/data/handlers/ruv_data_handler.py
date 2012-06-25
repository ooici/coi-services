#!/usr/bin/env python

"""
@package ion.agents.data.handlers.ruv_data_handler
@file ion/agents/data/handlers/ruv_data_handler
@author Christopher Mueller
@brief 
"""
from pyon.public import log
from pyon.util.containers import get_safe
from pyon.ion.granule.taxonomy import TaxyTool
from pyon.ion.granule.granule import build_granule
from pyon.ion.granule.record_dictionary import RecordDictionaryTool
from ion.agents.data.handlers.base_data_handler import BaseDataHandler, NoNewDataWarning
from ion.agents.data.handlers.handler_utils import list_file_info, get_sbuffer, get_time_from_filename
import numpy as np
import re
from StringIO import StringIO

DH_CONFIG_DETAILS = {
    'ds_desc_params': [
        ('base_url',str,'base path/url for this dataset'),
        ('pattern',str,'The filter pattern for this dataset.  If file-based, use shell-style notation; if remote (http, ftp), use regex'),
    ],
}

class RuvDataHandler(BaseDataHandler):
    @classmethod
    def _init_acquisition_cycle(cls, config):
        # TODO: Can't build a parser here because we won't have a file name!!  Just a directory :)
        # May not be much to do in this method...
        # maybe just ensure access to the dataset_dir and move some of the 'buried' params up to the config dict?
        ext_dset_res = get_safe(config, 'external_dataset_res', None)
        if not ext_dset_res:
            raise SystemError('external_dataset_res not present in configuration, cannot continue')

        config['ds_params'] = ext_dset_res.dataset_description.parameters

#        base_url = ext_dset_res.dataset_description.parameters['base_url']
#        pattern = get_safe(ext_dset_res.dataset_description.parameters, 'pattern')
#        config['base_url'] = base_url
#        config['pattern'] = pattern

    @classmethod
    def _constraints_for_new_request(cls, config):
        old_list = get_safe(config, 'new_data_check') or []

        ret = {}
        base_url = get_safe(config,'ds_params.base_url')
        list_pattern = get_safe(config,'ds_params.list_pattern')
        date_pattern = get_safe(config, 'ds_params.date_pattern')
        date_extraction_pattern = get_safe(config, 'ds_params.date_extraction_pattern')

        curr_list = list_file_info(base_url, list_pattern)

        # Determine which files are new
        new_list = [tuple(x) for x in curr_list if list(x) not in old_list]

        if len(new_list) is 0:
            raise NoNewDataWarning()

        # The curr_list is the new new_data_check - used for the next "new data" evaluation
        config['set_new_data_check'] = curr_list

        # The new_list is the set of new files - these will be processed
        ret['new_files'] = new_list
        ret['start_time'] = get_time_from_filename(new_list[0][0], date_extraction_pattern, date_pattern)
        ret['end_time'] = get_time_from_filename(new_list[len(new_list) - 1][0], date_extraction_pattern, date_pattern)
        ret['bounding_box'] = {}
        ret['vars'] = []

        return ret

    @classmethod
    def _constraints_for_historical_request(cls, config):
        base_url = get_safe(config,'ds_params.base_url')
        list_pattern = get_safe(config,'ds_params.list_pattern')
        date_pattern = get_safe(config, 'ds_params.date_pattern')
        date_extraction_pattern = get_safe(config, 'ds_params.date_extraction_pattern')

        start_time = get_safe(config, 'constraints.start_time')
        end_time = get_safe(config, 'constraints.end_time')

        new_list = []
        curr_list = list_file_info(base_url, list_pattern)

        for x in curr_list:
            curr_time = get_time_from_filename(x[0], date_extraction_pattern, date_pattern)
            if start_time <= curr_time <= end_time:
                new_list.append(x)

        return {'new_files':new_list}

    @classmethod
    def _get_data(cls, config):
        new_flst = get_safe(config, 'constraints.new_files', [])
#        log.debug('new_flist: {0}'.format(new_flst))
        for f in new_flst:
            log.debug('Processing File: {0}'.format(f))
            try:
                parser = RuvParser(f[0])

#                log.info('Header Info:\n{0}'.format(parser.header_map))
#                log.info('Tables Available:\n{0}'.format(parser.table_map.keys()))

                #For now, yield nothing. We need to figure out what to do with the secondary tables
                #before we can start building granules to send back.
                yield []

            except RuvParseException as rpe:
                # TODO: Decide what to do here, raise an exception or carry on
                log.error('Error parsing data file: \'{0}\''.format(f))

class RuvParser(object):
    _col_type_map = {
        'LLUV RDL9':{
            'LOND':('Longitude','deg',),
            'LATD':('Latitude','deg',),
            'VELU':('U comp','cm/s',),
            'VELV':('V comp','cm/s',),
            'VFLG':('VectorFlag','GridCode'),
            'ESPC':('Spatial Quality',),
            'ETMP':('Temporal Quality',),
            'MAXV':('Velocity Maximum',),
            'MINV':('Velocity Minimum',),
            'ERSC':('Spatial Count',),
            'ERTC':('Temporal Count',),
            'XDST':('X Distance','km',),
            'YDST':('Y Distance','km',),
            'RNGE':('Range','km',),
            'BEAR':('Bearing','True',),
            'VELO':('Velocity','cm/s',),
            'HEAD':('Direction','True',),
            'SPRC':('Spectra RngCell',),
            },
        'rads rad1':{
            'TIME':('Time FromStart','seconds',),
            'AMP1':('Calculated Amp1','1/v^2',),
            'AMP2':('Calculated Amp2','1/v^2',),
            'PH13':('Calculated Phase13','deg',),
            'PH23':('Corrected Phase23','deg',),
            'CPH1':('Corrected Phase1','deg',),
            'CPH2':('Corrected Phase2','deg',),
            'SNF1':('Noise Floor NF1','dBm',),
            'SNF2':('Noise Floor NF2','dBm',),
            'SNF3':('Noise Floor NF3','dBm',),
            'SSN1':('SignalToNoise SN1','dB',),
            'SSN2':('SignalToNoise SN2','dB',),
            'SSN3':('SignalToNoise SN3','dB',),
            'DGRC':('Diag Range Cell',),
            'DOPV':('Valid Dopplr Cells',),
            'DDAP':('Dual Angle Prcnt','%',),
            'RADV':('Radial Vector Count',),
            'RAPR':('RadsV per Range',),
            'RARC':('Rads Range Cells',),
            'RADR':('Max Range','km',),
            'RMCV':('Vel Max','cm/s',),
            'RACV':('Vel Aver','cm/s',),
            'RABA':('Bearing Average','deg CWN',),
            'RTYP':('Radial Type',),
            'STYP':('Spectra Type',),
            'TYRS':('Year',),
            'TMON':('Mo',),
            'TDAY':('Dy',),
            'THRS':('Hr',),
            'TMIN':('Mn',),
            'TSEC':('S',),
            },
        'rcvr rcv2':{
            'TIME':('LogTime','Minutes',),
            'RTMP':('Rcvr','iC',),
            'MTMP':('Awg3','iC',),
            'XTRP':('XmtTrip','HexCode',),
            'RUNT':('Awg3Run','Seconds',),
            'SP24':('Supply','Volts',),
            'SP05':('+5VDC','Volts',),
            'SN05':('-5VDC','Volts',),
            'SP12':('+12VDC','Volts',),
            'XPHT':('XInt','iC',),
            'XAHT':('XAmp','iC',),
            'XAFW':('XForw','Watts',),
            'XARW':('XRefl','Watts',),
            'XP28':('X+Ampl','Volts',),
            'XP05':('X+5VDC','Volts',),
            'GRMD':('GpsRcv Mode',),
            'GDMD':('GpsDsp Mode',),
            'GSLK':('GpsSat Lock',),
            'GSUL':('GpsSat Unlock',),
            'PLLL':('PLL Unlock',),
            'HTMP':('HiRcvr','iC',),
            'HUMI':('Humid','%',),
            'RBIA':('Supply','Amps',),
            'EXTA':('Extern InputA',),
            'EXTB':('Extern InputB',),
            'CRUN':('CompRunTime Minutes',),
            'TYRS':('Year',),
            'TMON':('Mo',),
            'TDAY':('Dy',),
            'THRS':('Hr',),
            'TMIN':('Mn',),
            'TSEC':('S',),
            },
        }

    header_map = {}
    table_map = {}

    _ctf_re=re.compile('%CTF: ?([0-9]+(?:\.[0-9]*)?)')

    _h_line_re=re.compile('%([a-zA-Z:]*) ?(.*)')

    _tbl_type_re=re.compile('%TableType: ?(.*)')
    _tbl_num_cols_re=re.compile('%TableColumns: ?(\d*)')
    _tbl_num_rows_re=re.compile('%TableRows: ?(\d*)')
    _tbl_col_types_re=re.compile('%TableColumnTypes: ?(.*)')
    _tbl_strt_re=re.compile('%TableStart: ?\d*')
    _tbl_end_re=re.compile('%TableEnd: ?\d*')
    _tbl_hdrs_re=re.compile('%%[ \t]*?(.*)')

    def _parse_header(self, header_str):
        header_lines=header_str.splitlines(True)
        for line in header_lines:
            m=self._h_line_re.search(line)
            self.header_map[m.group(1)]=m.group(2)

    def _parse_table(self, tbl_key, tbl_str):
        num_cols=self._tbl_num_cols_re.search(tbl_str)
        num_rows=self._tbl_num_rows_re.search(tbl_str)
        col_types=self._tbl_col_types_re.search(tbl_str)
        tbl_s=self._tbl_strt_re.search(tbl_str)
        tbl_e=self._tbl_end_re.search(tbl_str)

#        print "----"
#        print 'tbl_key: {0}'.format(tbl_key)
#        print 'num_cols: {0}'.format(num_cols.group(1))
#        print 'num_row: {0}'.format(num_rows.group(1))
#        print 'col_types: {0}'.format(col_types.group(1))
#        print 'tbl_start: {0}'.format(tbl_s.end()+1)
#        print 'tbl_end: {0}'.format(tbl_e.start())

        # Pull out the core table
        tbl_core = tbl_str[tbl_s.end()+1:tbl_e.start()-1]

        ctypes=col_types.group(1).split()
        tbl_hdrs=self._tbl_hdrs_re.findall(tbl_core)
        tbl_p_map = {}
        data_map = {}
        for i in xrange(len(ctypes)):
            tbl_p_map[ctypes[i]] = self._col_type_map[tbl_key][ctypes[i]]
            data_map[ctypes[i]] = np.genfromtxt(StringIO(tbl_core.replace('%','')), skip_header=len(tbl_hdrs), usecols=i, missing_values='999.000')

        self.table_map[tbl_key] = {'params':tbl_p_map, 'data':data_map}

    def __init__(self, url):

        fstr = None
        sb = None
        try:
            sb = get_sbuffer(url)
            fstr = sb.read()
        finally:
            if not sb is None:
                sb.close()

        if not fstr:
            raise RuvParseException('Error reading file: {0}'.format(url))

        # Verify that this is a CTF v 1.00 file
        ctf_m=self._ctf_re.search(fstr)
        if ctf_m is None:
            raise RuvParseException('\'{0}\' does not have %CTF flag'.format(url))

        if not ctf_m.group(1) == '1.00':
            raise RuvParseException('\'{0}\' not CTF version 1.00'.format(url))

        # Find the TableTypes - allows discovery of other pieces
        tbl_types=self._tbl_type_re.finditer(fstr)
        if not tbl_types:
            raise RuvParseException('\'{0}\' does not contain %TableTypes keywords'.format(url))
        ttps=[]
        for m in tbl_types:
            ttps.append((m.group(0),m.group(1),m.start(),m.end()))

        tbl_ends=self._tbl_end_re.finditer(fstr)
        if not tbl_ends:
            raise RuvParseException('\'{0}\' does not contain any %TableEnd keywords'.format(url))
        tends=[]
        for m in tbl_ends:
            tends.append((m.group(0),m.end()))

        # There should be the same number of results for ttype & tend
        assert len(ttps) == len(tends)

        header_str = fstr[0:ttps[0][2]]
        self._parse_header(header_str)
        for i in xrange(len(ttps)):
            key=ttps[i][1]
            self._parse_table(key, fstr[ttps[i][2]:tends[i][1]])

class RuvParseException(Exception):
    pass