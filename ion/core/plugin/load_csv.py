#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file extern/parameter-definitions/parameter_definitions/external/load_csv.py
@date Tue Oct 30 10:15:55 EDT 2012
@brief Plugin for loading parameter definitions from a csv file
'''

from coverage_model.parameter import ParameterContext
from coverage_model.parameter_types import QuantityType, ArrayType, RecordType
from coverage_model.basic_types import AxisTypeEnum
from ion.util.parameter_loader import ParameterPlugin
import numpy as np
import csv
import requests

FILEPATH='extern/parameter-definitions/parameter_definitions/external/example.csv'
URL='https://docs.google.com/spreadsheet/pub?key=0AgGScp7mjYjydGZVV1JDYUlhWnBHV0JvVXBGWkhsS3c&output=csv'

class NotSupportedError(Exception):
    pass

class Plugin(ParameterPlugin):
    name = 'Spreadsheet Plugin'
    url  = URL
    additional_attrs = {
            'Attributes':'attributes',
            'Index Key':'index_key',
            'Ion Name':'ion_name',
            'Standard Name':'standard_name',
            'Long Name':'long_name',
            'OOI Short Name':'ooi_short_name',
            'CDM Data Type':'cdm_data_type',
            'Variable Reports':'variable_reports',
            'References List':'references_list',
            'Comment' : 'comment',
            'Code Reports':'code_reports'
            }

    def __init__(self, config):
        if 'url' in config and config['url']:
            self.url = config['url']



    def load(self):
        contexts = []
        for record in self.load_url(self.url):
            context = self.build_context(record)
            contexts.append(context)
        return contexts


    def load_url(self,url):
        csv_doc = requests.get(url).content
        csv_doc = csv_doc.splitlines()
        reader = csv.DictReader(csv_doc)
        return list(reader)

    def load_file(self,path):
        with open(path,'r+') as csv_doc:
            reader = csv.DictReader(csv_doc)
            return list(reader)

    def build_context(self,record):
            context = ParameterContext(name=record['Name'], param_type=self.param_type(record['Parameter Type']))
            context.uom = record['Unit of Measure']
            if record['Fill Value']:
                context.fill_value = self.fill_value(record['Fill Value'], record['Parameter Type'])
            if record['Axis']:
                context.reference_frame = self.ref_frame(record['Axis'])
            for key in self.additional_attrs.iterkeys():
                if key in record and record[key]:
                    setattr(context,self.additional_attrs[key],record[key])
            return context


    def param_type(self,s):
        if s == 'record':
            return RecordType()
        elif s == 'array':
            return ArrayType()
        else: 
            return QuantityType(value_encoding=np.dtype(s))

    def ref_frame(self,s):
        s = s.lower()
        if 'lat' == s:
            return AxisTypeEnum.LAT
        elif 'lon' == s:
            return AxisTypeEnum.LON
        else:
            raise NotSupportedError('Unsupported reference frame %s' % s)

    def fill_value(self,v, dtype_str):
        if 'float' in dtype_str:
            return float(v)
        elif 'int' in dtype_str:
            return int(v)
        else:
            raise NotSupportedError('Unsupported fill value %s with %s')
        
if __name__ == '__main__':
    import sys
    sys.stderr.write('This plugin is not meant to be run')

