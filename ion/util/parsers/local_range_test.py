#!/usr/bin/env python
'''
@author Luke C
@file ion/util/parsers/local_range_test.py
@description Parser for the local range test
'''

from zipfile import ZipFile
import numpy as np
import os
from StringIO import StringIO
from csv import DictReader, reader
from pyon.util.log import log

def lrt_parser(document):
    sio = StringIO()
    sio.write(document)
    sio.seek(0)

    with ZipFile(sio) as zp:
        files = zp.namelist()

        tables = {}
        for f in files:
            if os.path.basename(f) != 'master.csv':
                with zp.open(f) as t:
                    headers = reader(t).next()
                    data = np.genfromtxt(t, delimiter=',')

                datlimz = data[:, :-2]
                datlim = data[:,-2:]

                tables[os.path.basename(f)] = {'datlim':datlim, 'datlimz':datlimz, 'dims':headers[:-2]}
        
            else:
                master = f

        with zp.open(f) as master:
            dr = DictReader(master)

            for row in dr:
                key = '_'.join(['lrt',row['Reference Designator'], row['Data Product']])

                doc = {}
                doc['array'] = row['Array']
                doc['instrument_class'] = row['Instrument Class']
                doc['reference_designator'] = row['Reference Designator']
                doc['data_product'] = row['Data Product']
                lookup_key = row['Lookup Table ID']
                if not lookup_key in tables:
                    log.warning('Table not found while parsing LRT file: %s', lookup_key)
                    continue
                doc['datlim'] = tables[lookup_key]['datlim'].tolist()
                doc['datlimz'] = tables[lookup_key]['datlimz'].tolist()
                doc['dims'] = tables[lookup_key]['dims']
                yield key,doc
    return

    
