#!/usr/bin/env python
'''
@author Luke C
@date Tue Mar 26 16:00:21 EDT 2013
'''

from csv import DictReader
from StringIO import StringIO

def grt_parser(document):
    '''
    This parser YIELDS a document per call or until it's done
    The format for the document is CSV in this table format
    Array,Instrument Class,Reference Designator,Data Products,Units,Data Product Flagged,Minimum Range (lim(1)),Maximum Range (lim(2))
    
    Document Schema:
        array: 
            origin: Array
            type: String
        instrument_class: 
            origin: Instrument Class
            type: String
        reference_designator:
            origin: Reference Designator
            type: String
        data_product_in:
            origin: Data Product In
            type: String
        units:
            origin: Units
            type: String
        data_product_flagged:
            origin: Data Product Flagged
            type: String
        grt_min_value:
            origin: Min Value (lim(1))
            type: float64
        grt_max_value:
            origin: Max Value (lim(2))
            type: float64

    '''
    sio = StringIO()
    sio.write(document)
    sio.seek(0)


    dr = DictReader(sio)
    for row in dr:
        key = '_'.join(['grt',row['Reference Designator'], row['Data Products']])
        document = {}
        document['array']                = row['Array']
        document['instrument_class']     = row['Instrument Class']
        document['reference_designator'] = row['Reference Designator']
        document['data_product_in']      = row['Data Products']
        document['units']                = row['Units']
        document['data_product_flagged'] = row['Data Product Flagged']
        try:
            document['grt_min_value']        = float(row['Minimum Range (lim(1))'])
            document['grt_max_value']        = float(row['Maximum Range (lim(2))'])
        except ValueError:
            continue
        yield key,document
    return



