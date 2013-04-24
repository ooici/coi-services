#!/usr/bin/env python
'''
@author Luke C
@date Wed Mar 27 09:12:35 EDT 2013
'''

from csv import DictReader
from StringIO import StringIO

def stuck_value_test_parser(document):
    '''
    This parser YIELDS a document per call or until it's done
    The format for the document is CSV in this table format

    Array,Instrument Class,Reference Designator,Data Product,Units,Resolution (R),Number of repeat values N

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
        data_product:
            origin: Data Product
            type: String
        units:
            origin: Units
            type: String
        svt_resolution:
            origin: Resolution (R)
            type: float64
        svt_n:
            origin: Number of repeat values N
            type: float64

    '''

    sio = StringIO()
    sio.write(document)
    sio.seek(0)

    dr = DictReader(sio)
    for row in dr:
        key = '_'.join(['svt',row['Reference Designator'], row['Data Product']])

        document = {}
        document['array']                = row['Array']
        document['instrument_class']     = row['Instrument Class']
        document['reference_designator'] = row['Reference Designator']
        document['data_product']         = row['Data Product']
        document['units']                = row['Units']
        document['svt_resolution']           = float(row['Resolution (R)'])
        document['svt_n']                    = float(row['Number of repeat values N'])

        yield key,document
    return

