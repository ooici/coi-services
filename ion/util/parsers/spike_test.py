#!/usr/bin/env python
'''
@author Tim G
@date Wed Mar 27
'''

from csv import DictReader
from StringIO import StringIO

def spike_parser(document):
    '''
    This parser YIELDS a document per call or until it's done
    The format for the document is CSV in this table format
    Array,Instrument Class,Reference Designator,Data Product,Units,ACC,N,L

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
            origin: Data Product
            type: String
        units:
            origin: Units
            type: String
        acc:
            origin: ACC
            type: String
        spike_n:
            origin: N
            type: float64
        spike_l:
            origin: L
            type: float64

    '''
    sio = StringIO()
    sio.write(document)
    sio.seek(0)


    dr = DictReader(sio)
    for row in dr:
        key = '_'.join(['spike', row['Reference Designator'], row['Data Product']])
        document = {}
        document['array']                = row['Array']
        document['instrument_class']     = row['Instrument Class']
        document['reference_designator'] = row['Reference Designator']
        document['data_product_in']      = row['Data Product']
        document['units']                = row['Units']
        document['acc']                  = row['ACC']
        document['spike_n']              = float(row['N'])
        document['spike_l']              = float(row['L'])
        yield key,document
    return
