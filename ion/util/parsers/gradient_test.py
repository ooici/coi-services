#!/usr/bin/env python
'''
@author Luke C
@date Tue Mar 26 16:00:21 EDT 2013
'''

from csv import DictReader
from StringIO import StringIO
from pyon.util.log import log

def gradient_test_parser(document):
    '''
    This parser YIELDS a document per call or until it's done
    The format for the document is CSV in this table format
    Array,Instrument Class,Reference Designator,Data Product used as Input Data (DAT),Data Product used as Input Parameter X,Units of DAT,Units of X,DDATDX,MINDX,STARTDAT,TOLDAT

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
        dat:
            origin: Data Product used as Input Data (DAT)
            type: String
        x:
            origin: Data Product used as Input Parameter X
            type: String
        units_dat:
            origin: Units of DAT
            type: String
        units_x:
            origin: Units of X
            type: String
        d_dat_dx:
            origin: DDATDX
            type: float64
        min_dx:
            origin: MINDX
            type: float64
        start_dat:
            origin: STARTDAT
            type: float64
        tol_dat:
            origin: TOLDAT
            type: float64

    '''
    sio = StringIO()
    sio.write(document)
    sio.seek(0)

    dr = DictReader(sio)
    for row in dr:
        try:
            key = '_'.join(['grad',row['Reference Designator'],row['Data Product used as Input Data (DAT)'],row['Data Product used as Input Parameter X']])

            document = {}
            document['array']                = row['Array']
            document['instrument_class']     = row['Instrument Class']
            document['reference_designator'] = row['Reference Designator']
            document['dat']                  = row['Data Product used as Input Data (DAT)']
            document['x']                    = row['Data Product used as Input Parameter X']
            document['units_dat']            = row['Units of DAT']
            document['units_x']              = row['Units of X']
            document['d_dat_dx']             = float(row['DDATDX'])
            document['min_dx']               = float(row['MINDX']) if row['MINDX'] else 0.
            document['start_dat']            = float(row['STARTDAT']) if row['STARTDAT'] else 0.
            document['tol_dat']              = float(row['TOLDAT'])
            yield key,document
        except TypeError:
            log.exception("Couldn't parse row")
            continue
        except ValueError:
            log.exception("Couldn't parse row")
            continue
    return

