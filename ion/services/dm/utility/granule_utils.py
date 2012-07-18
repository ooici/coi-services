#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/utility/granule_utils.py
@date Wed Jul 18 09:00:22 EDT 2012
@description Utilities for crafting granules into a coverage
'''

from pyon.core.exception import BadRequest
from pyon.ion.granule import TaxyTool, RecordDictionaryTool, build_granule 
from pyon.util.arg_check import validate_equal
from coverage_model.coverage import GridDomain, CRS, AxisTypeEnum, MutabilityEnum, GridShape, SimplexCoverage, RangeDictionary, ParameterContext
import numpy as np
'''
Assuming all values are np.float64 except data which is int8
'''
class CoverageCraft(object):
    tx = TaxyTool()
    tx.add_taxonomy_set('temp','long name for temp')
    tx.add_taxonomy_set('cond','long name for cond')
    tx.add_taxonomy_set('depth','')
    tx.add_taxonomy_set('lat','long name for latitude')
    tx.add_taxonomy_set('lon','long name for longitude')
    tx.add_taxonomy_set('time','long name for time')
    tx.add_taxonomy_set('data', 'arbitrary data uint8')

    def __init__(self,coverage=None, granule=None):
        self.rdt = RecordDictionaryTool(self.tx)
        if coverage is None:
            self.coverage = self.create_coverage()
        else:
            self.coverage = coverage
        
        if granule is not None:
            self.from_granule(granule)


    def from_granule(self, granule):
        tt = TaxyTool.load_from_granule(granule)
        validate_equal(tt,self.tx, "The taxonomies don't match up.")
        rdt = RecordDictionaryTool.load_from_granule(granule)
        self.rdt = rdt
        return rdt

    def add_granule(self, granule=None):
        if granule is not None:
            self.from_granule(granule)
        start_index = self.coverage.num_timesteps 
        elements = self.rdt._shp[0]
        if self.coverage.num_timesteps == 1:
            start_index = 0
            self.coverage.insert_timesteps(elements - 1)
        else:
            self.coverage.insert_timesteps(elements)

        for k,v in self.rdt.iteritems():
            print "key: %s" % k
            print "value: %s" % v
            slice_ = slice(start_index,None)
            print "slice: %s" % slice_
            self.coverage.set_parameter_values(k,tdoa=slice_, value=v)

            


    def to_granule(self):
        return build_granule('coverage_craft', self.tx, self.rdt)

    @classmethod
    def create_coverage(cls):
        rdict = RangeDictionary()
        rdict.items = {
            'coords' : ['time', 'lat', 'lon','depth'],
            'vars'   : ['temp', 'cond', 'data']
        }

        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT, AxisTypeEnum.HEIGHT])

        tdom = GridDomain(GridShape('temporal'), tcrs, MutabilityEnum.EXTENSIBLE)
        sdom = GridDomain(GridShape('spatial', [1]), scrs, MutabilityEnum.IMMUTABLE) # Dimensionality is excluded for now
    
        scov = SimplexCoverage('sample grid coverage_model', rdict, sdom, tdom)

        for val in cls.tx._by_nick_names.iterkeys():
            cls.create_parameters(scov,rdict,val)

        return scov

    @classmethod
    def create_parameters(cls, coverage,rdict, parameter):
        if parameter is not 'data':
            pcontext = ParameterContext(parameter, param_type=np.float64)
        else:
            pcontext = ParameterContext(parameter, param_type=np.int8)
        #--------------------------------------------------------------------------------
        # Coordinate parameters
        #--------------------------------------------------------------------------------
        if parameter is "time":
            pcontext.uom = 'urn:ogc:def:uom:UCUM::s'
            pcontext.description = 'time'
            pcontext.fill_value = np.float64(0e0)
            pcontext.axis = AxisTypeEnum.TIME
        elif parameter is "lat":
            pcontext.uom = "degrees_north"
            pcontext.description = 'wgs84 latitude'
            pcontext.fill_value = np.float64(0e0)
            pcontext.axis = AxisTypeEnum.LAT
        elif parameter is "lon":
            pcontext.uom = "degrees_east"
            pcontext.description = 'wgs84 longitude'
            pcontext.fill_value = np.float64(0e0)
            pcontext.axis = AxisTypeEnum.LON
        elif parameter is "depth":
            pcontext.uom = 'urn:ogc:def:uom:UCUM::m'
            pcontext.description = 'depth in metres'
            pcontext.fill_value = np.float64(0e0)
            pcontext.axis = AxisTypeEnum.HEIGHT
        #--------------------------------------------------------------------------------
        # Value parameters
        #--------------------------------------------------------------------------------
        elif parameter is "temp":
            pcontext.uom = 'urn:ogc:def:uom:UCUM::c'
            pcontext.description = 'degrees centigrade'
            pcontext.fill_value = np.float64(0e0)
        elif parameter is "cond":
            pcontext.uom = "???"
            pcontext.description = 'conductivity'
            pcontext.fill_value = np.float64(0e0)
        elif parameter is "data":
            pcontext.uom = "byte"
            pcontext.description = 'arbitrary data'
            pcontext.fill_value = np.int8(0x0)
        else:
            raise BadRequest('The parameter is not explicitly defined and therefore not accepted')

        coverage.append_parameter(pcontext)
         


    def build_coverage(self):
        pass

