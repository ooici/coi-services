import os
import urllib
import xml.dom.minidom
from xml.dom.minidom import parse, parseString
from ion.services.dm.inventory.dataset_management_service import DatasetManagementService
from pyon.ion.process import StandaloneProcess
from coverage_model.coverage import SimplexCoverage
from pyon.util.file_sys import FileSystem
from pyon.util.log import log
import numpy as np
import base64
import StringIO
from zipfile import ZipFile

class RegistrationProcess(StandaloneProcess):

    def on_start(self):
        #these values should come in from a config file, maybe pyon.yml
        self.pydap_host = self.CFG.get_safe('server.pydap.host', 'localhost')
        self.pydap_port = self.CFG.get_safe('server.pydap.port', '8001')
        self.pydap_url  = 'http://%s:%s/' % (self.pydap_host, self.pydap_port)
        self.pydap_data_path = self.CFG.get_safe('server.pydap.data_path', 'RESOURCE:ext/pydap')
        self.datasets_xml_path = self.CFG.get_safe('server.pydap.datasets_xml_path', "RESOURCE:ext/datasets.xml")
        self.pydap_data_path = FileSystem.get_extended_url(self.pydap_data_path) + '/'

        filename = self.datasets_xml_path.split('/')[-1]
        base = '/'.join(self.datasets_xml_path.split('/')[:-1])
        real_path = FileSystem.get_extended_url(base)
        self.datasets_xml_path = os.path.join(real_path, filename)
        self.setup_filesystem(real_path)

    def setup_filesystem(self, path):
        if os.path.exists(os.path.join(path,'datasets.xml')):
            return
        zip_str = base64.decodestring(datasets_xml_zip)
        zip_file = StringIO.StringIO()
        zip_file.write(zip_str)
        with ZipFile(zip_file) as zipper:
            zipper.extract('datasets.xml', path)
        zip_file.close()



    def register_dap_dataset(self, dataset_id, data_product_name=''):
        coverage_path = DatasetManagementService._get_coverage_path(dataset_id)
        try:
            self.add_dataset_to_xml(coverage_path=coverage_path, product_name=data_product_name)
            self.create_symlink(coverage_path, self.pydap_data_path)
        except:
            log.exception('Problem registering dataset')
            log.error('Failed to register dataset for coverage path %s' % coverage_path)

    def create_symlink(self, coverage_path, pydap_path):
        paths = os.path.split(coverage_path)
        os.symlink(coverage_path, pydap_path + paths[1])

    def add_dataset_to_xml(self, coverage_path, product_name=''):
        dom1 = parse(self.datasets_xml_path)
        dom2 = parseString(self.get_dataset_xml(coverage_path,product_name))
        erddap_datasets_element = dom1.getElementsByTagName('erddapDatasets')[0]
        erddap_datasets_element.appendChild(dom2.getElementsByTagName('dataset')[0])

        f = open(self.datasets_xml_path, 'w')
        dom1.writexml(f)
        f.close()

    def get_dataset_xml(self, coverage_path, product_name=''):

        result = ''

        paths = os.path.split(coverage_path)
        #cov = SimplexCoverage.pickle_load(coverage_path)
        cov = SimplexCoverage.load(coverage_path)
        #ds = open_url(url)
        doc = xml.dom.minidom.Document()

        #Get lists of variables with unique sets of dimensions.
        #Datasets can only have variables with the same sets of dimensions

        datasets = {}
        for key in cov.list_parameters():
            pc = cov.get_parameter_context(key)
            if np.dtype(pc.param_type.value_encoding).char == 'O':
                continue
            param = cov.get_parameter(key)
            dims = (cov.temporal_parameter_name,)
            if len(param.shape) == 2:
                dims = (cov.temporal_parameter_name, cov.spatial_domain.shape.name)

            if not dims in datasets.keys():
                datasets[dims] = []

            datasets[dims].append(key)

        index = 0
        for dims, vars in datasets.iteritems():

            if not (len(dims) == 1 and dims[0] == vars[0]):
                dataset_element = doc.createElement('dataset')
                dataset_element.setAttribute('type', 'EDDGridFromDap')
                dataset_element.setAttribute('datasetID', '{0}_{1}'.format(paths[1], index))
                dataset_element.setAttribute('active', 'True')

                source_element = doc.createElement('sourceUrl')
                text_node = doc.createTextNode(self.pydap_url + paths[1])
                source_element.appendChild(text_node)
                dataset_element.appendChild(source_element)

                reload_element = doc.createElement('reloadEveryNMinutes')
                text_node = doc.createTextNode('5')
                reload_element.appendChild(text_node)
                dataset_element.appendChild(reload_element)

                add_attributes_element = doc.createElement('addAttributes')

                atts = {}
                atts['title'] = product_name or urllib.unquote(cov.name)
                atts['infoUrl'] = self.pydap_url + paths[1]
                atts['summary'] = cov.name
                atts['institution'] = 'OOI'

                for key, val in atts.iteritems():
                    att_element = doc.createElement('att')
                    att_element.setAttribute('name', key)
                    text_node = doc.createTextNode(val)
                    att_element.appendChild(text_node)
                    add_attributes_element.appendChild(att_element)

                if len(add_attributes_element.childNodes) > 0:
                    dataset_element.appendChild(add_attributes_element)

                for var_name in vars:
                    param = cov.get_parameter(var_name)
                    var = param.context

                    units = var.uom

                    if len(param.shape) >=1 and not param.is_coordinate: #dataVariable
                        data_element = doc.createElement('dataVariable')
                        source_name_element = doc.createElement('sourceName')
                        text_node = doc.createTextNode(var.name)
                        source_name_element.appendChild(text_node)
                        data_element.appendChild(source_name_element)

                        destination_name_element = doc.createElement('destinationName')
                        text_node = doc.createTextNode(var.name)
                        destination_name_element.appendChild(text_node)
                        data_element.appendChild(destination_name_element)

                        add_attributes_element = doc.createElement('addAttributes')
                        if not var.attributes is None:
                            for key, val in var.attributes.iteritems():
                                att_element = doc.createElement('att')
                                att_element.setAttribute('name', key)
                                text_node = doc.createTextNode(val)
                                att_element.appendChild(text_node)
                                add_attributes_element.appendChild(att_element)

                        att_element = doc.createElement('att')
                        att_element.setAttribute('name', 'ioos_category')
                        text_node = doc.createTextNode(self.get_ioos_category(var.name, units))
                        att_element.appendChild(text_node)
                        add_attributes_element.appendChild(att_element)

                        data_element.appendChild(add_attributes_element)

                        dataset_element.appendChild(data_element)

                        #add category
                        #add long_name
                        #add standard_name
                        #add units

                for dim_name in dims: #axisVariable
                    param = cov.get_parameter(dim_name)
                    dim = param.context

                    units = var.uom

                    axis_element = doc.createElement('axisVariable')
                    source_name_element = doc.createElement('sourceName')
                    text_node = doc.createTextNode(dim.name)
                    source_name_element.appendChild(text_node)
                    axis_element.appendChild(source_name_element)

                    destination_name_element = doc.createElement('destinationName')
                    text_node = doc.createTextNode(dim.name)
                    destination_name_element.appendChild(text_node)
                    axis_element.appendChild(destination_name_element)

                    add_attributes_element = doc.createElement('addAttributes')
                    if not dim.attributes is None:
                        for key, val in dim.attributes.iteritems():
                            att_element = doc.createElement('att')
                            att_element.setAttribute('name', key)
                            text_node = doc.createTextNode(val)
                            att_element.appendChild(text_node)
                            add_attributes_element.appendChild(att_element)

                    att_element = doc.createElement('att')
                    att_element.setAttribute('name', 'ioos_category')
                    text_node = doc.createTextNode(self.get_ioos_category(dim.name, units))
                    att_element.appendChild(text_node)
                    add_attributes_element.appendChild(att_element)

                    axis_element.appendChild(add_attributes_element)

                    dataset_element.appendChild(axis_element)

                index += 1
                result += dataset_element.toprettyxml() + '\n'

        cov.close()

        return result

    def get_ioos_category(self, var_name, units):
        if var_name.find('sigma') >= 0 and var_name.find('theta') >= 0:
            return 'Physical Oceanography'

        elif var_name.find('count')  >= 0 or\
             var_name.find('stddev') >= 0 or\
             var_name.find('sd') >= 0 or\
             var_name.find('s.d.') >= 0 or\
             var_name.find('variance') >= 0 or\
             var_name.find('confidence') >= 0 or\
             var_name.find('precision') >= 0 or\
             var_name.find('error') >= 0 or\
             var_name.find('number') >= 0 or\
             var_name.find('radiusinfluencegridpoints') >= 0 or\
             var_name.find('standarddeviation') >= 0 or\
             var_name.find('standarderror') >= 0:
            return 'Statistics'	#catch statistics first    including special cases from WOA 2001

        elif var_name.find('sigma') >= 0:
            #ambiguous   statistics or pressure
            if var_name.find('coordinate') or\
               var_name.find('level'):
                return 'Location'
            else:
                return 'Unknown'

        elif var_name.find('qc')           >= 0 or\
             var_name.find('qa')           >= 0 or\
             (var_name.find('quality') >= 0 and var_name.find('sciencequality') < 0) or\
             var_name.find('flag')         >= 0:
            return 'Quality'


        elif var_name.find('bathym')       >= 0 or\
             var_name.find('topo')         >= 0:
            return 'Bathymetry'

        elif var_name.find('birth')        >= 0 or\
             var_name.find('chorion')      >= 0 or\
             var_name.find('diet')         >= 0 or\
             var_name.find('disease')      >= 0 or\
             var_name.find('egg')          >= 0 or\
             var_name.find('food')         >= 0 or\
             var_name.find('larv')         >= 0 or\
             var_name.find('myomere')      >= 0 or\
             var_name.find('sex')        >= 0 or\
             var_name.find('stage')        >= 0 or\
             var_name.find('yolk')         >= 0:
            return 'Biology'

        elif var_name.find('co2')          >= 0 or\
             var_name.find('carbonate')    >= 0 or\
             var_name.find('co3')          >= 0 or\
             var_name.find('carbondioxide')>= 0:
            return 'CO2'

        elif var_name.find('cfc11')        >= 0 or\
             var_name.find('debris')       >= 0 or\
             var_name.find('freon')        >= 0 or\
             var_name.find('ozone')        >= 0:
            return 'Contaminants'

        elif var_name.find('ammonia')      >= 0 or\
             var_name.find('ammonium')     >= 0 or\
             var_name.find('nn')        >= 0 or\
             var_name.find('nh3')          >= 0 or\
             var_name.find('nh4')          >= 0 or\
             var_name.find('nitrate')      >= 0 or\
             var_name.find('nitrite')      >= 0 or\
             var_name.find('no2')          >= 0 or\
             var_name.find('no3')          >= 0 or\
             var_name.find('phosphate')    >= 0 or\
             var_name.find('po4')          >= 0 or\
             var_name.find('silicate')     >= 0 or\
             var_name.find('si')         >= 0:
            return 'Dissolved Nutrients'

        #Sea Level before Location and Currents so tide is caught correctly
        elif (var_name.find('geopotential') >= 0 and var_name.find('height') >= 0) or\
             var_name.find('ssh')                   >= 0 or\
             var_name.find('surfel')               >= 0 or\
             (var_name.find('sea') >= 0 and var_name.find('surface') >= 0 and\
              var_name.find('wave') < 0 and\
              (var_name.find('elevation') >= 0 or var_name.find('height') >= 0)):
            return 'Sea Level'

        elif (var_name.find('ocean') >= 0 and var_name.find('streamfunction') >= 0) or\
             var_name.find('momentumcomponent') >= 0 or\
             var_name.find('momentumstress')    >= 0 or\
             var_name.find('u-flux')           >= 0 or\
             var_name.find('v-flux')           >= 0 or\
             var_name.find('current')            >= 0 or\
             var_name.find('waterdir')          >= 0 or\
             (var_name.find('water') >= 0 and\
              (var_name.find('direction')   >= 0 or\
               var_name.find('speed')       >= 0 or\
               var_name.find('spd')         >= 0 or\
               var_name.find('vel')         >= 0 or\
               var_name.find('velocity')    >= 0)):
            return 'Currents'

        elif (var_name.find('o2') >= 0 and var_name.find('co2') < 0) or\
             var_name.find('oxygen')       >= 0:
            return 'Dissolved O2'

        elif var_name.find('predator')     >= 0 or\
             var_name.find('prey')         >= 0 or\
             var_name.find('troph')       >= 0:  #don't catch geostrophic
            return 'Ecology'

        elif ((var_name.find('heat') >= 0 or var_name.find('radiative') >= 0) and\
              (var_name.find('flux') >= 0 or var_name.find('flx') >= 0)) or\
             var_name.find('shortwave')      >= 0 or\
             var_name.find('longwave')       >= 0 or\
             var_name.find('hflx')           >= 0 or\
             var_name.find('lflx')           >= 0 or\
             var_name.find('sflx')           >= 0:
            return 'Heat Flux'

        #see Hydrology below
        elif var_name.find('ice')         >= 0 or\
             var_name.find('snow')        >= 0:
            return 'Ice Distribution'

        elif var_name.find('mask')       >= 0 or\
             var_name.find('id')         >= 0 or\
             var_name.find('siteid')      >= 0 or\
             var_name.find('stationid')   >= 0 or\
             var_name.find('stationid')    >= 0 or\
             var_name.find('pi')         >= 0 or\
             var_name.find('project')    >= 0:
            return 'Identifier'

        #see Location below
        elif var_name.find('cldc')         >= 0 or\
             var_name.find('cloud')        >= 0 or\
             var_name.find('cloud')        >= 0 or\
             var_name.find('dew point')    >= 0 or\
             var_name.find('dewp')         >= 0 or\
             var_name.find('evapora')      >= 0 or\
             (var_name.find('front') >= 0 and var_name.find('probability') >= 0) or\
             var_name.find('humidity')     >= 0 or\
             var_name.find('precipita')    >= 0 or\
             var_name.find('rain')        >= 0 or\
             var_name.find('rhum')         >= 0 or\
             var_name.find('shum')       >= 0 or\
             var_name.find('total electron content') >= 0 or\
             var_name.find('visi')         >= 0:
            return 'Meteorology'

        elif var_name.find('chlor')        >= 0 or\
             var_name.find('chla')         >= 0 or\
             var_name.find('chla')       >= 0 or\
             var_name.find('k490')         >= 0 or\
             var_name.find('par')        >= 0:
            return 'Ocean Color'

        elif var_name.find('optical')      >= 0 or\
             var_name.find('rrs')         >= 0 or\
             var_name.find('667')          >= 0 or\
             var_name.find('fluor')        >= 0:
            return 'Optical Properties'

        #Physical Oceanography, see below
        elif var_name.find('phytoplankton') >= 0:\
            return 'Phytoplankton Abundance'

        elif var_name.find('aprs')         >= 0 or\
             var_name.find('ptdy')         >= 0 or\
             var_name.find('pressure')     >= 0 or\
             var_name.find('mbar')       >= 0 or\
             var_name.find('hpa')        >= 0:
            return 'Pressure'

        elif var_name.find('productivity') >= 0 or\
             var_name.find('primprod')    >= 0 or\
             var_name.find('primprod')     >= 0:
            return 'Productivity'

        #see Quality above
        elif var_name.find('ph')         >= 0 or\
             var_name.find('pss')          >= 0 or\
             (var_name.find('psu')         >= 0 and var_name.find('psue') < 0) or\
             var_name.find('salinity')     >= 0 or\
             var_name.find('salt')         >= 0 or\
             var_name.find('conductivity') >= 0:
            return 'Salinity'

        #see Sea Level above
        elif var_name.find('soil')         >= 0:
            return 'Soils'


        #see Statistics above
        elif (var_name.find('surf') >= 0 and var_name.find('roughness') >= 0) or\
             var_name.find('awpd')         >= 0 or\
             var_name.find('dwpd')         >= 0 or\
             var_name.find('mwvd')         >= 0 or\
             var_name.find('wvht')         >= 0 or\
             (var_name.find('wave') >= 0 and var_name.find('short') < 0 and var_name.find('long') < 0):
            return 'Surface Waves'

        elif var_name.find('phylum')       >= 0 or\
             var_name.find('order')        >= 0 or\
             var_name.find('family')       >= 0 or\
             var_name.find('genus')        >= 0 or\
             var_name.find('genera')       >= 0 or\
             var_name.find('species')      >= 0 or\
             var_name.find('sp.')          >= 0 or\
             var_name.find('spp')          >= 0 or\
             var_name.find('stock')        >= 0 or\
             var_name.find('taxa')         >= 0 or\
             var_name.find('scientific')   >= 0 or\
             var_name.find('vernacular')   >= 0 or\
             var_name.find('commonname')   >= 0:
            return 'Taxonomy'

        elif var_name.find('airtemp')              >= 0 or\
             var_name.find('airtemp')             >= 0 or\
             var_name.find('atemp')                >= 0 or\
             var_name.find('atmp')                 >= 0 or\
             var_name.find('sst')                  >= 0 or\
             var_name.find('temperature')          >= 0 or\
             var_name.find('temp')                 >= 0 or\
             var_name.find('wtmp')                 >= 0 or\
             var_name.find('wtemp')                >= 0 or\
             self.is_degrees_c(units) or\
             self.is_degrees_f(units) or\
             self.is_degrees_k(units):

            return 'Temperature'

        elif (var_name.find('time') >= 0 and var_name.find('time-averaged') < 0) or\
             var_name.find('year')        >= 0 or\
             var_name.find('month')       >= 0 or\
             var_name.find('date')        >= 0 or\
             var_name.find('day')         >= 0 or\
             var_name.find('hour')        >= 0 or\
             var_name.find('minute')      >= 0 or\
             var_name.find('second')     >= 0 or\
             var_name.find('seconds')    >= 0:  #e.g., but not meters/second  secondRef
            return 'Time'

        elif ((var_name.find('atmosphere')     >= 0 or var_name.find('air')    >= 0) and\
              (var_name.find('streamfunction') >= 0 or var_name.find('stress') >= 0)) or\
             var_name.find('momentumflux')>= 0 or\
             var_name.find('u-flux')     >= 0 or\
             var_name.find('v-flux')     >= 0 or\
             var_name.find('uwnd')         >= 0 or\
             var_name.find('vwnd')         >= 0 or\
             var_name.find('xwnd')         >= 0 or\
             var_name.find('ywnd')         >= 0 or\
             var_name.find('wdir')         >= 0 or\
             var_name.find('wspd')         >= 0 or\
             var_name.find('wgst')         >= 0 or\
             var_name.find('wspu')         >= 0 or\
             var_name.find('wspv')         >= 0 or\
             var_name.find('wind')         >= 0:
            return 'Wind'

        #Physical Oceanography here to catch 'stress' other than wind
        #this can be very inclusive
        elif var_name.find('stress')       >= 0 or\
             var_name.find('density')      >= 0 or\
             var_name.find('erosion')      >= 0 or\
             var_name.find('sand')       >= 0 or\
             var_name.find('sediment')     >= 0 or\
             var_name.find('roughness')    >= 0 or\
             var_name.find('tide')         >= 0 or\
             var_name.find('tidal')        >= 0 or\
             var_name.find('mixedlayer')  >= 0:
            return 'Physical Oceanography'

        elif var_name.find('zooplankton') >= 0:  #not a great test
            return 'Zooplankton Abundance'

        #Hydrology near end, so likely to catch other categories first (e.g., Temperature)
        elif var_name.find('runoff')         >= 0 or\
             var_name.find('waterfluxintoocean') >= 0 or\
             (var_name.find('stream') >= 0 and var_name.find('flow') >= 0) or\
             (var_name.find('surface') >= 0 and var_name.find('water') >= 0):
            return 'Hydrology'

        #catch Location last   so e.g., ocean_salt_x_transport caught by Salinity
        elif var_name.find('altitude')     >= 0 or\
             (var_name.find('depth')       >= 0 and var_name.find('mixedlayer') < 0) or\
             var_name.find('geox')         >= 0 or\
             var_name.find('geoy')         >= 0 or\
             var_name.find('lon')        >= 0 or\
             var_name.find('longitude')    >= 0 or\
             var_name.find('lat')        >= 0 or\
             var_name.find('latitude')     >= 0 or\
             var_name.find('x')          >= 0 or\
             var_name.find('xax')          >= 0 or\
             var_name.find('xpos')       >= 0 or\
             var_name.find('y')          >= 0 or\
             var_name.find('yax')          >= 0 or\
             var_name.find('ypos')       >= 0 or\
             var_name.find('z')          >= 0 or\
             var_name.find('zax')          >= 0 or\
             var_name.find('zpos')       >= 0 or\
             var_name.find('zlev')         >= 0 or\
             var_name.find('csw')       >= 0 or\
             var_name.find('etarho')    >= 0 or\
             var_name.find('etau')      >= 0 or\
             var_name.find('etav')      >= 0 or\
             var_name.find('srho')      >= 0 or\
             var_name.find('sw')        >= 0 or\
             var_name.find('xirho')     >= 0 or\
             var_name.find('xiu')       >= 0 or\
             var_name.find('xiv')       >= 0 or\
             var_name.find('nsites')     >= 0 or\
             var_name.find('srs')        >= 0 or\
             var_name.find('datum')      >= 0 or\
             var_name.find('vertdatum')  >= 0 or\
             var_name.find('location')     >= 0 or\
             var_name.find('locality')     >= 0 or\
             var_name.find('region')     >= 0 or\
             var_name.find('sites')      >= 0 or\
             var_name.find('city')       >= 0 or\
             var_name.find('county')     >= 0 or\
             var_name.find('province')   >= 0 or\
             var_name.find('state')      >= 0 or\
             var_name.find('country')    >= 0 or\
             var_name.find('fips')        >= 0:
            return 'Location'

        else:
            return 'Unknown'

    def is_degrees_c(self, units):
        if not units or units == '':
            return False
        degree_units = ['c', 'celsius', 'degreecentigrade', 'degreecelsius', 'degreescelsius',\
                        'degc', 'degreec', 'degreesc', 'degreec', 'degreesc', 'degc', 'degsc',\
                        'cel', '(degc)']
        return str.lower(units) in degree_units

    def is_degrees_f(self, units):
        if not units or units == '':
            return False
        degree_units = ['f', 'fahrenheit', 'degreefahrenheit', 'degreesfahrenheit',\
                        'degf', 'degreef', 'degreesf', 'degreef', 'degreesf', 'degf', 'degsf',\
                        '[degf]']
        return str.lower(units) in degree_units

    def is_degrees_k(self, units):
        if not units or units == '':
            return False
        degree_units = ['kelvin', 'degreekelvin', 'degreeskelvin',\
                        'degk', 'degreek', 'degreesk', 'degreek', 'degreesk', 'degk', 'degsk',\
                        'k']
        return str.lower(units) in degree_units

datasets_xml_zip = """
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
"""
