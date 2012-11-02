import os
import urllib
import xml.dom.minidom
from xml.dom.minidom import parse, parseString

from pyon.public import CFG
from pyon.ion.process import StandaloneProcess
from coverage_model.coverage import SimplexCoverage

class RegistrationProcess(StandaloneProcess):

    def on_start(self):
        #these values should come in from a config file, maybe pyon.yml
        self.pydap_url = CFG.dap.pydap_url # 'http://localhost:8001/'
        self.pydap_data_path = CFG.dap.pydap_data_path # '/Users/tgiguere/myserver/data/'
        self.datasets_xml_path = CFG.dap.datasets_xml_path # '/Library/Tomcat/content/erddap/datasets.xml'

    def register_dap_dataset(self, coverage_path):
        self.   add_dataset_to_xml(coverage_path=coverage_path)
        self.create_symlink(coverage_path, self.pydap_data_path)

    def create_symlink(self, coverage_path, pydap_path):
        paths = os.path.split(coverage_path)
        os.symlink(coverage_path, pydap_path + paths[1])

    def add_dataset_to_xml(self, coverage_path):
        dom1 = parse(self.datasets_xml_path)
        dom2 = parseString(self.get_dataset_xml(coverage_path))
        erddap_datasets_element = dom1.getElementsByTagName('erddapDatasets')[0]
        erddap_datasets_element.appendChild(dom2.getElementsByTagName('dataset')[0])

        f = open(self.datasets_xml_path, 'w')
        dom1.writexml(f)
        f.close()

    def get_dataset_xml(self, coverage_path):

        result = ''

        paths = os.path.split(coverage_path)
        cov = SimplexCoverage.load(coverage_path)
        #ds = open_url(url)
        doc = xml.dom.minidom.Document()

        #Get lists of variables with unique sets of dimensions.
        #Datasets can only have variables with the same sets of dimensions

        datasets = {}
        for key in cov.list_parameters():
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
                text_node = doc.createTextNode('1440')
                reload_element.appendChild(text_node)
                dataset_element.appendChild(reload_element)

                add_attributes_element = doc.createElement('addAttributes')

                atts = {}
                atts['title'] = urllib.unquote(cov.name)
                atts['infoUrl'] = self.pydap_url + paths[1]
                atts['summary'] = cov.name
                atts['institution'] = 'ASA'

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
