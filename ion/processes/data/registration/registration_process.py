from pyon.core.exception import BadRequest, NotFound
from pyon.ion.process import StandaloneProcess
from pyon.util.file_sys import FileSystem
from pyon.util.log import log
from pyon.public import PRED
from interface.objects import ParameterFunctionType as PFT

from ion.services.dm.inventory.dataset_management_service import DatasetManagementService

from coverage_model import AbstractCoverage, ParameterFunctionType
from coverage_model.parameter_functions import PythonFunction, NumexprFunction
#from coverage_model.parameter_types import QuantityType

from xml.dom.minidom import parse, parseString
from xml.parsers.expat import ExpatError
from zipfile import ZipFile
from jinja2 import Environment, FileSystemLoader
import lxml.etree as etree

import base64
import os
import urllib
import xml.dom.minidom
import StringIO
import re

class RegistrationProcess(StandaloneProcess):

    def on_start(self):
        #these values should come in from a config file, maybe pyon.yml
        self.pydap_host = self.CFG.get_safe('server.pydap.host', 'localhost')
        self.pydap_port = self.CFG.get_safe('server.pydap.port', '8001')
        self.pydap_url  = 'http://%s:%s/' % (self.pydap_host, self.pydap_port)
        self.pydap_data_path = self.CFG.get_safe('server.pydap.data_path', 'RESOURCE:ext/pydap')
        self.datasets_xml_path = self.get_datasets_xml_path(self.CFG)
        self.pydap_data_path = FileSystem.get_extended_url(self.pydap_data_path) + '/'
        self.ux_url = self.CFG.get_safe('system.web_ui_url','http://localhost:3000/')
        self.jenv = Environment(loader=FileSystemLoader('res/templates'), trim_blocks=True, lstrip_blocks=True)
        self.resource_registry = self.container.resource_registry
        self.setup_filesystem(self.datasets_xml_path)

    @classmethod
    def get_datasets_xml_path(cls, cfg):
        datasets_xml_path = cfg.get_safe('server.pydap.datasets_xml_path', 'RESOURCE:ext/datasets.xml')
        base, filename = os.path.split(datasets_xml_path)
        base = FileSystem.get_extended_url(base)
        path = os.path.join(base, filename)

        return path

    def setup_filesystem(self, path):
        template = self.jenv.get_template('datasets.xml')
        buff = template.render()
        if not os.path.exists(path):
            with open(path,'w') as f:
                f.write(buff)
        try:
            doc = self.container.object_store.read('datasets.xml')
        except NotFound:
            doc = {'xml':buff}
            self.container.object_store.create_doc(doc, object_id='datasets.xml')


    def dap_entry(self, data_product_id):
        dp = self.container.resource_registry.read(data_product_id)
        self.create_entry(dp)
        self.touch(data_product_id)

    def slam(self, d, dp, k):
        v = getattr(dp, k, None)
        if v:
            d[k] = v

    def map_data_product(self, data_product):
        ds = {} # Catalog Dataset
        ds['dataset_id'] = 'data' + data_product._id
        ds['url'] = self.pydap_url + data_product._id
        ds['face_page'] = self.ux_url + 'DataProduct/face/' + data_product._id
        ds['title'] = data_product.name
        ds['summary'] = data_product.description or data_product.name
        ds['attrs'] = {}
        metadata_attrs = [
            'comment',
            'ooi_short_name',
            'ooi_product_name',
            'regime',
            'qc_cmbnflg',
            'qc_glblrng',
            'qc_gradtst',
            'qc_loclrng',
            'qc_spketest',
            'qc_stuckvl',
            'qc_trndtst',
            'dps_dcn',
            'flow_diagram_dcn',
            'doors_l2_requirement_num',
            'doors_l2_requirement_text',
            'provenance_description',
            'citation_description',
            'lineage_description',
            'ioos_category',
            'iso_spatial_representation_type',
            'processing_level_code',
            'ISO_spatial_representation_type',
            'license_uri',
            'exclusive_rights_status',
            'exclusive_rights_end_date',
            'exclusive_rights_notes',
            'acknowledgement'
        ]
        for attr in metadata_attrs:
            self.slam(ds['attrs'], data_product, attr)
        if data_product.synonyms:
            ds['attrs']['synonyms'] = ','.join(data_product.synonyms)
        if data_product.iso_topic_category:
            ds['attrs']['iso_topic_category'] = ','.join(data_product.iso_topic_category)
        if data_product.reference_urls:
            ds['attrs']['reference_urls'] = '\n'.join(data_product.reference_urls)

        # Grab the parameters
        stream_def_id = self.resource_registry.find_objects(data_product._id, PRED.hasStreamDefinition, id_only=True)[0][0]
        pdict_id = self.resource_registry.find_objects(stream_def_id, PRED.hasParameterDictionary, id_only=True)[0][0]
        parameter_contexts, _ = self.resource_registry.find_objects(pdict_id, PRED.hasParameterContext, id_only=False)

        ds['vars'] = []
        for param in parameter_contexts:
            # Handle the placeholder variables
            if re.match(r'.*_[a-z0-9]{32}', param.name):
                continue # Let's not do this
            var = {}
            var['name'] = param.name
            var['attrs'] = {}
            attrs = var['attrs']
            attrs['units'] = param.units or '1'
            attrs['ioos_category'] = self.get_ioos_category(param.name, attrs['units'])
            attrs['long_name'] = param.display_name
            if param.standard_name: 
                attrs['standard_name'] = param.standard_name
            if 'seconds' in attrs['units'] and 'since' in attrs['units']:
                attrs['time_precision'] =  '1970-01-01T00:00:00.000Z'
            if param.ooi_short_name:
                sname = param.ooi_short_name
                sname = re.sub('[\t\n ]+', ' ', sname)
                attrs['ooi_short_name'] = sname
                m = re.match(r'[A-Z0-9]{7}', sname)
                if m:
                    reference_url = 'https://confluence.oceanobservatories.org/display/instruments/' + m.group()
                    attrs['references'] = reference_url
                if 'L2' in param.ooi_short_name:
                    attrs['data_product_level'] = 'L2'
                    attrs['source'] = 'level 2 calibrated sensor observation'
                elif 'L1' in param.ooi_short_name:
                    attrs['data_product_level'] = 'L1'
                    attrs['source'] = 'level 1 calibrated sensor observation'
                elif 'L0' in param.ooi_short_name:
                    attrs['data_product_level'] = 'L0'
                    attrs['source'] = 'level 0 calibrated sensor observation'
                elif 'QC' in param.ooi_short_name:
                    attrs['data_product_level'] = 'QC'
            elif param.parameter_type != 'function':
                if attrs['units'] == 'counts':
                    attrs['data_product_level'] = 'L0'
                    attrs['source'] = 'sensor observation'
                elif 'seconds' in attrs['units'] and 'since' in attrs['units']:
                    attrs['data_product_level'] = 'axis'
                elif var['name'].lower() in ('time', 'lat', 'lon', 'latitude', 'longitude'):
                    attrs['data_product_level'] = 'axis'
                else:
                    attrs['data_product_level'] = 'unknown'
            if param.reference_urls:
                attrs['instrument_type'] = '\n'.join(param.reference_urls)

            if param.parameter_type == 'function':
                parameter_function = self.resource_registry.read(param.parameter_function_id)
                if parameter_function.function_type == PFT.PYTHON:
                    attrs['function_module'] = parameter_function.owner or ''
                    attrs['function_name'] = parameter_function.function or ''
                    if attrs['function_module'].startswith('ion_functions'):
                        s = attrs['function_module']
                        url = s.replace('.','/') + '.py'
                        url = 'https://github.com/ooici/ion-functions/blob/master/' + url
                        attrs['function_url'] = url
                    elif parameter_function.egg_uri:
                        attrs['function_url'] = parameter_function.egg_uri
                elif parameter_function.function_type == PFT.NUMEXPR:
                    attrs['function_name'] = parameter_function.name
                    attrs['expression'] = parameter_function.function

            for k,v in param.additional_metadata:
                if re.match(r'[a-z][a-zA-Z0-9_]+', k):
                    attrs[k] = v
            ds['vars'].append(var)

        return ds

    def create_entry(self, data_product_id):
        data_product = self.resource_registry.read(data_product_id)
        ds = self.map_data_product(data_product)

        template = self.jenv.get_template('dataset.xml')
        entry = template.render(**ds)
        doc = self.container.object_store.read('datasets.xml')
        root = etree.fromstring(doc['xml'])
        dataset_element = etree.fromstring(entry)
        root.append(dataset_element)
        doc['xml'] = etree.tostring(root, xml_declaration=True, encoding='utf8', pretty_print=True)

        self.container.object_store.update_doc(doc)
        with open(self.datasets_xml_path, 'w') as f:
            f.write(doc['xml'])

        
    
    def read_entry(self, data_product_id):
        pass

    def update_entry(self, data_product_id):
        # Make a new XML entry for this data product from a template
        data_product = self.resource_registry.read(data_product_id)
        ds = self.map_data_product(data_product)
        template = self.jenv.get_template('dataset.xml')
        entry = template.render(**ds)
        dataset_element = etree.fromstring(entry)
        # Grab the XML document in the object store
        doc = self.container.object_store.read('datasets.xml')
        root = etree.fromstring(doc['xml'])
        # Remove the existing entries that correspond to this data product
        relevant = []
        for ele in root:
            if 'datasetID' in ele.attrib and ele.attrib['datasetID'] == 'data%s' % data_product_id:
                relevant.append(ele)
        for r in relevant:
            root.remove(ele)
        root.append(dataset_element)
        doc['xml'] = etree.tostring(root, xml_declaration=True, encoding='utf8', pretty_print=True)
        self.container.object_store.update_doc(doc)
        with open(self.datasets_xml_path, 'w') as f:
            f.write(doc['xml'])

    def delete_entry(self, data_product_id):
        pass

    def touch(self, data_product_id):
        '''Touches a file, so that pydap can catalog it'''
        path = os.path.join(self.pydap_data_path, data_product_id)
        with open(path, 'w'):
            pass

    def register_dap_dataset(self, data_product_id):
        return self.dap_entry(data_product_id)
        dataset_id = self.container.resource_registry.find_objects(data_product_id, PRED.hasDataset, id_only=True)[0][0]
        data_product = self.container.resource_registry.read(data_product_id)
        data_product_name = data_product.name
        stream_definition = self.container.resource_registry.find_objects(data_product_id, PRED.hasStreamDefinition, id_only=False)[0][0]
        coverage_path = DatasetManagementService._get_coverage_path(dataset_id)
        try:
            self.add_dataset_to_xml(coverage_path=coverage_path, product_id=data_product_id, product_name=data_product_name, available_fields=stream_definition.available_fields)
            #self.create_symlink(coverage_path, self.pydap_data_path)
        except: # We don't re-raise to prevent clients from bombing out...
            log.exception('Problem registering dataset')
            log.error('Failed to register dataset for coverage path %s' % coverage_path)
    
    def create_symlink(self, coverage_path, pydap_path):
        head, tail = os.path.split(coverage_path)
        if not os.path.exists(os.path.join(pydap_path, tail)):
            os.symlink(coverage_path, os.path.join(pydap_path, tail))

    def add_dataset_to_xml(self, coverage_path, product_id, product_name='', available_fields=None):
        dom1 = parse(self.datasets_xml_path)
        xml_str = self.get_dataset_xml(coverage_path, product_id, product_name, available_fields)
        try:
            dom2 = parseString(xml_str)
        except ExpatError:
            log.exception('Error parsing XML string for %s' % product_name)
            log.error(xml_str)
            raise

        erddap_datasets_element = dom1.getElementsByTagName('erddapDatasets')[0]
        erddap_datasets_element.appendChild(dom2.getElementsByTagName('dataset')[0])

        with open(self.datasets_xml_path, 'w') as f:
            dom1.writexml(f)
    
    def get_errdap_name_map(self, names):
        result = {}
        for name in names:
            if name == 'lon':
                result[name] = 'longitude'
            elif name == 'lat':
                result[name] = 'latitude'
            else:
                result[name] = name
        return result

    def xml_attr(self, doc, head, attr_name, attr_value):
        att_element = doc.createElement('att')
        att_element.setAttribute('name', attr_name)
        text_node = doc.createTextNode(attr_value)
        att_element.appendChild(text_node)
        head.appendChild(att_element)

    def get_dataset_xml(self, coverage_path, product_id, product_name='', available_fields=None):
        #http://coastwatch.pfeg.noaa.gov/erddap/download/setupDatasetsXml.html
        result = ''
        paths = os.path.split(coverage_path)
        cov = AbstractCoverage.load(coverage_path)
        doc = xml.dom.minidom.Document()
        
        #erd_type_map = {'d':'double', 'f':"float", 'h':'short', 'i':'int', 'l':'int', 'q':'int', 'b':'byte', 'b':'char', 'S':'String'} 
        
        #Get lists of variables with unique sets of dimensions.
        #Datasets can only have variables with the same sets of dimensions

        if not cov.list_parameters():
            raise BadRequest('Attempting to register an empty dataset. The coverage (%s) has no definition.\n%s' %(coverage_path, cov))

        datasets = {}
        for key in cov.list_parameters():
            pc = cov.get_parameter_context(key)
            #if getattr(pc, 'visible', None):
            #    continue
            if available_fields and pc.name not in available_fields:
                continue
            #if not isinstance(pc.param_type, QuantityType):
            #    continue

            param = cov.get_parameter(key)
            dims = (cov.temporal_parameter_name,)
            if len(param.shape) == 2:
                dims = (cov.temporal_parameter_name, cov.spatial_domain.shape.name)

            if not dims in datasets.keys():
                datasets[dims] = []
            

            datasets[dims].append(key)
        

        index = 0
        if not datasets:
            raise BadRequest('Attempting to register a dimensionless dataset. The coverage (%s) has no dimension(s).\n%s' %( coverage_path, cov))
        
        for dims, vars in datasets.iteritems():
            erd_name_map = self.get_errdap_name_map(vars) 
            
            if len(vars)==1:
                raise BadRequest('A dataset needs a proper range, not just the temporal dimension. %s\n%s' %( coverage_path, cov))

            dataset_element = doc.createElement('dataset')
            #dataset_element.setAttribute('type', 'EDDGridFromDap')
            dataset_element.setAttribute('type', 'EDDTableFromDapSequence')
            dataset_element.setAttribute('datasetID', 'data' + product_id)
            dataset_element.setAttribute('active', 'True')

            source_element = doc.createElement('sourceUrl')
            text_node = doc.createTextNode(self.pydap_url + paths[1])
            source_element.appendChild(text_node)
            dataset_element.appendChild(source_element)

            reload_element = doc.createElement('reloadEveryNMinutes')
            if self.CFG.get_safe('server.erddap.dataset_caching',True):
                text_node = doc.createTextNode('1440')
            else:
                text_node = doc.createTextNode('5')
            reload_element.appendChild(text_node)
            dataset_element.appendChild(reload_element)
            
            outer_element = doc.createElement('outerSequenceName')
            text_node = doc.createTextNode('data')
            outer_element.appendChild(text_node)
            dataset_element.appendChild(outer_element)

            default_element = doc.createElement('defaultDataQuery')
            text_node = doc.createTextNode('&amp;time&gt;=1970-01-01')
            default_element.appendChild(text_node)
            dataset_element.appendChild(default_element)

            # No longer applicable
            #if self.CFG.get_safe('server.erddap.dataset_caching',True):
                #refresh_interval = self.CFG.get_safe('server.erddap.refresh_interval', 30000)
                #update_element = doc.createElement('updateEveryNMillis')
                #text_node = doc.createTextNode(str(refresh_interval))
                #update_element.appendChild(text_node)
                #dataset_element.appendChild(update_element)
            

            add_attributes_element = doc.createElement('addAttributes')

            atts = {}
            atts['title'] = product_name or urllib.unquote(cov.name)
            atts['infoUrl'] = self.ux_url + 'DataProduct/face/' + product_id
            atts['institution'] = 'OOI'
            atts['Conventions'] = "COARDS, CF-1.6, Unidata Dataset Discovery v1.0"
            atts['license'] = '''These data were collected by the Ocean Observatory Initiative (OOI) project purely for internal system development purposes during the construction phase of the project and are offered for release to the public with no assurance of data quality, consistency, temporal continuity or additional support. The OOI Program assumes no liability resulting from the use of these data for other than the intended purpose. No data quality assurance steps have been implemented on this data to date.'''
            atts['summary'] = cov.name
            atts['cdm_data_type'] = 'Other'
            atts['standard_name_vocabulary'] = 'CF-12'
            
            for key, val in atts.iteritems():
                self.xml_attr(doc, add_attributes_element, key, val)

            if len(add_attributes_element.childNodes) > 0:
                dataset_element.appendChild(add_attributes_element)

            for var_name in vars:
                var = cov.get_parameter_context(var_name)
                if re.match(r'.*_[a-z0-9]{32}', var.name):
                    continue # Let's not do this
                


                #if len(param.shape) >=1 and not param.is_coordinate: #dataVariable
                data_element = doc.createElement('dataVariable')
                source_name_element = doc.createElement('sourceName')
                text_node = doc.createTextNode(var.name)
                source_name_element.appendChild(text_node)
                data_element.appendChild(source_name_element)

                destination_name_element = doc.createElement('destinationName')
                text_node = doc.createTextNode(erd_name_map[var.name])
                destination_name_element.appendChild(text_node)
                data_element.appendChild(destination_name_element)
                
                add_attributes_element = doc.createElement('addAttributes')
                units = "unknown"
                if hasattr(var,'uom') and var.uom:
                    units = var.uom
                self.xml_attr(doc, add_attributes_element, 'units', units)
                #if var.ATTRS is not None:
                    #for key in var.ATTRS:
                        #if not hasattr(var,key):
                            #continue
                        #val = getattr(var,key)
                        #if not val:
                            #val = ''
                        #att_element = doc.createElement('att')
                        #att_element.setAttribute('name', key)
                        #text_node = doc.createTextNode(val)
                        #att_element.appendChild(text_node)
                        #add_attributes_element.appendChild(att_element)

                ioos_cat = self.get_ioos_category(var.name, units)
                self.xml_attr(doc, add_attributes_element, 'ioos_category', ioos_cat)

                if hasattr(var,'display_name') and var.display_name is not None:
                    self.xml_attr(doc, add_attributes_element, 'long_name', var.display_name)
                
                if hasattr(var,'standard_name') and var.standard_name is not None:
                    self.xml_attr(doc, add_attributes_element, 'standard_name', var.standard_name)

                if 'seconds' in units and 'since' in units:
                    self.xml_attr(doc, add_attributes_element, 'time_precision', '1970-01-01T00:00:00.000Z')

                if hasattr(var, 'ooi_short_name') and var.ooi_short_name:
                    sname = var.ooi_short_name
                    sname = re.sub('[\t\n ]+', ' ', sname)
                    self.xml_attr(doc, add_attributes_element, 'ooi_short_name', sname)

                    m = re.match(r'[A-Z0-9]{7}', sname)
                    if m:
                        reference_url = 'https://confluence.oceanobservatories.org/display/instruments/' + m.group()
                        self.xml_attr(doc, add_attributes_element, 'references', reference_url)



                    if 'L2' in var.ooi_short_name:
                        self.xml_attr(doc, add_attributes_element, 'data_product_level', 'L2')
                        self.xml_attr(doc, add_attributes_element, 'source', 'level 2 calibrated sensor observation')
                    elif 'L1' in var.ooi_short_name:
                        self.xml_attr(doc, add_attributes_element, 'data_product_level', 'L1')
                        self.xml_attr(doc, add_attributes_element, 'source', 'level 1 calibrated sensor observation')
                    elif 'L0' in var.ooi_short_name:
                        self.xml_attr(doc, add_attributes_element, 'data_product_level', 'L0')
                        self.xml_attr(doc, add_attributes_element, 'source', 'sensor observation')
                    elif 'QC' in var.ooi_short_name:
                        self.xml_attr(doc, add_attributes_element, 'data_product_level', 'QC')

                elif not isinstance(var.param_type, ParameterFunctionType):
                    self.xml_attr(doc, add_attributes_element, 'ooi_short_name', var.name)
                    if units == 'counts':
                        self.xml_attr(doc, add_attributes_element, 'data_product_level', 'L0')
                        self.xml_attr(doc, add_attributes_element, 'source', 'sensor observation')
                    elif 'seconds' in units and 'since' in units:
                        self.xml_attr(doc, add_attributes_element, 'data_product_level', 'axis')
                    else:
                        self.xml_attr(doc, add_attributes_element, 'data_product_level', 'unknown')

                  
                if hasattr(var, 'reference_urls') and var.reference_urls:
                    if isinstance(var.reference_urls, list):
                        references = ','.join(var.reference_urls)
                    else:
                        references = var.reference_urls
                    self.xml_attr(doc, add_attributes_element, 'instrument_type', references)


                if isinstance(var.param_type, ParameterFunctionType):
                    if isinstance(var.function, PythonFunction):
                        self.xml_attr(doc, add_attributes_element, 'function_module', var.function.owner or '')
                        self.xml_attr(doc, add_attributes_element, 'function_name', var.function.func_name or '')
                        if var.function.owner.startswith('ion_functions'):
                            s = var.function.owner
                            url = s.replace('.','/') + '.py'
                            url = 'https://github.com/ooici/ion-functions/blob/master/' + url
                            self.xml_attr(doc, add_attributes_element, 'function_url', url)
                        elif var.function.egg_uri:
                            self.xml_attr(doc, add_attributes_element, 'function_url', var.function.egg_uri or '')
                    elif isinstance(var.function, NumexprFunction):
                        self.xml_attr(doc, add_attributes_element, 'function_name', var.function.name or '')

                data_element.appendChild(add_attributes_element)
                dataset_element.appendChild(data_element)

            index += 1
            #bug with prettyxml
            #http://ronrothman.com/public/leftbraned/xml-dom-minidom-toprettyxml-and-silly-whitespace/
            result += dataset_element.toprettyxml() + '\n'
            #result += dataset_element.toxml() + '\n'

        cov.close()

        if not result:
            log.error("Attempted to register empty dataset\nDims: %s\nDatasets: %s", dims, datasets)


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

datasets_xml="""<?xml version="1.0" ?><erddapDatasets>
<requestBlacklist/>
</erddapDatasets>
"""
