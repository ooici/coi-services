#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file 
@date 04/26/12 12:56
@description DESCRIPTION
'''
from interface.services.dm.iindex_management_service import IndexManagementServiceClient
from ion.services.dm.inventory.index_management_service import IndexManagementService
from pyon.core.bootstrap import get_sys_name
from pyon.ion.process import ImmediateProcess
from pyon.core.exception import BadRequest
from interface.objects import SearchOptions
from pyon.public import RT, log, CFG
import interface.objects
import elasticpy as ep
import re


'''
- Sites Index

- Agent Index

- Devices Index

- Models Index

- Data Products Index

- Notification Request Index

'''

STD_INDEXES  = {
    '%s_sites_index' % get_sys_name().lower()           : [RT.Site],
    '%s_agents_index' % get_sys_name().lower()          : [RT.PlatformAgent,  RT.InstrumentAgent, RT.ExternalDatasetAgent, RT.DataSourceAgent],
    '%s_agents_instance_index' % get_sys_name().lower() : [RT.PlatformAgentInstance,RT.InstrumentAgentInstance,RT.ExternalDatasetAgentInstance,
                                                            RT.DataSourceAgentInstance,RT.AgentInstance],
    '%s_devices_index' % get_sys_name().lower()         : [RT.SensorDevice, RT.PlatformDevice, RT.InstrumentDevice],
    '%s_models_index' % get_sys_name().lower()          : [RT.SensorModel, RT.PlatformModel, RT.InstrumentModel, RT.ExternalDatasetModel, 
                                                            RT.DataSourceModel],
    '%s_data_products_index' % get_sys_name().lower()   : [RT.DataProduct],
    '%s_searches_and_catalogs' % get_sys_name().lower() : [RT.Index, RT.Catalog]
}
COUCHDB_INDEXES = {
    'resources_couch_index'  : '%s_resources' % get_sys_name().lower()
}
ELASTICSEARCH_CONTEXT_SCRIPT = 'if(ctx.doc.lcstate == "RETIRED") { ctx.deleted = true; } ctx._id = ctx.doc._id; ctx._type = ctx.doc.type_'
class IndexBootStrap(ImmediateProcess):
    def on_start(self):
        if self.CFG.system.force_clean and not self.CFG.system.testing:
            text = "system.force_clean=True. ION Preload does not support this"
            log.error(text)
            raise BadRequest(text)

        if not self.CFG.get_safe('system.elasticsearch', False):
            text = 'Can not initialize indexes without ElasticSearch enabled.  Please enable system.elasticsearch.'
            log.error(text)
            raise BadRequest(text)


        self.sysname = get_sys_name().lower()

        self.es_host        = self.CFG.get_safe('server.elasticsearch.host', 'localhost')
        self.es_port        = self.CFG.get_safe('server.elasticsearch.port', '9200')

        self.index_shards   = self.CFG.get_safe('server.elasticsearch.shards',5)
        self.index_replicas = self.CFG.get_safe('server.elasticsearch.replicas', 1)

        self.river_shards   = self.CFG.get_safe('server.elasticsearch.river_shards',5)
        self.river_replicas = self.CFG.get_safe('server.elasticsearch.river_replicas',1)

        self.es = ep.ElasticSearch(host=self.es_host, port=self.es_port, timeout=10)

        op = self.CFG.get('op',None)

        if op == 'index_bootstrap':
            self.index_bootstrap()
        elif op == 'clean_bootstrap':
            self.clean_bootstrap()
        else:
            raise BadRequest('Operation Unknown')

    @staticmethod
    def es_mapping(rtype):
        schema = getattr(interface.objects,rtype)._schema
        mapping = {}
        for k,v in schema.iteritems():
            ion_type = v['type']
            if ion_type=='int':
                mapping.update( ep.ElasticMap(k).type('double'))
            elif ion_type=='long':
                mapping.update( ep.ElasticMap(k).type('double'))
            elif ion_type=='float':
                mapping.update( ep.ElasticMap(k).type('double'))
            elif ion_type=='str':
                mapping.update( ep.ElasticMap(k).type('string'))
            elif ion_type=='GeospatialLocation':
                mapping.update( ep.ElasticMap(k).type('geo_point'))

        return {rtype : {'properties' : mapping}}

    @staticmethod
    def attr_mapping(resources):
        options = SearchOptions()
        attribute_match = set()
        range_fields    = set()
        geo_fields      = set()
        for t in resources:
            schema = getattr(interface.objects, t)._schema
            for k,v in schema.iteritems():
                ion_type = v['type']

                if ion_type=='int':
                    range_fields.add(k)
                elif ion_type =='long':
                    range_fields.add(k)
                elif ion_type == 'float':
                    range_fields.add(k)
                elif ion_type == 'str':
                    attribute_match.add(k)
                elif ion_type == 'GeospatialLocation':
                    geo_fields.add(k)
        options.attribute_match = list(attribute_match)
        options.range_fields    = list(range_fields)
        options.geo_fields      = list(geo_fields)

        return options


    def clean_bootstrap(self):

        for k,v in STD_INDEXES.iteritems():
            IndexManagementService._es_call(self.es.river_couchdb_delete,k)
            IndexManagementService._es_call(self.es.index_delete,k)

        IndexManagementService._es_call(self.es.river_couchdb_delete,'%s_resources_index' % self.sysname)
        IndexManagementService._es_call(self.es.index_delete,'%s_resources_index' % self.sysname)


        self.index_bootstrap()

    def index_bootstrap(self):
        '''
        Creates the initial set of desired indexes based on a standard definition
        '''

        #---------------------------------------------------------------------------------------
        # Create the _river index based on the cluster configurations
        #---------------------------------------------------------------------------------------
        IndexManagementService._es_call(self.es.index_create,'_river',number_of_shards = self.river_shards, number_of_replicas = self.river_replicas)

        filters = {
            '_id' : '_design/filters',
            'filters' : {
            }
        }
        #=======================================================================================
        # For each of the resource types in the list of values for each standard index,
        # create a mapping and a context type in ElasticSearch based on the searchable fields.
        #=======================================================================================

        for k,v in STD_INDEXES.iteritems():
            response = IndexManagementService._es_call(self.es.index_create,
                k,
                number_of_shards=self.index_shards,
                number_of_replicas=self.index_replicas
            )
            IndexManagementService._check_response(response)
            
            body = 'function(doc, req) { switch(doc.type_) { default: return false; }}'
            for res in v:
                body = re.sub(r'default:', 'case "%s": return true; default:' % res, body)
                mappings = self.es_mapping(res)
                response = IndexManagementService._es_call(self.es.raw,'%s/%s/_mapping' %(k,res), 'POST', mappings)
                IndexManagementService._check_response(response)

            filters['filters'][k] = body
        
        #=======================================================================================
        # Get an instance of the datastore instance used to create the CouchDB filters
        # in support of the ElasticSearch river's filter
        #  - Allows us to filter based on resource type
        #=======================================================================================
       
        cc = self.container
        db = cc.datastore_manager.get_datastore('resources')
        datastore_name = db.datastore_name
        db = db.server[datastore_name]

        db.create(filters)

        for k,v in STD_INDEXES.iteritems():
            response = IndexManagementService._es_call(self.es.river_couchdb_create,
                index_name = k,
                couchdb_db = datastore_name,
                couchdb_host = CFG.server.couchdb.host,
                couchdb_port = CFG.server.couchdb.port, 
                couchdb_user = CFG.server.couchdb.username,
                couchdb_password = CFG.server.couchdb.password,
                couchdb_filter = 'filters/%s' % k,
                script= ELASTICSEARCH_CONTEXT_SCRIPT
            )
            IndexManagementService._check_response(response)

        #=======================================================================================
        # For each of the resource types add a mapping to the resources_index
        #=======================================================================================
        response = IndexManagementService._es_call(self.es.index_create,'%s_resources_index' % self.sysname,
            number_of_shards=self.index_shards,
            number_of_replicas=self.index_replicas
        )
        IndexManagementService._check_response(response)
        for t in RT.values():
            mappings = self.es_mapping(t)
            response = IndexManagementService._es_call(self.es.raw,'%s_resources_index/%s/_mapping' % (self.sysname,t), 'POST', mappings)
            IndexManagementService._check_response(response)

        response = IndexManagementService._es_call(self.es.river_couchdb_create,
            index_name = '%s_resources_index' % self.sysname,
            couchdb_db = datastore_name,
            couchdb_host = CFG.server.couchdb.host,
            couchdb_port = CFG.server.couchdb.port,
            couchdb_user = CFG.server.couchdb.username,
            couchdb_password = CFG.server.couchdb.password,
            script= ELASTICSEARCH_CONTEXT_SCRIPT
        )
        IndexManagementService._check_response(response)





        
       
        #=======================================================================================
        # Construct the index resources 
        #=======================================================================================

        ims_cli = IndexManagementServiceClient()

        for index,resources in STD_INDEXES.iteritems():
            ims_cli.create_index(
                name=index,
                description='%s ElasticSearch Index Resource' % index,
                content_type=IndexManagementService.ELASTICSEARCH_INDEX,
                options=self.attr_mapping(resources)
            )

        for index,datastore in COUCHDB_INDEXES.iteritems():
            ims_cli.create_index(
                name=index,
                description='%s CouchDB Index Resource' % index,
                content_type=IndexManagementService.COUCHDB_INDEX,
                datastore_name=datastore
            )

        ims_cli.create_index(
            name='%s_resources_index' % self.sysname,
            description='Resources Index',
            content_type=IndexManagementService.ELASTICSEARCH_INDEX,
            options=self.attr_mapping(RT.keys())
        )
