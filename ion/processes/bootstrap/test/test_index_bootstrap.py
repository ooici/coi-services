#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file test_index_bootstrap.py
@date 05/03/12 10:26
@description DESCRIPTION
'''
from pyon.core.exception import BadRequest
from pyon.core.bootstrap import get_sys_name
from pyon.util.containers import DotDict
from pyon.util.unit_test import PyonTestCase
from pyon.util.int_test import IonIntegrationTestCase
from pyon.public import RT, CFG
from mock import Mock, patch, DEFAULT
from ion.services.dm.inventory.index_management_service import IndexManagementService
from ion.processes.bootstrap.index_bootstrap import IndexBootStrap, STD_INDEXES, COUCHDB_INDEXES
from unittest.case import skipIf
from nose.plugins.attrib import attr
import elasticpy as ep


use_es = CFG.get_safe('system.elasticsearch',False)


@attr('UNIT')
class IndexBootStrapUnitTest(PyonTestCase):
    pass
@attr('INT',group='dm')
class IndexBootStrapIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')
        self.es = ep.ElasticSearch(host=CFG.server.elasticsearch.host, port=CFG.server.elasticsearch.port)

    def wipe(self):
        index_list = IndexManagementService._es_call(self.es.list_indexes)
        for index in index_list:
            IndexManagementService._es_call(self.es.index_delete,index)


    @skipIf(not use_es, 'No ElasticSearch')
    def test_bootstrap(self):
        cc = self.container
        #=======================================
        # Clean indexes
        #=======================================
        self.assertTrue(CFG.system.force_clean)
        self.wipe()
        config = CFG
        config.op='index_bootstrap'
        
        # Thankfully, the default system.force_clean for integration tests is False :)
        
        cc.spawn_process(
            name='index_bootstrap',
            module='ion.processes.bootstrap.index_bootstrap',
            cls='IndexBootStrap',
            config=config
        )
        index_list = IndexManagementService._es_call(self.es.list_indexes)


        for index in STD_INDEXES.iterkeys():
            self.assertTrue(index in index_list)

    @skipIf(not use_es, 'No ElasticSearch')
    def test_clean_bootstrap(self):
        cc = self.container
        IndexManagementService._es_call(self.es.index_delete,'sites_index')
        response = IndexManagementService._es_call(self.es.index_create,'sites_index') # Force a conflict
        IndexManagementService._check_response(response)

        config = CFG
        config.op='clean_bootstrap'
        
        # Thankfully, the default system.force_clean for integration tests is False :)
        
        cc.spawn_process(
            name='index_bootstrap',
            module='ion.processes.bootstrap.index_bootstrap',
            cls='IndexBootStrap',
            config=config
        )
        index_list = IndexManagementService._es_call(self.es.list_indexes)


        for index in STD_INDEXES.iterkeys():
            self.assertTrue(index in index_list)

