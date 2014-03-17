#!/usr/bin/python
"""
A Service to load data products in to postgres and geoserver
"""

__author__ = 'abird'

import os
from pyon.util.breakpoint import breakpoint
from pyon.ion.resource import LCS, LCE, PRED
from pyon.util.file_sys import FileSystem, FS
import time
import psycopg2
import sys
import requests
import os
from pyon.public import CFG

DEBUG = False

REAL = "real,"
INT = "int,"
TIMEDATE = "timestamp,"

class resource_parser():

    def __init__(self):
        self.USING_EOI_SERVICES = CFG.get_safe('eoi.meta.use_eoi_services', False)
        
        if (self.USING_EOI_SERVICES):
            self.LATITUDE = CFG.get_safe('eoi.meta.lat_field', False)
            self.LONGITUDE = CFG.get_safe('eoi.meta.lon_field', False)

            self.RESETSTORE = CFG.get_safe('eoi.importer_service.reset_store', False)
            self.REMOVELAYER = CFG.get_safe('eoi.importer_service.remove_layer', False)
            self.ADDLAYER = CFG.get_safe('eoi.importer_service.add_layer', False)
            #add default varaibles
            self.SERVER = CFG.get_safe('eoi.importer_service.server', "localhost")+":"+str(CFG.get_safe('eoi.importer_service.port', 8844))
            self.DATABASE = CFG.get_safe('eoi.postgres.database', False)
            self.DB_USER = CFG.get_safe('eoi.postgres.user_name', False)
            self.DB_PASS =  CFG.get_safe('eoi.postgres.password', False)

            self.TABLE_PREFIX = CFG.get_safe('eoi.postgres.table_prefix', False)
            self.VIEW_SUFFIX = CFG.get_safe('eoi.postgres.table_suffix', False)

            self.coverage_fdw_sever = CFG.get_safe('eoi.fdw.server', False)

        self.con = None
        self.postgres_db_availabe = False
        self.importer_service_available = False
        
        try:
            self.con = psycopg2.connect(database=self.DATABASE, user=self.DB_USER,password=self.DB_PASS)
            self.cur = self.con.cursor()
            #checks the connection
            self.cur.execute('SELECT version()')
            ver = self.cur.fetchone()
            self.postgres_db_availabe = True
            self.importer_service_available = self.checkForImporterService();
            print ver

        except psycopg2.DatabaseError, e:
            #error setting up connection
            print 'Error %s' % e

        self.use_geo_services = False
        if (self.postgres_db_availabe and self.importer_service_available):
            self.use_geo_services = True
            print "TableLoader:Using geoservices..."
        else:
            print "TableLoader:NOT using geoservices..."

    def checkForImporterService(self):
        try:
            r = requests.get(self.SERVER+'/service=alive&name=ooi&id=ooi')
            print "importerservice status code:" + str(r.status_code)
            #alive service returned ok
            if (r.status_code == 200):
                return True
            else:
                return False
        except Exception, e:
            #SERVICE IS REALLY NOT AVAILABLE
            print "service is really not available..."
            return False

    def close(self):
        if self.con:
            self.con.close()


    def sendGeoNodeRequest(self,request,resource_id, prim_types=None):
        try:
            
            if prim_types is None:
                r = requests.get(self.SERVER+'/service='+request+'&name='+resource_id+'&id='+resource_id)
                self.processStatusCode(r.status_code) 
            else:
                r = requests.get(self.SERVER+'/service='+request+'&name='+resource_id+'&id='+resource_id+"&params="+str(prim_types))
                self.processStatusCode(r.status_code) 
                
        except Exception, e:
            raise e

    '''
    Reset all data and rows, and layers
    '''
    def reset(self):
        #remove all FDT from the DB
        self.cur.execute(self.dropAllFDT())    
        self.con.commit()
        listRows = self.cur.fetchall()
        for row in listRows:
            self.dropExistingTable(row[0],use_cascade =True)    

        #reset the layer information on geoserver
        self.sendGeoNodeRequest(self.RESETSTORE,"ooi") 

    def processStatusCode(self,status_code):        
        if (status_code ==200):
            print "SUCCESS!"
        else:
            print "Error Processing layer"

    @staticmethod
    def _get_coverage_path(dataset_id):
        file_root = FileSystem.get_url(FS.CACHE,'datasets')
        return os.path.join(file_root, '%s' % dataset_id)        

    '''
    removes a single resource
    '''
    def removeSingleResource(self,resource_id):
        if (self.doesTableExist(resource_id)):
            self.dropExistingTable(resource_id,use_cascade =True) 
        else:
            print "could not remove,does not exist"
            pass

        # try and remove it from geoserver
        self.sendGeoNodeRequest(self.REMOVELAYER,resource_id)

    '''
    creates a single resource
    '''
    def createSingleResource(self,new_resource_id,param_dict):
        #parse 
        relevant = []
        for k,v in param_dict.iteritems():
            if isinstance(v, list) and len(v)==2 and 'param_type' in v[1]:
                relevant.append(k)
        
        if (DEBUG):
            print 'params are', relevant

        coverage_path = self._get_coverage_path(new_resource_id)

        #generate table from params and id
        [success,prim_types]= self.generateSQLTable(new_resource_id,param_dict,relevant,coverage_path)

        if (DEBUG):
            print prim_types

        if (success):
            #generate geoserver layer
            self.sendGeoNodeRequest(self.ADDLAYER,new_resource_id,prim_types)
    
    def getValueEncoding(self,name,value_encoding):
        encodingString = None
        prim_type = None
        #get the primitve type, and generate something using NAME
        if name =="time":
            encodingString="\""+name+"\" "+TIMEDATE
            prim_type = "time"
        elif (name.find('time')>=0):
            #ignore other times
            encodingString=None
            prim_type = None
        elif (value_encoding.startswith('int')):
            #int                                
            encodingString="\""+name+"\" "+INT
            prim_type = "int"
        elif(value_encoding.find('i8')>-1):    
            #int
            encodingString="\""+name+"\" "+INT
            prim_type = "int"
        elif(value_encoding.startswith('float')):
            #float
            encodingString="\""+name+"\" "+REAL
            prim_type = "real"
        elif(value_encoding.find('f4')>-1):
            #float
            encodingString="\""+name+"\" "+REAL    
            prim_type = "real"
        elif(value_encoding.find('f8')>-1):
            #float
            encodingString="\""+name+"\" "+REAL    
            prim_type = "real"
        else:
            encodingString=None
            prim_type = None
             #no value encoding available   
        return (encodingString, prim_type)

    """
    Generates Foreign data table for used with postgres
    """
    def generateSQLTable(self, dataset_id, params, relevant,coverage_path):
        #check table exists
        if (not self.doesTableExist(dataset_id)):

            valid_types={}
            create_table_string ="create foreign table \""+dataset_id+"\" ("

            #loop through the params
            for param in relevant:
                #get the information
                data_item = params[param]
                desc =  data_item[1]['description']
                ooi_short_name =  data_item[1]['ooi_short_name']
                name =  data_item[1]['name']
                disp_name = data_item[1]['display_name']
                internal_name = data_item[1]['internal_name']
                cm_type = data_item[1]['param_type']['cm_type']
                units = ""
                try:
                    units= data_item[1]['uom']
                except Exception, e:
                    if (DEBUG):
                        print "no units available..."
                
                value_encoding = data_item[1]['param_type']['_value_encoding']
                fill_value = data_item[1]['param_type']['_fill_value']
                std_name = data_item[1]['standard_name']

                #only use things that have valid value
                if (len(name)>0): #and (len(desc)>0) and (len(units)>0) and (value_encoding is not None)):
                    if (DEBUG):
                        print "-------processed-------"
                        print ooi_short_name
                        print desc
                        print name
                        print disp_name
                        print units
                        print internal_name
                        print value_encoding
                        print cm_type[1]

                    if (cm_type[1] == "ArrayType"):
                        #ignore array types
                        pass
                    else:
                        [encoding,prim_type] = self.getValueEncoding(name,value_encoding)          
                        if encoding is not None:
                            create_table_string+=encoding
                            valid_types[name] = prim_type

                pass

            pos = create_table_string.rfind(',')
            create_table_string = create_table_string[:pos] + ' ' + create_table_string[pos+1:]
            print coverage_path
            create_table_string = self.addServerInfo(create_table_string,coverage_path)
            
            if (DEBUG):
                print "\n"
                print create_table_string

            try:
                self.cur.execute(create_table_string)
                self.con.commit()
                #should always be lat and lon
                self.cur.execute(self.generateTableView(dataset_id,self.LATITUDE,self.LONGITUDE))
                self.con.commit()

                return ((self.doesTableExist(dataset_id)),valid_types)

            except Exception, e:
                #error setting up connection
                print 'Error %s' % e

        else:
            if (DEBUG):
                print "table is already there dropping it"
            self.dropExistingTable(dataset_id)
            return False



    '''
    generate table view including geom
    '''
    def generateTableView(self,dataset_id,lat_field,lon_field):
        sqlquery = '''
        CREATE or replace VIEW "%s%s%s" as SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as 
        geom, * from "%s";
        '''% (self.TABLE_PREFIX,dataset_id,self.VIEW_SUFFIX,lon_field,lat_field,dataset_id)
        return sqlquery

    '''
    add the server info to the sql create table request
    '''
    def addServerInfo(self, sqlquery,coverage_path):
        sqlquery += ") server " +self.coverage_fdw_sever+ " options(k \'1\',cov_path \'"+coverage_path+"\');"
        return sqlquery

    def modifySQLTable(self, dataset_id, params):
        print "sql modify"

    def removeSQLTable(self, dataset_id):
        print "sql remove"

    def dropExistingTable(self, dataset_id,use_cascade =False):
        self.cur.execute(self.getTableDropCmd(dataset_id))
        self.con.commit()

    '''
    Checks to see if the table already exists before we add it
    '''    
    def doesTableExist(self,dataset_id):
        self.cur.execute(self.getTableExistCmd(dataset_id))
        out = self.cur.fetchone()
        #check table exist
        if (out is None):
            return False
        else:
            return True

    '''
    looks in the psql catalog for the table, therefore is quick and does not hit the table itself
    '''
    def getTableExistCmd(self,dataset_id):
        #check table exists 
        sqlcmd = "SELECT 1 FROM pg_catalog.pg_class WHERE relname = \'"+dataset_id+"\';"
        return sqlcmd


    def getTableDropCmd(self,dataset_id,use_cascade =False):
        #drop table
        if (use_cascade):
            sqlcmd = "drop foreign table \""+dataset_id+"\" cascade;"
        else:
            sqlcmd = "drop foreign table \""+dataset_id+"\" cascade;"    
        return sqlcmd

    def dropAllFDT(self):
        sqlcmd = "SELECT relname FROM pg_catalog.pg_class where relkind ='foreign table';"
        return sqlcmd        
