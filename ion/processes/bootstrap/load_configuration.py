#!/usr/bin/env python

__author__ = ' '
__license__ = 'Apache 2.0'

import os
import yaml

from pyon.util import yaml_ordered_dict
from pyon.public import CFG, log, ImmediateProcess, iex
from pyon.core.path import list_files_recursive
from pyon.core import bootstrap
from pyon.ion.directory import Directory


class LoadConfiguration(ImmediateProcess):
    '''
    Loads configuration to datastore

    How to run it:
    bin/pycc -x ion.processes.bootstrap.load_configuration.LoadConfiguration [load_from_file=file phat]
    Example:
     To load all configuration files:
      bin/pycc -x ion.processes.bootstrap.load_configuration.LoadConfiguration 
     
     To load configuration from a file:
       bin/pycc -x ion.processes.bootstrap.load_configuration.LoadConfiguration load_from_file=obj/data/ion.yml
    '''

    directory_path = "configuration"    

    def on_init(self):
        print "on_init\n"

    def on_start(self):

        load_from_file = CFG.get("load_from_file", None)
        if load_from_file:
            if os.path.exists(load_from_file):
                self.load_from_files(self.directory_path, [load_from_file])
            else:
                print "LoadConfiguration: Error couldn't find the file path\n"
        else:    
            # load from all files
            self.load_object_model_interface()
            self.load_service_interface()
            self.load_resource_configuration()

    def on_quite(self):
        pass
  

    def load_service_interface(self):
        service_yaml_filenames = list_files_recursive('obj/services', '*.yml')
        print "\nLoading service interfaces..."
        self.load_from_files(self.directory_path, service_yaml_filenames, self.get_service_file_content)

    def load_object_model_interface(self):
        print "\nLoading object model..."
        data_yaml_filenames = list_files_recursive('obj/data', '*.yml', ['ion.yml', 'resource.yml'])
        self.load_from_files(self.directory_path, data_yaml_filenames, self.get_data_file_content)

    def load_resource_configuration(self):
        print "\nLoading... resource config"
        resource_filenames = list_files_recursive('res/config', '*.yml')
        self.load_from_files(self.directory_path, resource_filenames, self.get_resource_file_content)


    def display_header(self):
        print "----------------------------------------------------------------------------"
        print "Key".ljust(40), "Filename".ljust(60)
        print "----------------------------------------------------------------------------"



    def load_from_files(self, path, filenames, get_file_content_method):
        '''
        Gets file content and load it to datastore
        '''
        self.display_header()
        for file_path in filenames:
            #content = self.get_data_file_content(file_path)
            content = get_file_content_method(file_path)
            for key in content.keys():
                print  key.ljust(40) , file_path.ljust(60)
                #print "Loading... Object: %s File: %s " % (key, file_path)
                log.debug('Loading: ' + key);
                self.load_to_datastore(path, key, content[key])
        
        
    def load_to_datastore (self, path, key, content):
        '''
         Load data to datastore
        '''
        log.debug(' Adding ' + key +' to the directory')
        dir = Directory()
        
        if not dir.lookup('/' + path):
            dir.register('/',path)

        dir.register ('/' + path, key, data=content)
        new_entry = dir.lookup ('/' + path + '/' + key)

        if content == new_entry['data']:
            log.debug(key + ' has been added to the directory')
        else:
            print '\n\nError adding: '+ key +' to the directory'
            log.error('Error adding: '+ key +' to the directory')



    def get_service_file_content(self, file_path):
        file_content_str = ""
        key = ""
        objs = {}
        with open(file_path, 'r') as f:
           file_content_str = f.read()

        for line in file_content_str.split('\n'):
            if line[:5] == "name:":
                key = line.split()[1]
                objs[key]= file_content_str

        if not objs:
           print "\n\n=====ERROR===== Can't find object name for: ", file_path
           log.error("=====ERROR===== Can't find object name for: " + file_path)


        return objs 

    def get_resource_file_content(self, file_path):
        '''
            Read file content from res/config and returns dict with key
            equals to filename and value equals to file content
        '''
        with open(file_path, 'r') as f:
            file_content_str = f.read()
        objs = {}
        objs[os.path.basename(file_path).split(".")[0]] = file_content_str

        return objs


    def get_data_file_content(self, file_path):
        '''
            Return dict with key equal to classname and value equal to text 
            representation of class definition
        '''
        file_content_str = ""
        objs = {}
        class_content = ''

        # Open and read data model definition file
        with open(file_path, 'r') as f:
            file_content_str = f.read()

        first_time = True
        for line in file_content_str.split('\n'):
            if len(line) == 0:
                continue
            if line == "---":
                continue
            if line[0].isalpha():
                if not first_time:
                    objs[classname] = class_content
                    class_content = ""
                else:
                    first_time = False
                classname = line.split(':')[0]
                class_content += line + "\n"
            else:
                class_content += line + "\n"
        objs[classname] = class_content
        
        return objs

