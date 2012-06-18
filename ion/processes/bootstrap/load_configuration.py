#!/usr/bin/env python

import sys
import os
from pyon.datastore.datastore_standalone import DirectorySa
import optparse
from pyon.core.path import list_files_recursive
from collections import OrderedDict
import argparse

class OptionParser (optparse.OptionParser):
    def check_required(self, opt):
        option = self.get_option(opt)
        if getattr(self.values, option.dest) is None:
            self.error("-s option is not supplied ")


class LoadConfiguration:
    '''
    Load configuration data into the datastore
    How to run this from command line:
        bin/python bin/load_configuration  -n system name [ -o filename | -s filename | -c true|false]
        -o Load object definition file
        -s Load service definition file
        -c Force clean the database

     Example:
        Load all object and service definitions
        bin/python bin/load_configuration  -s mysysname

        Load all object and service definitions with force clean the database
        bin/python bin/load_configuration  -s mysysname -c true

        Load object definition from a file
        bin/python bin/load_configuration  -s mysysname -o obj/data/coi/org.yml

        Load service definition from a file
        bin/python bin/load_configuration  -s mysysname -r obj/services/coi/datastore_service.yml
    '''

    object_directory_path = "ObjectTypes"
    service_directory_path = "ServiceDefinitions"
    resource_directory_path = "ResourceDefinitions"

    def __init__(self, sysname, object_definition_file=None,
                 service_definition_file=None, forceclean=False):
        self.sysname = sysname
        self.object_definition_file = object_definition_file
        self.service_definition_file = service_definition_file
        self.forceclean = forceclean
        self.dir = DirectorySa(sysname=self.sysname,
            forceclean=self.forceclean)

    def load(self):
        if self.service_definition_file:
            if os.path.exists(self.service_definition_file):
                self.load_from_files(self.service_directory_path,
                    [self.service_definition_file],
                    self.get_service_file_content)
            else:
                print "LoadConfiguration: Error couldn't find the file path\n"
        elif self.object_definition_file:
            if os.path.exists(self.object_definition_file):
                self.load_from_files(self.object_directory_path,
                    [self.object_definition_file],
                    self.get_data_file_content)
            else:
                print "LoadConfiguration: Error couldn't find the file path\n"
        else:
            # load all files
            self.load_object_model_interface()
            self.load_service_interface()
            self.load_resource_configuration()

    def load_service_interface(self):
        service_yaml_filenames = list_files_recursive('obj/services', '*.yml')
        print "\nLoading service interfaces..."
        self.load_from_files(self.service_directory_path,
            service_yaml_filenames,
            self.get_service_file_content)

    def load_object_model_interface(self):
        print "\nLoading object model..."
        data_yaml_filenames = list_files_recursive('obj/data', '*.yml',
            ['ion.yml', 'resource.yml',
             'shared.yml'])
        self.load_from_files(self.object_directory_path, data_yaml_filenames,
            self.get_data_file_content)

    def load_resource_configuration(self):
        print "\nLoading... resource config"
        resource_filenames = list_files_recursive('res/config', '*.yml')
        self.load_from_files(self.resource_directory_path, resource_filenames,
            self.get_resource_file_content)

    def display_header(self):
        print "--------------------------------------------------------------"
        print "Key".ljust(40), "Filename".ljust(60)
        print "--------------------------------------------------------------"

    def load_from_files(self, path, filenames, get_file_content_method):
        '''
        Gets file content and load it to datastore
        '''
        self.display_header()
        for file_path in filenames:
            content = get_file_content_method(file_path)
            for key in content.keys():
                print key.ljust(40), file_path.ljust(60)
                self.load_to_datastore(path, key, content[key], file_path)

    def load_to_datastore(self, path, key, content, filename):
        '''
         Load data to datastore
        '''
        if not self.dir.lookup('/' + path):
            self.dir.register('/', path)

        new_entry = self.dir.register('/' + path, key, file_path=filename, definition=content)
        if new_entry is not None and self.forceclean:
            print '\033[91m' "\nLoad Configuration Error!!! Multiple definitions found:\n"
            print '\033[92m' "Filename:", filename
            print '\033[91m', content
            print '\033[92m' "\nPrevious definition:"
            print '\033[91m', new_entry['attributes']['definition']
            print '\033[0m'
            exit()

        new_entry = self.dir.lookup('/' + path + '/' + key)
        if content != new_entry['definition']:
            print '\n\nError adding: ' + key + ' to the directory'
            exit()

    def get_service_file_content(self, file_path):
        file_content_str = ""
        key = ""
        objs = {}
        with open(file_path, 'r') as f:
            file_content_str = f.read()
        for line in file_content_str.split('\n'):
            if line[:5] == "name:":
                key = line.split()[1]
                objs[key] = file_content_str

        if not objs:
            print "\n\n=====ERROR===== Can't find object name for: ", file_path

        return objs

    def get_resource_file_content(self, file_path):
        '''
            Read file content from res/config and returns dict with key
            equals to filename and value equals to file content
        '''
        with open(file_path, 'r') as f:
            file_content_str = f.read()
        objs = {}
        objs[os.path.basename(file_path)] = file_content_str

        return objs

    def get_data_file_content(self, file_path):
        '''
            Return dict with key equal to classname and value equal to text
            representation of class definition
        '''
        file_content_str = ""
        objs = OrderedDict()
        class_content = ''

        # Open and read data model definition file
        with open(file_path, 'r') as f:
            file_content_str = f.read()

        first_time = True
        for line in file_content_str.split('\n'):
            #if len(line) == 0:
            #    continue
            #if line == "---":
            #    continue
            if len(line) > 0 and (line[0].isalpha()):
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

def main():
    parser = OptionParser()
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--name", dest="sysname", help="System name")
    parser.add_argument("-o", "--object", dest="fobject",
        help="Load object definition from a file")
    parser.add_argument("-s", "--service", dest="fservice",
        help="Load service definition from a file")
    parser.add_argument('-c', '--forceclean', action='store_true',
        help='Read configuration from datastore.')
    options = parser.parse_args()

    if not options.sysname:
        options.sysname = 'ion_%s' % os.uname()[1].replace('.', '_').lower()
    lc = LoadConfiguration(options.sysname, options.fobject, options.fservice,
        options.forceclean)
    lc.load()

if __name__ == '__main__':
    main()

