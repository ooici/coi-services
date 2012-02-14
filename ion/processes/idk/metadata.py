#!/usr/bin/env python

"""
@file coi-services/ion/processes/idk/metadata.py
@author Bill French
@brief Gather and store metadata for driver creation
"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import sys
import os

import yaml

from ion.processes.idk import prompt


class Metadata():
    """
    Gather and store metadata for the IDK driver creation process.  When the metadata is stored it also creates a link
    to current.yml in the config dir.  That symlink indicates which driver you are currently working on.
    """

    ###
    #   Configuration
    ###
    def idk_dir(self):
        """
        @brief directory to store the idk driver configuration
        @retval dir name
        """
        return os.environ['HOME'] + "/.idk"

    def metadata_filename(self):
        """
        @brief metadata file name
        @retval filename
        """
        return self.name + "_metadata.yml"

    def metadata_path(self):
        """
        @brief path to the metadata config file
        @retval metadata path
        """
        return self.idk_dir() + "/" + self.metadata_filename()

    def current_metadata_path(self):
        """
        @brief path to link the current metadata file
        @retval current metadata path
        """
        return self.idk_dir() + "/current.yml"


    ###
    #   Private Methods
    ###
    def __init__(self, name=None, author=None, email=None, notes=None):
        """
        @brief Constructor
        """
        if( name or author or email or notes ):
            self.author = author
            self.email = email
            self.name = name
            self.notes = notes
        else:
            self.read_from_file()

    def _init_from_yaml(self, yamlInput):
        """
        @brief initialize the object from YAML data
        @param data structure with YAML input
        """
        self.author = yamlInput['driver_metadata']['author']
        self.email = yamlInput['driver_metadata']['email']
        self.name = yamlInput['driver_metadata']['name']
        self.notes = yamlInput['driver_metadata']['release_notes']


    ###
    #   Public Methods
    ###
    def display_metadata(self):
        """
        @brief Pretty print the current metadata object to STDOUT
        """
        print( "Driver Name: " + self.name )
        print( "Author: " + self.author )
        print( "Email: " + self.email )
        print( "Release Notes: \n" + self.notes )


    def confirm_metadata(self):
        """
        @brief Confirm the metadata entered is correct.  Run from the console
        @retval True if the user confirms otherwise False.
        """
        print ( "\nYou Have entered:\n " )
        self.display_metadata();
        return prompt.yes_no( "\nIs this metadata correct? (y/n)" )


    def serialize(self):
        """
        @brief Serialize metadata object data into a yaml string.
        @retval yaml string
        """
        return yaml.dump( {'driver_metadata': {
                                'author': self.author,
                                'email': self.email,
                                'name': self.name,
                                'release_notes': self.notes
                          }
        }, default_flow_style=False)


    def store_to_file(self):
        """
        @brief Write YAML file with metadata.  Once the file is written it also creates a symlink to current.yml
            indicating that this is the working metadata file.
        """
        outputFile = self.metadata_path()

        if not os.path.exists(self.idk_dir()):
            os.makedirs(self.idk_dir())

        ofile = open( outputFile, 'w' )

        ofile.write( self.serialize() )
        ofile.close()

        if( os.path.exists(self.current_metadata_path()) ):
            os.remove(self.current_metadata_path())

        os.symlink(self.metadata_path(), self.current_metadata_path())


    def read_from_file(self,infile = None):
        """
        @brief Read a YAML metadata file and initialize the current object with that data.
        @params infile filename to a YAML metadata file, default is to use the current.yml file
        """
        if( infile ):
            inputFile = infile
        else:
            inputFile = self.current_metadata_path()

        try:
            infile = open( inputFile )
        except IOError:
            return True

        input = yaml.load( infile )

        if( input ):
            self._init_from_yaml( input )
            infile.close()


    def get_from_console(self):
        """
        @brief Read metadata from the console and initialize the object.  Continue to do this until we get valid input.
        """
        self.name = prompt.text( 'Driver Name', self.name )
        self.author = prompt.text( 'Author', self.author )
        self.email = prompt.text( 'Email', self.email )
        self.notes = prompt.multiline( 'Release Notes', self.notes )

        if( self.confirm_metadata() ):
            self.store_to_file()
        else:
            return self.get_from_console()



if __name__ == '__main__':
    metadata = Metadata()
    metadata.read_from_file(metadata.current_metadata_path())
    metadata.get_from_console()