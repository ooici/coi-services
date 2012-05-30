#!/usr/bin/env python

"""
@file coi-services/ion/idk/config.py
@author Bill French
@brief Configuration object for the IDK.  It reads values from
and IDK yaml file and can write the user configuration to a file.

ConfigManager is the main configuration object and Config provides
a mechanism to access the CM singleton.

Usage:

use ion.idk.config import Config

Config().rebase()
repo = Config().working_repo()

"""

__author__ = 'Bill French'
__license__ = 'Apache 2.0'

import os
import sys
import yaml
from git import Repo

from ion.idk.exceptions import IDKConfigMissing
from ion.idk.exceptions import IDKWrongRunningDirectory

from ion.idk.logger import Log
from ion.idk.common import Singleton
from pyon.util.config import Config

####
#    Config defaults.  These are hard coded because they shouldn't
#    be overrided
####
PATH = os.path.join(os.path.expanduser("~"), ".idk")
CONFIG_FILENAME = os.path.join(PATH, "idk.yml")

DEFAULT_CONFIG = "extern/ion-definitions/res/config/idk.yml"
IDK_YAML_GROUP = "idk"
YAML_CONFIG_WORKING_REPO = "working_repo"

MI_REPO_NAME = "marine-integrations"

class ConfigManager(Singleton):
    """
    Config Manager.
    Creates a config file if it doesn't already exist.
    """
    def init(self):
        if not os.path.isdir(PATH):
            try:
                os.mkdir(PATH) # create dir if it doesn't exist
            except:
                raise IOError("Couldn't create \"" + PATH + "\" folder. Check" \
                              " permissions")

        ## Initialize the config file if one doesn't exist
        if not os.path.isfile(CONFIG_FILENAME):
            Log.debug("User IDK config doesn't exist: " + CONFIG_FILENAME)
            self.init_config_file()
            self.rebase();
            
        ## Read the user config file
        try:
            Log.debug("Reading IDK config: " + CONFIG_FILENAME)
            infile = open(CONFIG_FILENAME)
            self.yaml = yaml.load(infile)
            infile.close()
            
        except:
            raise IOError("Couldn't read config file \"" + \
                          CONFIG_FILENAME + "\". Check permissions.")
            

    def init_config_file(self):
        """
        @brief If a user config file doesn't exist, read the system idk.yml file
               then write it to the user's file.
        """
        Log.debug("Reading system IDK config: " + DEFAULT_CONFIG)

        if not os.path.isfile(DEFAULT_CONFIG):
            raise IDKConfigMissing(msg="Could not read default configuration " + DEFAULT_CONFIG )
        
        Log.debug("Read default config: " + DEFAULT_CONFIG)
        infile = open(DEFAULT_CONFIG)
        self.yaml = yaml.load(infile)
        infile.close()
        
        self.write()
        
    def rebase(self):
        """
        @brief determine if we are in the MI working git repo.  If so set the
               local yaml file with the path.  Note, this MUST be run from the
               root of the local git working directory.
        """
        Log.debug("Rebase IDK working repository")
        idk_repo = os.getcwd();
        
        # We assume we are in the root of the local repository directory because
        # DEFAULT_CONFIG is a relative path from there
        Log.debug("Check for GIT information in: " + os.curdir);
        repo = Repo(idk_repo)
       
        if repo.bare:
            raise IDKWrongRunningDirectory(msg="Please run this process from the root your local MI git repository")
            
        ### This would be nice to ultimately pull from the repo object, but the version of gitpython
        ### installed doesn't support remotes. 
        origin = idk_repo
        #origin = repo.remotes.origin.url
        #Log.debug("Current repo origin: " + origin)
        
        Log.debug( "Does '%s' contain '%s'" % (origin, MI_REPO_NAME))
        if origin.find(MI_REPO_NAME) < 0:
            raise IDKWrongRunningDirectory(msg="Please run this process from the root your local MI git repository")
        
        self.set(YAML_CONFIG_WORKING_REPO, idk_repo)
           
    
    def write(self):
        """
        @brief write the current yaml config to the user 
        """
        ofile = open(CONFIG_FILENAME, 'w')
        ofile.write(yaml.dump(self.yaml, default_flow_style=False))
        ofile.close()
        
    def set(self, name, path):
        """
        @brief change a value in the yaml file
        """
        self.yaml[IDK_YAML_GROUP][name] = path
        self.write()
        
    def get(self, config_name):
        """
        @brief get a named parameter from the yaml file
        @retval value from the yaml file
        """
        return self.yaml[IDK_YAML_GROUP].get(config_name)


class Config(object):
    """
    Config object.
    """
    def __init__(self):
        self.cm = ConfigManager() # ConfigManager instance

    def get(self, name):
        """
        @brief get a named parameter from the yaml file
        @retval value from the yaml file
        """
        return self.cm.get(name)
        
    ###
    #    IDK Configuration Compositions
    ###
    def base_dir(self):
        """
        @brief base directory for the new driver
        @retval dir name
        """
        return self.cm.get(YAML_CONFIG_WORKING_REPO)
        
    def idk_config_dir(self):
        """
        @brief base directory for the new driver
        @retval dir name
        """
        return PATH

    def template_dir(self):
        """
        @brief directory where code templates are stored
        @retval template dir name
        """
        return os.path.join(self.base_dir(), "extern/coi-services/ion/idk/templates")
        
    def rebase(self):
        """
        @brief reset the working repository directory
        """
        self.cm.rebase()


IDK_CFG = Config()


