#!/usr/bin/env python

"""
@file coi-services/ion/idk/idk_logger.py
@author Bill French
@brief Setup logging for the idk
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

PATH = os.path.join(os.path.expanduser("~"), ".idk")
CONFIG_FILENAME = os.path.join(PATH, "idk.yml")

DEFAULT_CONFIG = "extern/ion-definitions/res/config/idk.yml"
IDK_YAML_GROUP = "idk"
YAML_CONFIG_WORKING_REPO = "working_repo"

MI_REPO_NAME = "coi-services"
#MI_REPO_NAME = "marine-integrations"


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

        if not os.path.isfile(CONFIG_FILENAME):
            Log.debug("User IDK config doesn't exist: " + CONFIG_FILENAME)
            self._init_config_file()
            
        try:
            Log.debug("Reading IDK config: " + CONFIG_FILENAME)
            infile = open(CONFIG_FILENAME)
            self.yaml = yaml.load(infile)
            infile.close()
            
        except:
            raise IOError("Couldn't read config file \"" + \
                          CONFIG_FILENAME + "\". Check permissions.")
            
    def _init_config_file(self):
        Log.debug("Reading system IDK config: " + DEFAULT_CONFIG)
        idk_repo = os.getcwd();
        
        if not os.path.isfile(DEFAULT_CONFIG):
            raise IDKConfigMissing(msg="Could not read default configuration " + DEFAULT_CONFIG )
        
        # We assume we are in the root of the local repository directory because
        # DEFAULT_CONFIG is a relative path from there
        Log.debug("Check for GIT information in: " + os.curdir);
        repo = Repo(idk_repo)
       
        if repo.bare:
            raise IDKWrongRunningDirectory(msg="Please run this process from the root your local MI git repository")
            
        origin = repo.remotes.origin.url
        Log.debug("Current repo origin: " + origin)
        
        Log.debug( "Does '%s' contain '%s'" % (origin, MI_REPO_NAME))
        if origin.find(MI_REPO_NAME) < 0:
            raise IDKWrongRunningDirectory(msg="Please run this process from the root your local MI git repository")
        
        Log.debug("Read default config: " + DEFAULT_CONFIG)
        infile = open(DEFAULT_CONFIG)
        default_yaml = yaml.load(infile)
        infile.close()
        
        default_yaml[IDK_YAML_GROUP][YAML_CONFIG_WORKING_REPO] = idk_repo
        
        yaml_output = yaml.dump(default_yaml, default_flow_style=False)
        
        Log.debug("Storing YAML: " + yaml_output)
        
        Log.debug("Saving User IDK Config: " + CONFIG_FILENAME)
        ofile = open(CONFIG_FILENAME, 'w')

        ofile.write(yaml_output)
        ofile.close()
        
    def get(self, config_name):
        return self.yaml[IDK_YAML_GROUP].get(config_name)


class Config(object):
    """
    Config object.
    """
    def __init__(self):
        self.cm = ConfigManager() # ConfigManager instance

    def get(self, name):
        self.cm.get(name)
        
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


IDK_CFG = Config()


