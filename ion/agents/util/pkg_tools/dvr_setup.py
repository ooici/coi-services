#!/usr/bin/env python

"""
@package ion.agents.util.pkg_tools.dvr_setup
@file ion/agents/util/pkg_tools/dvr_setup.py
@author Edward Hunter
@brief A setuptools wrapper to package platform external data dirver eggs.
"""

__author__ = 'Edward Hunter'


"""
python pkg_tools/dvr_setup.py /Users/edward/Documents/Dev/code/test_eggs/drivers/driver_a/v_0_1/driver.py
python pkg_tools/dvr_setup.py /Users/edward/Documents/Dev/code/test_eggs/drivers/driver_a/v_0_2/driver.py

import sys
import pkg_resources

pkg_resources.working_set.add_entry('/Users/edward/Documents/Dev/code/test_eggs/pkg_tools/eggs/driver_a_0_1-0.1-py2.7.egg')
pkg_resources.working_set.add_entry('/Users/edward/Documents/Dev/code/test_eggs/pkg_tools/eggs/driver_a_0_2-0.2-py2.7.egg')

ctr1 = pkg_resources.load_entry_point('driver_a_0_1', 'drivers.platform.a', 'driver-0.1')
ctr2 = pkg_resources.load_entry_point('driver_a_0_2', 'drivers.platform.a', 'driver-0.2')

dvr1 = ctr1()
dvr2 = ctr2()

dvr1.hello()
dvr2.hello()

for x in pkg_resources.iter_entry_points('drivers.platform.a') : print x

"""

import sys
import os
from shutil import copy2, move, rmtree, copytree

from setuptools import setup, find_packages

def check_args():
    if len(sys.argv) != 2 \
        or not isinstance(sys.argv[1], str) \
        or not os.path.isfile(sys.argv[1]):
            usage()
            sys.exit(-1)

def usage():
    print 'Usage: python dvr_setup.py "/path/to/valid/driver/file.py" '

if __name__ == '__main__':
    
    # First check args and report usage.
    check_args()
    
    # Store driver path and tools dir. Get relevant dir paths.
    driver_path = sys.argv[1]
    tools_dir = os.path.dirname(os.path.realpath(__file__))
    (driver_dir, driver_fname) = os.path.split(driver_path)
    (driver_parent_dir, dirver_dir_name) = os.path.split(driver_dir)

    # Put the dirver directory on sys path. Import driver setup_config.
    # Create versioned package and module dir paths.
    sys.path.insert(0, driver_dir)
    from setup_config import versioned_module_name
    package_dir = os.path.join(driver_parent_dir,
                          versioned_module_name)
    module_dir = os.path.join(package_dir,
                         versioned_module_name)

    try:
        
        # Copy the dirver dir contents to the module dir.
        copytree(driver_dir, module_dir)
        
        # Switch to package dir.
        os.chdir(package_dir)
        
        # Set args to create an egg distro. Populate kwargs and call
        # setuptools setup.
        sys.argv[1] = 'bdist_egg'
        from setup_config import setup_kwargs
        from setup_config import entry_point_group
        from setup_config import versioned_constructor
        from setup_config import version
        
        versioned_constructor = versioned_constructor.replace('FILENAME',driver_fname.strip('.py'))
        setup_kwargs['packages'] = find_packages()
        setup_kwargs['entry_points'] = {
            entry_point_group : versioned_constructor
        }
        setup(**setup_kwargs)
        
        # Create tools directories necessary.
        staging_dir = os.path.join(tools_dir, 'staging')    
        eggs_dir = os.path.join(tools_dir, 'eggs')
        if not os.path.isdir(staging_dir):
            os.mkdir(staging_dir)
        if not os.path.isdir(eggs_dir):
            os.mkdir(eggs_dir)
        target_dir = os.path.join(staging_dir, versioned_module_name)
        if os.path.isdir(target_dir):
            rmtree(target_dir)
            os.mkdir(target_dir)
    
        # Copy the egg over.
        egg_name = '%s-%s-py%i.%i.egg' % (versioned_module_name,
                                          version,
                                          sys.version_info.major,
                                          sys.version_info.minor)
        egg_src = os.path.join('dist', egg_name)    
        copy2(egg_src, eggs_dir)
        
        # Copy all the build directories to the staging area.
        egginfo_src = versioned_module_name + '.egg-info'
        build_dst = os.path.join(target_dir, 'build')
        dist_dst = os.path.join(target_dir, 'dist')
        egginfo_dst = os.path.join(target_dir, egginfo_src)
        os.renames('build', build_dst)
        os.renames('dist', dist_dst)
        os.renames(egginfo_src, egginfo_dst)
    
    finally:
        
        # Remote the versioned package module.
        rmtree(package_dir)
    