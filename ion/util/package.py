#!/usr/bin/env python

'''
@author Luke Campbell
@file ion/util/package.py
@description Process Egg Packager
'''


from setuptools import setup
from shutil import copytree, move, rmtree
from pkgutil import walk_packages
from copy import deepcopy
import glob
import os
import sys
from tempfile import mkdtemp

tempdir_name = mkdtemp()

def fix_imports(package, basename):
    '''
    Iterates through each module in a package-root and updates the import clauses to include the namespaced version
    '''
    for impt, name, is_pkg in walk_packages([package], '%s.' % package):
        if is_pkg:
            continue
        # Load the source file and replace all instances of "from package" with "from package_0_1"
        source_path = name.replace('.','/') + '.py'
        print 'Modifying', source_path
        if not os.path.exists(source_path):
                raise IOError("Can't find source file %s" % source_path)
        with open(source_path, 'r') as f:
            source_buffer = f.read()
        source_buffer = source_buffer.replace('from %s' % basename, 'from %s' % package)
        source_buffer = source_buffer.replace('import %s' % basename, 'import %s' % package)
        with open(source_path, 'w') as f:
            f.write(source_buffer)


def main(name, version, roots=None):
    '''
    Copies each package root folder into a temporary folder, then updates the source files
    to include the namespaced version and builds an egg.
    '''
    if roots is None:
        roots = find_roots()

    versioned_packages = []
    version_s = version.replace('.','_')
    if os.path.exists(tempdir_name):
        rmtree(tempdir_name)
    os.makedirs(tempdir_name)
    for p in roots:
        p_version = '_'.join([p, version_s])
        copytree(p, p_version)
        versioned_packages.append(p_version)
        fix_imports(p_version, p)
        move(p_version, os.path.join(tempdir_name, p_version))

    cwd = os.getcwd()
    os.chdir(tempdir_name) 

    local_roots = find_roots()
    packages = find_packages(local_roots)
    print 'packages found: ' + ', '.join(packages)

    kwargs = {}
    kwargs['name'] = name
    kwargs['version'] = version
    kwargs['packages'] = packages
    sys.argv = ['setup.py', 'bdist_egg']

    setup(**kwargs)
    egg_path = glob.glob('dist/*egg')[0]

    tail, head = os.path.split(egg_path)
    if os.path.exists(os.path.join(cwd, head)):
        print 'removing', os.path.join(cwd,head)
        os.remove(os.path.join(cwd, head))
    print 'moving egg', egg_path
    move(egg_path, cwd)
    os.chdir(cwd)
    rmtree(tempdir_name)

def find_roots():
    '''
    Determines the packages that exist in ./
    '''
    roots = []
    for impt,name,is_pkg in walk_packages(['.']):
        if is_pkg and impt.path == '.':
            roots.append(name)
    return roots

def find_packages(roots):
    '''
    Recursively finds all packages in a list of paths
    '''
    packages = deepcopy(roots)
    for r in roots:
        q = [r]
        while q:
            pkg = q.pop()
            for impt,name,is_pkg in walk_packages([pkg]):
                path = os.path.join(impt.path, name)
                if is_pkg:
                    pyname = path.replace('/','.')
                    packages.append(pyname)
                    q.append(path)
    return packages

if __name__ == '__main__':
    name = sys.argv[1]
    version = sys.argv[2]
    packages = None
    if len(sys.argv) > 3:
        packages = sys.argv[3:]
    main(name, version, packages)





