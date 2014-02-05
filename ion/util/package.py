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
from subprocess import Popen
import glob
import os
import sys
from tempfile import mkdtemp

tempdir_name = mkdtemp()

def fix_imports(path, package, versioned_package):
    '''
    Iterates through each module in a package-root and updates the import clauses to include the namespaced version
    '''
    q = [path]
    while q:
        pkg = q.pop()
        for impt, name, is_pkg in walk_packages([pkg]):
            module_path = os.path.join(impt.path, name)
            if not is_pkg:
                source_path = module_path + '.py'
                if not os.path.exists(source_path):
                    raise IOError("Can't find source file %s" % source_path)
                replace_imports(source_path, package, versioned_package)
            else:
                q.append(module_path)

def replace_imports(source_path, package, versioned_package):
    '''
    Replace a source file's imports with a versioned namespace
    '''
    print package
    print versioned_package
    print 'Modifying', source_path
    with open(source_path, 'r') as f:
        source_buffer = f.read()
    source_buffer = source_buffer.replace('from %s' % package, 'from %s' % versioned_package)
    source_buffer = source_buffer.replace('import %s' % package, 'import %s' % versioned_package)
    with open(source_path, 'w') as f:
        f.write(source_buffer)

def make_package(roots, version):
    '''
    Copies the named packages into the temporary directory then applies a namespaced version
    to all the specified packages
    '''
    versioned_packages = []
    for p in roots:
        path, name = os.path.split(p)
        versioned_name = '_'.join([name, version])
        versioned_path = os.path.join(tempdir_name, versioned_name)
        print 'Copying %s to %s' % (p, versioned_path)
        copytree(p, versioned_path)
        versioned_packages.append(versioned_name)
        fix_imports(versioned_path, name, versioned_name)
    local_roots = find_roots(tempdir_name)
    packages = find_packages(tempdir_name, local_roots)
    return packages

def main(name, version, roots=None):
    '''
    Copies each package root folder into a temporary folder, then updates the source files
    to include the namespaced version and builds an egg.
    '''
    if roots is None:
        roots = find_roots()

    # Initialize our temporary directory
    if os.path.exists(tempdir_name):
        rmtree(tempdir_name)
    os.makedirs(tempdir_name)

    print 'working directory:', tempdir_name

    version_s = version.replace('.','_') # 0.1 -> 0_1 and 2.16.3rc -> 2_16_3rc
    packages = make_package(roots, version_s)

    print 'packages found: ' + ', '.join(packages)

    kwargs = {}
    kwargs['name'] = name
    kwargs['version'] = version
    kwargs['packages'] = packages

    build_egg(kwargs)

    rmtree(tempdir_name)

def build_egg(setup_args):
    '''
    Creates a templated setup.py file and uses it to build the distributable egg
    '''
    with open(os.path.join(tempdir_name, 'setup.py'), 'w') as f:
        f.write('''
from setuptools import setup

kwds = %(kwds)s

setup(
    **kwds
)''' % {'kwds' : setup_args})
    command = 'python %s bdist_egg' % os.path.join(tempdir_name, 'setup.py')
    p = Popen(command.split(), cwd=tempdir_name)
    p.wait()
    egg_path = glob.glob(os.path.join(tempdir_name, 'dist/*egg'))[0]

    tail, head = os.path.split(egg_path)
    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, head)):
        print 'removing', os.path.join(cwd,head)
        os.remove(os.path.join(cwd, head))
    print 'moving egg', egg_path
    move(egg_path, cwd)

def find_roots(path):
    '''
    Determines the packages that exist in ./
    '''
    roots = []
    for impt,name,is_pkg in walk_packages([path]):
        if is_pkg and impt.path == path:
            roots.append(name)
    return roots

def find_packages(path, roots):
    '''
    Recursively finds all packages in a list of paths
    '''

    if not path.endswith('/'):
        path = path + '/'
    packages = deepcopy(roots)
    for r in roots:
        r = os.path.join(path, r)
        q = [r]
        while q:
            pkg = q.pop()
            for impt,name,is_pkg in walk_packages([pkg]):
                module_path = os.path.join(impt.path, name)
                if is_pkg:
                    pyname = module_path.replace(path,'').replace('/','.')
                    packages.append(pyname)
                    q.append(module_path)
    return packages

if __name__ == '__main__':
    name = sys.argv[1]
    version = sys.argv[2]
    packages = None
    if len(sys.argv) > 3:
        packages = sys.argv[3:]
    main(name, version, packages)

