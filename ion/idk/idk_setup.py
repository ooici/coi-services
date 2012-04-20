"""
@file coi-services/ion/idk/idk_setup.py
@author Bill French
@brief Setup IDK environment.

The intent is to not use any pyon code so this script can be run
independantly.
"""

import os
import urllib2
import logging
import subprocess

PYTHON = '/Library/Frameworks/Python.framework/Versions/2.7/bin/python'

###
#   Setup Logger
###
LOGLEVEL = logging.INFO
#LOGLEVEL = logging.DEBUG

# create logger
log = logging.getLogger(__name__)
log.setLevel(LOGLEVEL)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(LOGLEVEL)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
log.addHandler(ch)


###
#   IDK Dependancies
###
class IDKDependancy():
    logfile = open('/tmp/run.log', "w");
        
    def version(self):
        return None
    
    def executable(self):
        return None
    
    def is_installed(self):
        if(self.executable() and os.path.exists(self.executable())):
            return True
        
        return False
    
    def install(self):
        if(not self.is_installed()):
            print "- Installing " + self.package()
            self._install()
        else:
            print "- %s already installed." % self.package()
            
    def _install(self):
        raise Exception("Must be overloaded")
        
    def _install_brew(self, package):
        self._run_cmd('brew install %s' % package)
        
    def _install_pip(self, package):
        self._run_cmd('sudo pip install %s' % package)
        
    def _fetch_file(self, url):
        log.debug("Downloading: %s" % url)
        file_name = url.split('/')[-1]
        
        if(os.path.exists('/tmp/' + file_name)):
             log.debug(" -- file already downloaded")
             return '/tmp/' + file_name
            
        u = urllib2.urlopen(url)
        f = open('/tmp/' + file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        print "Downloading: %s Bytes: %s" % (file_name, file_size)

        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8)*(len(status)+1)
            print status,

        f.close()
        return '/tmp/' + file_name
    
    def _run_cmd(self, cmd):
        log.debug(" ** CMD: %s " % cmd)
        if( subprocess.call(cmd, shell=True, stdout=self.logfile, stderr=self.logfile) != 0):
            raise Exception("CMD Failed: " + cmd)
        
                
    
    
class IDKPython(IDKDependancy):
    def package(self): return 'Python 2.7'
    
    def executable(self):
        return PYTHON
    
    def _install(self):
        file = self._fetch_file('http://python.org/ftp/python/2.7.2/python-2.7.2-macosx10.6.dmg')
        
        try:
            mount = self._mount(file)
            self._install_mpkg(mount)
        except:
            raise Exception("Failed to install python")
        finally:
            self._umount(mount)
            
    def _mount_point(self):
        return "/tmp/pythondmg.%d" % os.getpid()
        
    def _mount(self, file):
        self._run_cmd("/usr/bin/hdiutil attach -mountpoint %s %s >/dev/null" % (self._mount_point(), file))
        return self._mount_point()        
        
    def _umount(self, mount):
        self._run_cmd("/usr/bin/hdiutil detach %s >/dev/null" % mount)
        
    def _install_mpkg(self, mount):
        self._run_cmd("sudo installer -pkg %s/Python.mpkg -target / 2>&1 >/dev/null" % mount)
        
            
class IDKSetupTools(IDKDependancy):
    def package(self): return 'Setup Tools 0.6c11'
    
    def _install(self):
        file = self._fetch_file('http://pypi.python.org/packages/2.7/s/setuptools/setuptools-0.6c11-py2.7.egg')
        self._install_egg(file)
    
    def _install_egg(self, file):
        self._run_cmd("sudo sh %s" % file)
        
        
class IDKProfile(IDKDependancy):
    def package(self): return 'bash profile'
    
    def _install(self):
        pass
        
        
class IDKBrew(IDKDependancy):
    def package(self): return 'OSX Homebrew'
    
    def executable(self):
        return '/usr/local/bin/brew'
    
    def _install(self):
        self._run_cmd('sudo /usr/bin/ruby -e "$(curl -fsSL https://raw.github.com/gist/323731)"')
        
        
class IDKGit(IDKDependancy):
    def package(self): return 'GIT'
    
    def executable(self):
        return '/usr/local/bin/git'
    
    def _install(self):
        self._install_brew('git')
        
        
class IDKLibEvent(IDKDependancy):
    def package(self): return 'libevent'
    
    def executable(self):
        return '/usr/local/lib/libevent.a'
    
    def _install(self):
        self._install_brew('libevent')
        
        
class IDKLibYAML(IDKDependancy):
    def package(self): return 'libyaml'
    
    def executable(self):
        return '/usr/local/lib/libyaml.a'
    
    def _install(self):
        self._install_brew('libyaml')
        
        
class IDKRabbitMQ(IDKDependancy):
    def package(self): return 'Rabbit MQ'
    
    def executable(self):
        return '/usr/local/sbin/rabbitmq-server'
    
    def _install(self):
        self._install_brew('rabbitmq')
        
        
class IDKZeroMQ(IDKDependancy):
    def package(self): return 'zero mq'
    
    def executable(self):
        return '/usr/local/Cellar/zeromq'
    
    def _install(self):
        self._install_brew('zeromq')
    
        
class IDKCouchDB(IDKDependancy):
    def package(self): return 'CouchDB'
    
    def executable(self):
        return '/usr/local/bin/couchdb'
    
    def _install(self):
        self._install_brew('couchdb')
    
        
class IDKHDF5(IDKDependancy):
    def package(self): return 'HDF5'
    
    def executable(self):
        return '/usr/local/bin/h5cc'
    
    def _install(self):
        self._install_brew('hdf5')
    
        
class IDKVirtualenv(IDKDependancy):
    def package(self): return 'Virtualenv'
    
    def _install(self):
        self._install_pip('virtualenv')
    
        
class IDKVirtualenvWrapper(IDKDependancy):
    def package(self): return 'Virtualenv Wrapper'
    
    def _install(self):
        self._install_pip('virtualenvwrapper')
    
        
class IDKDistribute(IDKDependancy):
    def package(self): return 'Distribute'
    
    def _install(self):
        self._install_pip('distribute')
    
        
class IDKGEvent(IDKDependancy):
    def package(self): return 'GEvent'
    
    def _install(self):
        self._run_cmd("CFLAGS=-I/usr/local/Cellar/libevent/2.0.14/include sudo pip install gevent")
    
        
class IDKION(IDKDependancy):
    def package(self): return 'ION'
    
    def _install(self):
        basedir = os.environ['HOME'] + "/Workspace/code";
        self._run_cmd("mkdir -p %s" % basedir)
        os.chdir(basedir)
        
        if( os.path.exists(basedir + '/pyon') ):
            os.chdir(basedir + '/pyon')
            self._run_cmd("git pull git://github.com/ooici/pyon.git")
        else:
            os.chdir(basedir)
            self._run_cmd("git clone git://github.com/ooici/pyon.git")
            
        if( os.path.exists(basedir + '/coi-services') ):
            os.chdir(basedir + '/coi-services')
            self._run_cmd("git pull git://github.com/ooici/coi-services.git")
        else:
            os.chdir(basedir)
            self._run_cmd("git clone git://github.com/ooici/coi-services.git")
            
        
class IDKDevelEnv(IDKDependancy):
    def package(self): return 'Python Virtual Environment'
    
    def executable(self):
        basedir = os.environ['HOME'] + "/Workspace/virtenvs/ion27";
    
    def _install(self):
        basedir = self.executable()
        self._run_cmd("mkdir -p %s" % basedir)
        self._run_cmd("mkvirtualenv --no-site-packages --python=/Library/Frameworks/Python.framework/Versions/2.7/bin/python ion27")


class IDKBuildOut(IDKDependancy):
    def package(self): return 'Build Out ION'
    
    def _install(self):
        basedir = os.environ['HOME'] + "/Workspace/code/coi-services";
        os.chdir(basedir)
        self._run_cmd(PYTHON + " bootstrap.py")
        self._run_cmd("bin/buildout")
    
        
    
###
#   IDK Setup Process
###
class IDKSetup():
    """
    Setup IDK Environment.  Download prerequisites and source tree.
    """

    def run(self):
        print "*** IDK Installation ***"
        IDKPython().install()
        IDKSetupTools().install()
        IDKProfile().install()
        IDKBrew().install()
        IDKGit().install()
        IDKLibEvent().install()
        IDKLibYAML().install()
        IDKRabbitMQ().install()
        IDKZeroMQ().install()
        IDKCouchDB().install()
        IDKHDF5().install()
        IDKVirtualenv().install()
        IDKVirtualenvWrapper().install()
        IDKDistribute().install()
        #IDKGEvent().install()
        #IDKION().install()
        #IDKDevelEnv().install()
        #IDKBuildOut().install()

if __name__ == '__main__':
    app = IDKSetup()
    app.run()
