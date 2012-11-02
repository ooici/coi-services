import os
import base64
import tempfile
import subprocess

from ion.util.zip import zip_of_b64
from ion.util.path import path_subtract

from ooi.logging import log



class RegisterModulePreparerBase(object):
    """
    class to register a file by putting it in a web-accessible location

    """

    def __init__(self,
                 dest_user='',
                 dest_host='',
                 dest_path='',
                 dest_wwwprefix=''):

        self.dest_user = dest_user
        self.dest_host = dest_host
        self.dest_path = dest_path
        self.dest_wwwprefix = dest_wwwprefix

        #for mock
        self.modules = {}
        self.modules["subprocess"] = subprocess
        self.modules["tempfile"]   = tempfile
        self.modules["os"]         = os


    def get_uploader_class(self):
        """
        this will drive the factory method
        """
        return RegisterModuleUploader


    def get_dest_url(self, dest_filename):
        """
        calculate the destination URL from the filename
        """
        return "%s/%s" % (self.dest_wwwprefix, dest_filename)


    def prepare(self, dest_contents_b64, dest_filename=None):
        """
        validate a file and process it to prepare for uploading

        return RegisterModuleUploader object (or None), message
        """
        return self.uploader_object_factory(dest_contents_b64, dest_filename), ""


    def uploader_object_factory(self, dest_contents_b64, dest_filename):

        cls = self.get_uploader_class()
        ret = cls(dest_user     = self.dest_user,
                  dest_host     = self.dest_host,
                  dest_path     = self.dest_path,
                  dest_file     = dest_filename,
                  dest_contents = dest_contents_b64,
                  dest_url      = self.get_dest_url(dest_filename),
                  modules       = self.modules)
        return ret



class RegisterModulePreparerPy(RegisterModulePreparerBase):
    """
    class to register a python file by putting it in a web-accessible location
    """

    def prepare(self, py_b64, dest_filename=None):
        """
        perform syntax check and return uploader object
        """

        mytempfile   = self.modules["tempfile"]
        myos         = self.modules["os"]
        mysubprocess = self.modules["subprocess"]

        try:
            contents = base64.decodestring(py_b64)
        except Exception as e:
            return None, e.message

        log.debug("creating tempfile with contents")
        f_handle, tempfilename = mytempfile.mkstemp()
        log.debug("writing contents to disk at '%s'", tempfilename)
        myos.write(f_handle, contents)

        log.info("syntax checking file")
        py_proc = mysubprocess.Popen(["python", "-m", "py_compile", tempfilename],
                                        stdout=mysubprocess.PIPE,
                                        stderr=mysubprocess.PIPE)

        py_out, py_err = py_proc.communicate()

        # clean up
        log.debug("removing tempfile at '%s'", tempfilename)

        if 0 != py_proc.returncode:
            return None, ("Syntax check failed.  (STDOUT: %s) (STDERR: %s)"
                           % (py_out, py_err))

        ret = self.uploader_object_factory(py_b64, dest_filename or tempfilename)

        return ret, ""





class RegisterModulePreparerEgg(RegisterModulePreparerBase):
    """
    class to register an egg file by putting it in a web-accessible location
    """

    def get_uploader_class(self):
        """
        this will drive the factory method
        """
        return RegisterModuleUploaderEgg


    def prepare(self, egg_b64, dest_filename=None):
        """
        validate the egg and process it to prepare for uploading

        return RegisterModuleUploader object (or None), message
        """

        egg_zip_obj, b64err = zip_of_b64(egg_b64, "python egg")

        if None is egg_zip_obj:
            return None, ("Base64 error: %s" % b64err)

        #validate egg
        log.debug("validating egg")
        if not "EGG-INFO/PKG-INFO" in egg_zip_obj.namelist():
            return None, "no PKG-INFO found in egg; found %s" % str(egg_zip_obj.namelist())

        log.debug("processing driver")
        pkg_info_data = {}
        pkg_info = egg_zip_obj.read("EGG-INFO/PKG-INFO")
        for l in pkg_info.splitlines():
            log.debug("Reading %s", l)
            tmp = l.partition(": ")
            pkg_info_data[tmp[0]] = tmp[2]

        for f in ["Name", "Version"]:
            if not f in pkg_info_data:
                return None, "Egg's PKG-INFO did not include a field called '%s'" % f

        #determine egg name
        dest_filename = "%s-%s-py2.7.egg" % (pkg_info_data["Name"].replace("-", "_"), pkg_info_data["Version"])
        log.info("Egg filename is '%s'", dest_filename)


        egg_url = self.get_dest_url(dest_filename)
        log.info("Egg url will be '%s'", egg_url)

        egg_url_filename = "%s v%s.url" % (pkg_info_data["Name"].replace("-", "_"), pkg_info_data["Version"])


        ret = self.uploader_object_factory(egg_b64, dest_filename)

        ret.set_egg_urlfile_name(egg_url_filename)


        return ret, ""





class RegisterModuleUploader(object):
    def __init__(self,
                 dest_user='',       # username for scp
                 dest_host='',       # host for scp
                 dest_path='',       # destination path for scp, no trailing slash
                 dest_file='',       # destination filename for scp
                 dest_contents='',   # contents of destination file
                 dest_url='',        # the url to the file after it's been uploaded
                 modules=None):

        self.did_upload = False
        self.subprocess = modules["subprocess"] # for mock purposes
        self.tempfile   = modules["tempfile"]
        self.os         = modules["os"]

        self.dest_user     = dest_user
        self.dest_host     = dest_host
        self.dest_path     = dest_path
        self.dest_file     = dest_file
        self.dest_contents = dest_contents
        self.dest_url      = dest_url


    def get_destination_url(self):
        """
        return the (calculated) download URL
        """
        return self.dest_url


    def upload(self):
        """
        move output egg to another directory / upload it somewhere

        return boolean success of uploading, message
        """

        if self.did_upload:
            return False, "Tried to upload a file twice"

        log.debug("creating tempfile with contents")
        f_handle, tempfilename = self.tempfile.mkstemp()
        log.debug("writing contents to disk at '%s'", tempfilename)
        self.os.write(f_handle, base64.decodestring(self.dest_contents))

        remotefilename = "%s@%s:%s/%s" % (self.dest_user,
                                          self.dest_host,
                                          self.dest_path,
                                          self.dest_file)

        log.info("executing scp: '%s' to '%s'", tempfilename, remotefilename)
        scp_proc = self.subprocess.Popen(["scp", "-v", "-o", "PasswordAuthentication=no",
                                           "-o", "StrictHostKeyChecking=no",
                                          tempfilename, remotefilename],
                                          stdout=self.subprocess.PIPE,
                                          stderr=self.subprocess.PIPE)

        scp_out, scp_err = scp_proc.communicate()

        # clean up
        log.debug("removing tempfile at '%s'", tempfilename)
        self.os.unlink(tempfilename)

        # check scp status
        if 0 != scp_proc.returncode:
            return False, ("Secure copy to %s:%s failed.  (STDOUT: %s) (STDERR: %s)"
                           % (self.dest_host, self.dest_path, scp_out, scp_err))

        self.did_upload = True
        return True, ""






class RegisterModuleUploaderEgg(RegisterModuleUploader):

    def set_egg_urlfile_name(self, name):
        self.egg_urlfile_name = name

    def get_egg_urlfile_name(self):
        return self.egg_urlfile_name

