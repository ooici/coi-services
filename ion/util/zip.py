import base64
from StringIO import StringIO
import zipfile

from ooi.logging import log




def zip_of_b64(b64_data, title):
    """
    return a zipfile object based on a base64 input string.

    "title" is a string to provide a helpful error message

    function returns a (zipfile object, err_msg) tuple... zipfile object will be None on error
    """

    log.debug("decoding base64 zipfile for %s", title)
    try:
        zip_str  = base64.decodestring(b64_data)
    except:
        return None, ("could not base64 decode the supplied %s" % title)

    log.debug("opening zipfile for %s", title)
    try:
        zip_file = StringIO(zip_str)
        zip_obj  = zipfile.ZipFile(zip_file)
    except:
        return None, ("could not parse zipfile contained in %s" % title)

    return zip_obj, ""

