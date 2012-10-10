import os
import base64
from StringIO import StringIO
import zipfile

from ooi.logging import log


def path_subtract(minuend, subtrahend):
    """
    subtract path 2 from path 1

    ex:
     path_subtract("/var/www/htdocs/modules", /var/www/htdocs") ==> "/modules"
    """

    p1 = minuend
    p2 = subtrahend

    # convert a path to a list of directories
    def listify(path):
        def listify_h(acc, path2):
            root, last = os.path.split(path2)
            if last:
                acc.append(last)
                return listify_h(acc, root)
            else:
                acc.append(root)
                acc.reverse()
                return acc

        return listify_h([], path)

    p1l = listify(p1)
    p2l = listify(p2)

    lp1l = len(p1l)
    lp2l = len(p2l)
    assert(lp1l >= lp2l)       # check that minuend is bigger than subtrahend
    assert(p1l[0] == p2l[0])   # check that initial characters are the same ("/" vs "//" or other nonsense)
    assert(p2l == p1l[:lp2l])  # check that there is a prefix match on the 2 lists

    return reduce(os.path.join, p2l[0:1] + p1l[lp2l:lp1l])




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

