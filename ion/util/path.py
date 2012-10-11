import os

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

