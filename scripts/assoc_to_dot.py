#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@date Wed Sep  5 15:36:24 EDT 2012
@file scripts/assoc_to_dot.py
@brief Produces a dot file output for the associations defined
'''

from yaml import load

def dot_string(yaml_doc):
    retval = []
    for definition in yaml_doc['AssociationDefinitions']:
        for domain in definition['domain']:
            for range in definition['range']:
                retval.append('%s -> %s [ label="%s" ]' %(domain, range, definition['predicate']))
    return '\n'.join(retval)

def load_associations(path='res/config/associations.yml'):
    doc = open(path,'r')
    ydoc = load(doc.read())
    return ydoc

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = 'res/config/associations.yml'

    if len(sys.argv) > 2:
        outfile_path = sys.argv[2]
    else:
        outfile_path = 'res/config/associations.dot'

    with open(outfile_path,'w') as f:
        f.write('digraph {\n')
        ydoc = load_associations(path)
        f.write(dot_string(ydoc))
        f.write('}\n')



                


