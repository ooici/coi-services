#!/usr/bin/env
'''
@author Luke Campbell
@file ion/services/dm/utility/jsonify.py
@description Utility to make json strings
'''
import json


class JSONtree(object):
    def __init__(self, root_name, leaf=False, association=None, id=None):
        super(JSONtree,self).__init__()
        self.name = root_name
        if not leaf:
            self.children = list()
        self.leaf = leaf
        self.association = association
        self.id=id

    def add_child(self, child, association=None,id=None):
        if isinstance(child,JSONtree):
            child.association=association
            self.children.append(child)
            return child
        if isinstance(child,str):
            node = JSONtree(root_name=child,leaf=False,association=association,id=id)
            self.children.append(node)
            return node


    def add_leaf(self, name):
        self.add_child(JSONtree(name,leaf=True))

    def __getitem__(self,item):
        if self.leaf:
            return
        for child in self.children:
            if child.name==item:
                return child

    def __str__(self):
        return str(self.__dict__())

    def __dict__(self):
        d = dict()
        d['name'] = self.name
        if self.id:
            d['id'] = self.id
        if self.association:
            d['association'] = self.association
        if self.leaf:
            return d
        d['children'] = list()
        for child in self.children:
            d['children'].append(child.__dict__())
        return d

    def to_j(self):
        return json.dumps(self.__dict__())

if __name__ == '__main__':
    JSONtree = jsonify.JSONtree
    i = JSONtree('Instrument')
    i.add_child('Stream','hasStream')
    stream = i['Stream']
    stream.add_child('Definition','hasDefinition')