#!/usr/bin/env
'''
@author Luke Campbell
@file ion/services/dm/utility/jsonify.py
@description Utilities to make json strings as resource queries and a good template for adding good JSON integration
'''
import json


class JSONtree(object):
    '''
    Constructs a Tree-like object that is supported by the D3 Framework
    '''
    def __init__(self, root_name, leaf=False, association=None, id=None):
        '''
        Constructs a root node with the specified attributes
        @param root_name Name of the node, used as a label.
        @param leaf Flag to determine if this node has no children, or a leaf.
        @param association Label to the association
        @param id Identification string for the node
        '''
        super(JSONtree,self).__init__()
        self.name = root_name
        if not leaf:
            self.children = list()
        self.leaf = leaf
        self.association = association
        self.id=id

    def add_child(self, child, association=None,id=None):
        '''
        Adds a child to this root node
        @param child either a string or jsontree node
        @param association The association label for this relationship, e.g. "has"
        @param id The identification for the new child created (if not a JSONtree object)
        @return the child node
        '''
        if isinstance(child,JSONtree):
            child.association=association
            self.children.append(child)
            return child
        if isinstance(child,str):
            node = JSONtree(root_name=child,leaf=False,association=association,id=id)
            self.children.append(node)
            return node


    def add_leaf(self, name):
        '''
        Adds a leaf to the node
        @param name Name of the leaf
        '''
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
        '''
        Returns a JSON string of this tree structure
        '''
        return json.dumps(self.__dict__())

if __name__ == '__main__':
    JSONtree = jsonify.JSONtree
    i = JSONtree('Instrument')
    i.add_child('Stream','hasStream')
    stream = i['Stream']
    stream.add_child('Definition','hasDefinition')
