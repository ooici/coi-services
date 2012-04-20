#!/usr/bin/env python

__author__ = 'tgiguere'

from pyon.public import log
from interface.objects import Variable, Attribute
from ion.agents.eoi.handler.ascii_external_data_handler import *
import numpy
from HTMLParser import HTMLParser
import urllib

class AnchorParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self._link_names = []
        self._in_file_link = False
        self._directory_names = []
        self._in_dir_link = False

    def handle_starttag(self, tag, attrs):
        if tag == 'img' and not self._in_file_link and not self._in_dir_link: #check the associated image first, to make sure it's a file link
            for key, value in attrs:
                if key == 'src' and 'unknown.gif' in value:
                    self._in_file_link = True
                elif key == 'src' and 'folder.gif' in value:
                    self._in_dir_link = True
        if tag =='a' :
            for key, value in attrs:
                if key == 'href':
                    if self._in_file_link:
                        self._link_names.append(value)
                        self._in_file_link = False
                    elif self._in_dir_link:
                        self._directory_names.append(value)
                        self._in_dir_link = False


class HfrRadialDataHandler(AsciiExternalDataHandler):

    def __init__(self, data_provider=None, data_source=None, ext_dataset=None, *args, **kwargs):
        AsciiExternalDataHandler.__init__(self, data_provider, data_source, ext_dataset, *args, **kwargs)
        #TODO: Verify parameters as appropriate IonObjects

        self._variables = []
        self._global_attributes = []
        self._data_source = data_source
        self._load_attributes(data_source)
        self._comments = '%'

    def _load_attributes(self, filename=''):
        import urllib
        column_names = []
        #looping through the whole file to get the attributes; not sure if this is such a good idea
        if filename.startswith('http'):
            f = urllib.urlopen(filename)
        else:
            f = open(filename, 'r')

        in_table_data = False
        correct_table_type = False
        variables_populated = False
        for line in f:
            if in_table_data:
                parsed_line = line.replace('%%', '')
                if len(column_names) == 0:
                    column_names = parsed_line.split()
                    column_names.reverse()
                elif not variables_populated:
                    units = parsed_line.split()
                    units.reverse()
                    index = 0
                    for var in self._variables:
                        var.units = units.pop()
                        attr = Attribute()
                        attr.name = 'units'
                        attr.value = var.units
                        var.attributes.append(attr)
                        attr = Attribute()
                        attr.name = 'long_name'
                        attr.value = column_names.pop()
                        if attr.value == 'U' or attr.value == 'V' or attr.value == 'X' or attr.value == 'Y':
                            attr.value += ' ' + column_names.pop()
                        var.attributes.append(attr)
                    variables_populated = True

            if line.startswith('%TableType:'):
                parsed_line = line.partition(': ')
                if parsed_line[2].startswith('LLUV'):
                    correct_table_type = True
                else:
                    correct_table_type = False
            if line.startswith('%TableStart:'):
                in_table_data = True
            if line.startswith('%TableEnd:') and in_table_data:
                in_table_data = False
                correct_table_type = False


            if not in_table_data:
                self._parse_attribute(line, correct_table_type, self._variables, self._global_attributes)
        f.close()

    def _parse_attribute(self, line='', correct_table_type=False, variables=None, attributes=None):
        #strip out leading %
        new_line = line.replace('%', '')

        parsed_line = new_line.partition(':')
        if parsed_line[0] == 'TableColumnTypes' and correct_table_type:
            cols = parsed_line[2].split(' ')
            index = 0
            for col in cols:
                if not col == '' and not col == '\n':
                    var = Variable()
                    var.attributes = []
                    var.name = col
                    var.index_key = str(index)
                    self._variables.append(var)
                    index += 1
        elif not parsed_line[0].startswith('Table'):
            if not parsed_line[2] == '':
                att = Attribute()
                att.name = parsed_line[0]
                att.value = parsed_line[2].replace('\n', '')
                attributes.append(att)

    def has_new_data(self, **kwargs):
        if 'url' in self._ext_dataset_res.update_description.parameters:
            url = self._ext_dataset_res.update_description.parameters['url']
        else:
            return True

        if 'new_data_check' in self._ext_dataset_res.update_description.parameters:
            list_of_previous_files = self._ext_dataset_res.update_description.parameters['new_data_check']
        else:
            return True

        log.debug('>>>previous files: (%s)\n%s' % (type(list_of_previous_files), list_of_previous_files))

        # Obtain the current list of files from the source
        parser = AnchorParser()
        data = urllib.urlopen(url).read()
        parser.feed(data)
        file_names = parser._link_names

        log.debug('>>>current files:\n%s' % file_names)

        # If the first name in the list of 'new' files is not in the old list, assume new data is present
        if not file_names[0] in list_of_previous_files:
            return True

        # Find the index in the 'old' array that matches the first value of the 'new' array and trim the 'old' array
        # Avoids the issue of 'windowed' data (old items removed)
        sidx = list_of_previous_files.index(file_names[0])
        list_of_previous_files = list_of_previous_files[sidx:]

        if list_of_previous_files == file_names:
            return False

        return True

#        if not 'url' in kwargs:
#            return result
#
#        #TODO: need to get list of previous files from couch, instead of passing in as parameter
#        if not 'previous_files' in kwargs:
#            return result
#
#        url = kwargs['url']
#        list_of_previous_files = kwargs['previous_files']
#
#        parser = AnchorParser()
#        data = urllib.urlopen(url).read()
#        parser.feed(data)
#        directory_names = parser._directory_names
#        for dir_name in directory_names:
#            parser2 = AnchorParser()
#            data2 = urllib.urlopen(url + '/' + dir_name).read()
#            parser2.feed(data2)
#
#            file_names = parser2._link_names
#            for file_name in file_names:
#                file_url = url + dir_name + file_name
#                if not file_url in list_of_previous_files:
#                    list_of_previous_files.append(file_url)
#                    result = True

        #TODO: need to store the list of files somewhere for next time

        return result


    #def acquire_data_by_request(self, request=None, **kwargs):
    #    """
    #    Returns data based on a request containing the name of a variable (request.name) and a tuple of slice objects (slice_)
    #    @param request An object (nominally an IonObject of type PydapVarDataRequest) with "name" and "slice" attributes where: "name" == the name of the variable; and "slice" is a tuple ('var_name, (slice_1(), ..., slice_n()))
    #    """
    #    name = request.name
    #    slice_ = request.slice
    #    typecode = ''
    #    dims = []
    #
    #    if name in self._variables:
    #        var = self._variables[name]
    #        if var.shape:
    #            data = ArrayIterator(var, self._block_size)[slice_]
    #        else:
    #            data = numpy.array(var.getValue())
    #        typecode = var.dtype.char
    #        #dims = var.dimensions
    #        attrs = self.get_attributes()
    #
    #    return name, data, typecode, dims, attrs
