#!/usr/bin/env python
'''
@author Luke Campbell
@file ion/util/h5_graph.py
'''

from struct import unpack
from StringIO import StringIO
import os

import logging
logger = logging.getLogger('h5debug')


def read_num(buf):
    size = len(buf)
    if size == 1:
        return unpack('B', buf)[0]
    if size == 2:
        return unpack('H', buf)[0]
    if size == 4:
        return unpack('I', buf)[0]
    if size == 8:
        return unpack('Q', buf)[0]

class StringBuffer(StringIO):
    def remaining(self):
        return self.len - self.tell()

class HDFFile:
    superblock = None
    def __init__(self, path):
        self.f = open(path)
        self.superblock = SuperBlock(self.f)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.close()

def out(context,s):
    buf = ''
    for i in xrange(context.indent_guide):
        buf += '    '
    buf += s
    if logger.isEnabledFor(logging.INFO):
        logger.info(buf)

class HDFContext:
    indent_guide = 0
    btrees_loaded = {}
    leaves_loaded = {}
    K=0
    offset_size=8
    length_size=8
    file_object=None
    name_heaps=[]
    object_eof=[]
    def __init__(self, file_object, offset_size, length_size, K):
        self.indent_guide = 0
        self.btrees_loaded = {}
        self.leaves_loaded = {}
        self.file_object = file_object
        self.offset_size = offset_size
        self.length_size = length_size
        self.K = K
        self.name_heaps = []
        self.object_eof = []
    def indent(self):
        out(self, '{')
        self.indent_guide += 1

    def unindent(self):
        self.indent_guide -= 1
        out(self, '}')


def align(file_object): # Align on word boundary
    # Padding offset
    if (file_object.tell() % 8) != 0:
        file_object.read(8 - (file_object.tell() % 8))

class SuperBlock:
    offset=None
    format_signature=None
    superblock_version=None
    file_free_space_version=None
    root_group_symbol_tree_version=None
    shared_header_message_format_version=None
    offset_size=None
    length_size=None
    group_leaf_node_k=None
    group_internal_node_k=None
    file_consistency_flags=None
    indexed_storage_internal_node_k=None
    base_address=None
    address_of_file_free_space=None
    eof_address=None
    driver_info_block_address=None
    root_group_symbol_table_offset=None
    file_object=None

    symbol_table=None

    def __init__(self,file_object):
        self.offset = file_object.tell()

        self.format_signature = file_object.read(8)
        self.superblock_version = read_num(file_object.read(1))
        self.file_free_space_version = read_num(file_object.read(1))
        self.root_group_symbol_tree_version = read_num(file_object.read(1))
        file_object.read(1) # Reserved

        self.shared_header_message_format_version = read_num(file_object.read(1))
        self.offset_size = read_num(file_object.read(1))
        self.length_size = read_num(file_object.read(1))
        file_object.read(1) # Reserved

        self.group_leaf_node_k = read_num(file_object.read(2))
        self.group_internal_node_k = read_num(file_object.read(2))

        self.file_consistency_flags = read_num(file_object.read(4))

        assert self.format_signature == '\x89HDF\x0d\x0a\x1a\x0a'
        if self.superblock_version not in (0x01, 0x00):
            raise IOError("Unsupported HDF5 Superblock Version: %s" % self.superblock_version)

        if self.file_free_space_version != 0x00:
            raise IOError("File Free-Space information version is incorrect: %s" % self.file_free_space_version)

        if self.root_group_symbol_tree_version != 0x00:
            raise IOError("Root Group Symbol Table Version is incorrect: %s" % self.root_group_symbol_tree_version)

        if self.shared_header_message_format_version != 0x00:
            raise IOError("Shared Header Message Format Version is incorrect: %s" % self.shared_header_message_format_version)

        if self.offset_size % 2:
            raise IOError("Impossible offset size: %s" % self.offset_size)

        if self.length_size % 2:
            raise IOError("Impossible length size: %s" % self.length_size)

        if self.superblock_version == 0x01:
            self.indexed_storage_internal_node_k = read_num(file_object.read(2))
            file_object.read(2) # Reserved

        
        self.base_address = read_num(file_object.read(self.offset_size))
        self.address_of_file_free_space = read_num(file_object.read(self.offset_size))
        self.eof_address = read_num(file_object.read(self.offset_size))
        self.driver_info_block_address = read_num(file_object.read(self.offset_size))

        st = os.lstat(file_object.name)
        if st.st_size < self.eof_address:
            raise IOError("EOF exceeds actual file address space: %s" % self.eof_address)

        if st.st_size-1 != self.eof_address:
            logger.warning("Superblock EOF != File EOF (%s != %s)" % (hex(self.eof_address), hex(st.st_size)))


        if self.address_of_file_free_space == 0xFFFFFFFFFFFFFFFFL:
            self.address_of_file_free_space = None
        if self.driver_info_block_address == 0xFFFFFFFFFFFFFFFFL:
            self.driver_info_block_address = None


        self.root_group_symbol_table_offset = file_object.tell()

        self.context = HDFContext(file_object, self.offset_size, self.length_size, self.group_leaf_node_k)
        context = self.context
        out(context,'Reading superblock at: %s' % hex(self.offset))
        context.indent()
        out(context,'Signature Matches HDF5')
        out(context,'Superblock Version: %s' % self.superblock_version)
        out(context,'Offset Size (haddr_t): %s' % self.offset_size)
        out(context,'Length Size (hsize_t): %s' % self.length_size)

        out(context,'Group Leaf Node K: %s' % self.group_leaf_node_k)
        out(context,'Group Internal Node K: %s' % self.group_internal_node_k)
        out(context,'Base Address Offset: %s' % self.base_address)
        out(context,'End-of-File Address: %s' % hex(self.eof_address))
        out(context,'Address of File Free Space: %s' % self.address_of_file_free_space or 'NULL')
        out(context,'Driver Info Block Address: %s' % self.driver_info_block_address or 'NULL')
        if (self.file_consistency_flags & 0x1):
            out(context,'File Consistency Flag: Opened for Write-Access')
        if (self.file_consistency_flags & 0x2):
            out(context,'File Consistency Flag: File Verified')

        self.symbol_table = SymbolTableEntry(self.context)

        context.unindent()


class SymbolTableEntry:
    offset=None
    link_name_offset=None
    object_header_address=None
    cache_type=None
    scratch_pad=None

    
    def __init__(self, context):
        file_object = context.file_object

        self.offset = context.file_object.tell()
        out(context,"Reading Symbol Table @  %s" % hex(self.offset))
        context.indent()


        buf = StringBuffer(file_object.read(context.offset_size * 2))

        self.link_name_offset = read_num(buf.read(context.offset_size))
        out(context,'Link Name Offset: %s' % hex(self.link_name_offset))
        self.object_header_address = read_num(buf.read(context.offset_size))
        out(context,'Object Header Address: %s' % hex(self.object_header_address))

        self.cache_type = read_num(file_object.read(4))
        file_object.read(4) # Reserved / Unused space

        out(context,'Cache Type: %s' % self.cache_type)

        self.scratch_pad = StringBuffer(file_object.read(16))
        end_of_entry = file_object.tell()
            
        # Get the name from the previous named heap since this symbol belongs to its parent not itself
        self.link_name = ''
        if context.name_heaps:
            file_object.seek(context.name_heaps[-1].data_segment_addr + self.link_name_offset)
            done=False
            while not done:
                buf = file_object.read(8)
                for i in buf:
                    if i == '\0':
                        done=True
                        break
                    self.link_name += i

        if self.link_name == '':
            out(context,'Link Name: /')
        else:
            out(context,'Link Name: %s' % self.link_name)
        
        if self.cache_type == 0x0:
            file_object.seek(self.object_header_address)
            self.object_header = ObjectHeader(context)
        
        if self.cache_type == 0x01:
            out(context,'Cache Type: Group Objects Head')
            self.b_tree_addr = read_num(self.scratch_pad.read(context.offset_size))
            self.name_heap_addr = read_num(self.scratch_pad.read(context.offset_size))

            out(context,'B-Tree Address: %s' % hex(self.b_tree_addr))
            out(context,'Name-Heap Address: %s' % hex(self.name_heap_addr))


            file_object.seek(self.name_heap_addr)
            self.local_heap = LocalHeap(context)


            
            # Push the heap onto the context queue
            context.name_heaps.append(self.local_heap)


            file_object.seek(self.object_header_address)
            self.object_header = ObjectHeader(context)
            
            file_object.seek(self.b_tree_addr)
            if self.b_tree_addr not in context.btrees_loaded:
                self.b_tree = BlinkTreeNode(context)

            # Pop off the heap, no one else should use it
            context.name_heaps.pop()
        
        if self.cache_type == 0x02:
            raise NotImplementedError('Not implemeneted')

        file_object.seek(end_of_entry)
        context.unindent()



class ObjectHeader:
    version=None
    number_of_header_message=None
    reference_count=None
    header_size=None
    offset=None
    messages=[]

    def __init__(self, context):
        file_object = context.file_object
        self.offset = file_object.tell()
        out(context,'Reading Data Object Header @ %s' % hex(self.offset))
        context.indent()

        self.version = read_num(file_object.read(1))
        out(context,'Version: %s' % self.version)
        file_object.read(1) # Reserved
        self.number_of_header_message = read_num(file_object.read(2))
        out(context,'Number of messages: %s' % self.number_of_header_message)
        self.reference_count = read_num(file_object.read(4))
        out(context,'Reference count: %s' % self.reference_count)
        self.header_size = read_num(file_object.read(4))
        out(context,'Header Size: %s' % self.header_size)
        context.object_eof.append(self.header_size + file_object.tell())

        for i in xrange(self.number_of_header_message):
            if file_object.tell() < context.object_eof[-1]:
                m = HeaderMessage(context)
                self.messages.append(m)
            else:
                out(context, "Message # > Header Size")

        context.unindent()
        context.object_eof.pop()


class HeaderMessage:
    mtype=None
    size=None
    flags=None
    data=None

    def __init__(self, context):
        # Align file to nearest word
        file_object = context.file_object

        align(file_object)
        out(context,'Object Header @ %s' % hex(file_object.tell()) )
        context.indent()

        self.mtype = read_num(file_object.read(2))
        out(context,'Type: %s' % hex(self.mtype))
        self.size  = read_num(file_object.read(2))
        out(context,'Size: %s' % self.size)
        self.flags = read_num(file_object.read(1))
        file_object.read(3) # Reserved

        end_offset = file_object.tell() + self.size
        

        if self.mtype == 0x00: # NIL Block
            out(context,'Nil Block')

        elif self.mtype == 0x01:
            self.dataspace_message = DataspaceMessage(context)
            
        elif self.mtype == 0x03:
            self.datatype_message = DatatypeMessage(context)

        elif self.mtype == 0x05:
            self.fill_value_message = FillValueMessage(context)

        elif self.mtype == 0x0010: # Continuation block
            self.continuation = ContinuationMessage(context)
            context.object_eof[-1] = self.continuation.continuation_length + self.continuation.continuation_offset

            # Go to the offset
            file_object.seek(self.continuation.continuation_offset)

        elif self.mtype == 0x0011:
            self.symbol_table_message = SymbolTableMessage(context)

        elif self.mtype == 0x000C:
            self.attr = AttributeMessage(context)
            if self.attr.is_vlen_str():
                foffset = file_object.tell()
                file_object.seek(self.attr.global_heap_addr)

                global_heap = GlobalHeap(context)

                self.attr.vlen_str = global_heap.objects[ self.attr.global_heap_index ].data
                
                file_object.seek(foffset)

                out(context,'Vlen-str Value: %s' % self.attr.vlen_str)
        else:
            out(context,'Unknown type')
            file_object.read(self.size) # 
        if self.mtype != 0x0010:
            file_object.seek(end_offset) # Properly move to the end even if all the fields weren't read

        context.unindent()

class SymbolTableMessage:
    btree_addr = None
    local_heap_addr = None
    offset = None

    def __init__(self, context):
        file_object = context.file_object
        self.offset = file_object.tell()

        out(context,"Symbol Table Message @ %s" % hex(self.offset))
        context.indent()


        self.btree_addr = read_num(file_object.read(context.offset_size))
        self.local_heap_addr = read_num(file_object.read(context.offset_size))

        out(context,"B-Tree Address: %s" % hex(self.btree_addr))
        out(context,"Local Heap Address: %s" % hex(self.local_heap_addr))

        foffset = file_object.tell()
        file_object.seek(self.btree_addr)
        if self.btree_addr not in context.btrees_loaded:
            self.btree = BlinkTreeNode(context)
        file_object.seek(foffset)

        context.unindent()



class AttributeMessage:
    offset = None
    version = None
    name_size = None
    datatype_size = None
    dataspace_size = None
    name=None
    datatype=None
    dataspace=None
    data=None

    def __init__(self, context):
        file_object = context.file_object
        out(context,'Attribute Message')
        context.indent()
        self.version = read_num(file_object.read(1))
        file_object.read(1) # Reserved
        self.name_size = read_num(file_object.read(2))
        self.datatype_size = read_num(file_object.read(2))
        self.dataspace_size = read_num(file_object.read(2))
        self.name = file_object.read(self.name_size)
        align(file_object)

        out(context,'Attribute Version: %s' % self.version)
        out(context,'Attribute Name: %s' % self.name)

        foffset = file_object.tell()
        self.datatype = DatatypeMessage(context)
        file_object.seek(foffset + self.datatype_size)
        align(file_object)

        foffset = file_object.tell()
        self.dataspace = DataspaceMessage(context)
        file_object.seek(foffset + self.dataspace_size)
        align(file_object)

        if self.datatype.dt_class == 0x9 and self.datatype.vlen_type == 'string':
            slen = read_num(file_object.read(4))
            self.global_heap_addr = read_num(file_object.read(context.offset_size))
            out(context,'Global Heap Addr: %s' % hex(self.global_heap_addr))
            self.global_heap_index = read_num(file_object.read(4))
            out(context,'Global Heap Object: %s' % hex(self.global_heap_index))
            out(context,'String Length: %s' % slen)

        if self.datatype.dt_class == 0x3:
            dim_len = self.dataspace.dims[0]
            self.data = [file_object.read(self.datatype.size) for i in xrange(dim_len)]
            out(context,'Data: %s' % self.data)
        context.unindent()

    def is_vlen_str(self):
        return (self.datatype.dt_class == 0x9 and self.datatype.vlen_type == 'string')


class BlinkTreeNode:
    offset=None
    signature=None
    node_type=None
    node_level=None
    entries_used=None
    left_addr=None
    right_addr=None
    keys=[]

    def __init__(self, context):
        file_object = context.file_object
        self.offset = file_object.tell()
        if self.offset in context.btrees_loaded:
            raise Exception('Circular Dependency Detected')
        context.btrees_loaded[self.offset] = 1
        out(context,'Reading B-Tree @ %s' % hex(self.offset))
        context.indent()
        self.signature = unpack('4s', file_object.read(4))[0]
        assert self.signature == 'TREE'
        self.node_type = read_num(file_object.read(1))
        self.node_level = read_num(file_object.read(1))
        self.entries_used = read_num(file_object.read(2))
        self.left_addr = read_num(file_object.read(context.offset_size))
        self.right_addr = read_num(file_object.read(context.offset_size))



        out(context,'Node Level: %s' % hex(self.node_level))
        out(context,'Entries Used: %s' % hex(self.entries_used))
        out(context,'Left Address: %s' % hex(self.left_addr))
        out(context,'Right Address: %s' % hex(self.right_addr))
        if self.left_addr == 0xffffffffffffffffL:
            self.left_addr = None
        if self.right_addr ==  0xffffffffffffffffL:
            self.right_addr = None
        
        if self.node_type == 0:
            out(context,'Node Type: Group Node')
            for i in xrange(context.K):
                key = read_num(file_object.read(context.length_size))
                out(context,'Key %s: %s' % (i,hex(key)))
                child = read_num(file_object.read(context.offset_size))
                out(context,'Child %s: %s' % (i,hex(child)))

                if child != 0x0:
                    step = file_object.tell()

                    file_object.seek(child) # GO to the child offset
                    identifier = file_object.read(4)
                    file_object.seek(child)

                    if identifier == 'TREE':
                        if file_object.tell() not in context.btrees_loaded:
                            BlinkTreeNode(context)

                    if identifier == 'SNOD':
                        if file_object.tell() not in context.leaves_loaded:
                            BlinkTreeLeaf(context)


                    file_object.seek(step)
        elif self.node_type == 1:
            out(context,'Node Type: Chunked raw Data Node')

        context.unindent()


class BlinkTreeLeaf:
    offset = None
    signature = None
    version = None
    symbol_number = None
    def __init__(self, context):
        file_object = context.file_object
        self.offset = file_object.tell()
        if self.offset in context.leaves_loaded:
            raise Exception('Circular Dependencey')
        context.leaves_loaded[self.offset] = 1
        self.signature = file_object.read(4)
        assert self.signature == 'SNOD'
        out(context,'B-Tree Leaf @ %s' % hex(self.offset))
        context.indent()
        self.version = read_num(file_object.read(1))
        out(context,'Version: %s' % self.version)
        file_object.read(1) # Reserved
        self.symbol_number = read_num(file_object.read(2))
        out(context,'Number of Symbols: %s' % self.symbol_number)
        for i in xrange(self.symbol_number):
            symbol = SymbolTableEntry(context)

        
        context.unindent()


class LocalHeap:
    offset=None
    signature=None
    version=None
    dss=None
    offset_to_head_of_free=None
    data_segment_addr=None

    def __init__(self, context):
        file_object = context.file_object
        self.offset = file_object.tell()
        out(context,'Reading local heap @ %s' % hex(self.offset))
        context.indent()
        self.signature = unpack('4s', file_object.read(4))[0]
        assert self.signature == 'HEAP'
        self.version = read_num(file_object.read(1))
        file_object.read(3) # Reserved

        self.dss = read_num(file_object.read(context.length_size))
        self.offset_to_head_of_free = read_num(file_object.read(context.length_size))
        self.data_segment_addr = read_num(file_object.read(context.offset_size))

        out(context,'Data segment starts @ %s' % hex(self.data_segment_addr))
        out(context,'Data segment size: %s' % self.dss)
        context.unindent()


class DatatypeMessage:
    dt_class=None
    version = None
    flags = None
    size = None
    properties = None

    def __init__(self, context):
        file_object = context.file_object
        n = read_num(file_object.read(1))
        self.dt_class = (n & 0x0F)
        self.version = (n % 0xF0) >> 4

        out(context,'Datatype Message')
        context.indent()
        out(context,'Version: %s' % self.version)
        out(context,'Class: %s' % self.dt_class)

        bm1 = read_num(file_object.read(1))
        bm2 = read_num(file_object.read(1))
        bm3 = read_num(file_object.read(1))

        self.size = read_num(file_object.read(4))
        out(context,'Size: %s' % self.size)

        if self.dt_class == 0x0: 
            out(context,'Fixed Point Data type')
            self.byte_order = bm1 & 0x1
            self.padding_type = (bm1 & 0x6) >> 1
            self.signed = (bm1 & 0x8) >> 3

            out(context,'Byte order: %s' % self.byte_order)
            out(context, 'Padding: %s' %self.padding_type)
            out(context, 'Signed: %s' %self.signed)

            self.bit_offset = read_num(file_object.read(2))
            self.bit_precision = read_num(file_object.read(2))

        if self.dt_class == 0x3: # String
            out(context,'Fixed Length String')
            flag1 = bm1 & 0xF
            if flag1 == 0x0:
                self.str_padding = 'null-terminate'
            elif flag1 == 0x1:
                self.str_padding = 'null-pad'
            elif flag1 == 0x2:
                self.str_padding = 'space-pad'
            else:
                self.str_padding = 'reserved'

            flag2 = (bm1 & 0xF0) >> 4
            if flag2 == 0x0:
                self.char_set = 'ASCII'
            elif flag2 == 0x1:
                self.char_set = 'UTF8'
            else:
                self.char_set = 'reserved'

            out(context,'String-Padding: %s' % self.str_padding)
            out(context,'String-Charset: %s' % self.char_set)

        if self.dt_class == 0x9:
            out(context, 'Variable Length Data type')
            flag1 = bm1 & 0xF
            if flag1 == 0:
                self.vlen_type = 'sequence'
            elif flag1 == 1:
                self.vlen_type = 'string'
            else:
                self.vlen_type = 'reserved'

            flag2 = (bm1 & 0xF0) >> 4
            if flag2 == 0:
                self.vlen_padding = 'null-terminate'
            elif flag2 == 1:
                self.vlen_padding = 'null-pad'
            elif flag2 == 2:
                self.vlen_padding = 'space-pad'
            else:
                self.vlen_padding = 'reserved'

            flag3 = bm2 & 0xF
            if flag3 == 0:
                self.char_set = 'ASCII'
            elif flag3 == 1:
                self.char_set = 'UTF8'
            else:
                self.char_set = 'reserved'

            out(context,'VLen-Type: %s' % self.vlen_type)
            out(context,'VLen-Padding: %s' % self.vlen_padding)
            out(context,'Charset: %s' % self.char_set)

            self.inner_datatype = DatatypeMessage(context)
        context.unindent()



class DataspaceMessage:
    version=None
    dimensionality=None
    flags=None
    dims=[]

    def __init__(self, context):
        file_object = context.file_object

        out(context,'Dataspace Message')
        context.indent()
        self.dims = []
        self.version = read_num(file_object.read(1))
        self.dimensionality = read_num(file_object.read(1))
        self.flags = read_num(file_object.read(1))
        file_object.read(5) # Reserved

        out(context, 'Dataspace Version: %s' % self.version)
        out(context, 'Dims: %s' % self.dimensionality)

        if (self.flags & 0x1) == 0x1:
            out(context, 'Maximum dims are present')
        if (self.flags & 0x2) == 0x1:
            out(context, 'Permutation indices are present')

        for dim in xrange(self.dimensionality):
            dim_len = read_num(file_object.read(context.length_size))
            self.dims.append(dim_len)
            out(context,'Dim %s Length: %s' % (dim, dim_len))
        if (self.flags & 0x1) == 0x1:
            for dim in xrange(self.dimensionality):
                dim_len = read_num(file_object.read(context.length_size))
                out(context,'Dim %s Max Length: %s' % (dim, dim_len))
        if (self.flags & 0x2) == 0x1:
            for dim in xrange(self.dimensionality):
                dim_len = read_num(file_object.read(context.length_size))
                out(context,'Dim %s Permutation Index Length: %s' % (dim, dim_len))
        context.unindent()


class GlobalHeap:
    offset = None
    signature=None
    version=None
    collection_size=None
    object_offset = None
    objects = {}

    def __init__(self, context):
        file_object = context.file_object
        out(context,'Global Heap')
        context.indent()
        self.offset = file_object.tell()
        self.signature = file_object.read(4)
        assert self.signature == 'GCOL'




        self.version = read_num(file_object.read(1))
        out(context,'Version: %s' %self.version)
        file_object.read(3) # Reserved
        self.collection_size = read_num(file_object.read(context.length_size))
        self.object_offset = file_object.tell()

        while True:
            align(file_object)
            gho = GlobalHeapObject(context)
            self.objects[gho.object_index] = gho
            if gho.object_index == 0:
                break
        context.unindent()


        
class GlobalHeapObject:
    object_index=None
    reference_count=None
    object_size=None
    data=None

    def __init__(self, context):
        file_object = context.file_object
        out(context,'Global Heap Object')
        context.indent()
        out(context, 'Global Heap Object')
        self.object_index = read_num(file_object.read(2))
        out(context, 'Index: %s' %self.object_index)
        self.reference_count = read_num(file_object.read(2))

        file_object.read(4) # Reserved

        self.object_size = read_num(file_object.read(context.length_size))
        if self.object_index > 0:
            self.data = file_object.read(self.object_size)
        context.unindent()

        
class ContinuationMessage:
    continuation_offset=None
    continuation_length=None

    def __init__(self, context):
        file_object = context.file_object
        out(context,'Continuation Block')
        context.indent()
        self.continuation_offset = read_num(file_object.read(context.offset_size))
        self.continuation_length = read_num(file_object.read(context.length_size))
        out(context,'Continuation Offset: %s' % hex(self.continuation_offset))
        out(context,'Continuation Length: %s' % hex(self.continuation_length))

        context.unindent()


class FillValueMessage:
    version=None
    space_alloc_time=None
    write_time=None
    defined=None
    size=None
    fill_value=None

    def __init__(self, context):
        file_object = context.file_object
        out(context,'Fill Value Message')
        context.indent()

        self.version = read_num(file_object.read(1))
        out(context,'Version: %s' % self.version)

        self.space_alloc_time = read_num(file_object.read(1))
        out(context,'Space Allocation Time: %s' % self.space_alloc_time)

        self.write_time = read_num(file_object.read(1))
        out(context,'Write Time: %s' % self.write_time)

        self.defined = read_num(file_object.read(1))
        out(context,'Defined: %s' % self.defined)

        if not (self.version > 1 and self.defined == 0):
            self.size = read_num(file_object.read(4))
            out(context,'Size: %s' % self.size)
            
            self.fill_value = file_object.read(self.size)


        context.unindent()

