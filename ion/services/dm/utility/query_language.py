#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file query_language
@date 06/12/12 10:50
@description DESCRIPTION
'''
from pyparsing import ParseException, Regex, quotedString, CaselessLiteral, MatchFirst, removeQuotes, Optional
from pyon.core.exception import BadRequest


class QueryLanguage(object):
    '''
    Pyon Discovery Query DSL BNF

    SEARCH "model" IS "abc*" FROM "models" AND BELONGS TO "platformDeviceID"
    SEARCH "runtime" IS VALUES FROM 1. TO 100 FROM "devices" AND BELONGS TO "RSN resource ID"
    BELONGS TO "org resource ID"
    SEARCH "model" IS "sbc*" FROM "deices" ORDER BY "name" LIMIT 30 AND BELONGS TO "platformDeviceID"


             <sentence> ::= <query> [<query-filter>] [("AND" <sentence>)|("OR" <sentence>)]
                <query> ::= <search-query> | <association-query> | <collection-query>
    <association-query> ::= "BELONGS TO" <resource-id> [<limit-parameter>]
     <collection-query> ::= "IN" <collection-id>
         <search_query> ::= "SEARCH" <field> (<term-query> | <range-query> |<time-query> | <geo-query>) "FROM" <index-name> [<query-parameter>]*
      <query-parameter> ::= <order-parameter> | <limit-parameter> | <offset-parameter>
     <offset-parameter> ::= "SKIP" <integer>
      <order-parameter> ::= "ORDER BY" <limited-string>
      <limit-parameter> ::= "LIMIT" <integer>
      <depth-parameter> ::= "DEPTH" <integer>
           <term-query> ::= "IS" <field-query>
          <field-query> ::= <wildcard-string>
          <range-query> ::= "VALUES" [<from-statement>] [<to-statement>]
           <time-query> ::= "TIME" [<from-statement>] [<to-statement>]
            <geo-query> ::= "GEO" ( <geo-distance> | <geo-bbox> )
         <geo-distance> ::= "DISTANCE" <distance> "FROM" <coords>
       <from-statement> ::= "FROM" <number>
         <to-statement> ::= "TO" <number>
             <geo-bbox> ::= "BOX" "TOP-LEFT" <coords> "BOTTOM-RIGHT" <coords>
           <index-name> ::= <python-string>
        <collection-id> ::= <resource_id>
          <resource-id> ::= REGEX( "[a-zA-Z0-9]+" )
         <query-filter> ::= "FILTER" <python-string>
             <distance> ::= <number> <units>
                <units> ::= ('km' | 'mi' )
               <coords> ::= "LAT" <number> "LON" <number>
                <field> ::= <limited-string> | "*"
       <limited-string> ::= REGEX( "[a-zA-Z0-9_\.]+" )
      <wildcard-string> ::= <python-string>
        <python-string> ::= REGEX( "[^"]+" )
              <number>  ::= <integer> | <double>
              <double>  ::= 0-9 ('.' 0-9)
              <integer> ::= 0-9
    '''

    def __init__(self):


        self.json_query = {'query':{}, 'and': [], 'or': []}
        self.tokens = None
        #--------------------------------------------------------------------------------------
        # <integer> ::= 0-9
        # <double>  ::= 0-9 ('.' 0-9)
        # <number>  ::= <integer> | <double>
        #--------------------------------------------------------------------------------------
        integer = Regex(r'-?[0-9]+') # Word matches space for some reason
        double = Regex(r'-?[0-9]+.?[0-9]*')
        number = double | integer

        #--------------------------------------------------------------------------------------
        # <python-string>   ::= (String surrounded by double-quotes)
        # <wildcard-string> ::= <python-string>
        # <limited-string>  ::= '"' a..z A..Z 9..9 _ . '"' (alpha nums and ._ surrounded by double quotes)
        # <field>           ::= <limited-string> | "*"
        # <coords>          ::= "LAT" <number> "LON" <number>
        # <units>           ::= ('km' | 'mi' | 'nm')
        # <distance>        ::= REGEX(([0-9]*\.?[0-9]*)(km|mi|nm)?)
        #--------------------------------------------------------------------------------------
        python_string = quotedString.setParseAction(removeQuotes)
        wildcard_string = python_string
        limited_string = Regex(r'("(?:[a-zA-Z0-9_\.])*"|\'(?:[a-zA-Z0-9_\.]*)\')').setParseAction(removeQuotes)
        field = limited_string ^ CaselessLiteral('"*"').setParseAction(removeQuotes)
        coords = CaselessLiteral("LAT") + number + CaselessLiteral("LON") + number
        units = CaselessLiteral('km') | CaselessLiteral('mi')
        distance = number + units
        distance.setParseAction( lambda x : self.frame.update({'dist' : float(x[0]), 'units' : x[1]}))


        #--------------------------------------------------------------------------------------
        # Date
        #--------------------------------------------------------------------------------------
        date = python_string
        
        #--------------------------------------------------------------------------------------
        # <query-filter> ::= "FILTER" <python-string>
        # <index-name>   ::= <python-string>
        # <resource-id>  ::= '"' a..z A..Z 0..9 $ _ -'"' (alpha nums surrounded by double quotes)
        # <collection-id> ::= <resource-id>
        #--------------------------------------------------------------------------------------
        query_filter = CaselessLiteral("FILTER") + python_string
        # Add the filter to the frame object
        query_filter.setParseAction(lambda x : self.frame.update({'filter' : x[1]}))
        index_name = MatchFirst(python_string)
        # Add the index to the frame object
        index_name.setParseAction(lambda x : self.frame.update({'index' : x[0]}))
        resource_id = Regex(r'("(?:[a-zA-Z0-9\$_-])*"|\'(?:[a-zA-Z0-9\$_-]*)\')').setParseAction(removeQuotes)
        collection_id = resource_id


        #--------------------------------------------------------------------------------------
        # <from-statement> ::= "FROM" <number> 
        # <to-statement>   ::= "TO" <number>
        #--------------------------------------------------------------------------------------
        from_statement = CaselessLiteral("FROM") + number
        from_statement.setParseAction(lambda x : self.frame.update({'from' : x[1]}))
        to_statement = CaselessLiteral("TO") + number
        to_statement.setParseAction(lambda x : self.frame.update({'to' : x[1]}))


        #--------------------------------------------------------------------------------------
        # <date-from-statement> ::= "FROM" <date> 
        # <date-to-statement>   ::= "TO" <date>
        #--------------------------------------------------------------------------------------
        date_from_statement = CaselessLiteral("FROM") + date
        date_from_statement.setParseAction(lambda x : self.frame.update({'from' : x[1]}))
        date_to_statement = CaselessLiteral("TO") + date
        date_to_statement.setParseAction(lambda x : self.frame.update({'to' : x[1]}))


        #--------------------------------------------------------------------------------------
        # <time-query> ::= "TIME FROM" <date> "TO" <date>
        #--------------------------------------------------------------------------------------
        time_query = CaselessLiteral("TIME") + Optional(date_from_statement) + Optional(date_to_statement)
        time_query.setParseAction(lambda x : self.time_frame())
           # time.mktime(dateutil.parser.parse(x[2])), 'to':time.mktime(dateutil.parser.parse(x[4]))}}))


        #--------------------------------------------------------------------------------------
        # <range-query>  ::= "VALUES" [<from-statement>] [<to-statement>]
        #--------------------------------------------------------------------------------------
        range_query = CaselessLiteral("VALUES") + Optional(from_statement) + Optional(to_statement)
        # Add the range to the frame object
        range_query.setParseAction(lambda x : self.range_frame())

        #--------------------------------------------------------------------------------------
        # <geo-distance> ::= "DISTANCE" <distance> "FROM" <coords>
        # <geo-bbox>     ::= "BOX" "TOP-LEFT" <coords> "BOTTOM-RIGHT" <coords>
        #--------------------------------------------------------------------------------------
        geo_distance = CaselessLiteral("DISTANCE") + distance + CaselessLiteral("FROM") + coords
        geo_distance.setParseAction(lambda x : self.frame.update({'lat': float(x[5]), 'lon':float(x[7])}))
        geo_bbox = CaselessLiteral("BOX") + CaselessLiteral("TOP-LEFT") + coords + CaselessLiteral("BOTTOM-RIGHT") + coords
        geo_bbox.setParseAction(lambda x : self.frame.update({'top_left':[float(x[5]),float(x[3])], 'bottom_right':[float(x[10]),float(x[8])]}))

        #--------------------------------------------------------------------------------------
        # <field-query>  ::= <wildcard-string>
        # <term-query>   ::= "IS" <field-query>
        # <geo-query>    ::= "GEO" ( <geo-distance> | <geo-bbox> )
        #--------------------------------------------------------------------------------------
        field_query = wildcard_string
        term_query = CaselessLiteral("IS") + field_query
        # Add the term to the frame object
        term_query.setParseAction(lambda x : self.frame.update({'value':x[1]}))
        geo_query = CaselessLiteral("GEO") + ( geo_distance | geo_bbox )

        #--------------------------------------------------------------------------------------
        # <limit-parameter>  ::= "LIMIT" <integer>
        # <depth-parameter>  ::= "DEPTH" <integer>
        # <order-parameter>  ::= "ORDER" "BY" <limited-string>
        # <offset-parameter> ::= "SKIP" <integer>
        # <query-parameter>  ::= <order-paramater> | <limit-parameter>
        #--------------------------------------------------------------------------------------
        limit_parameter = CaselessLiteral("LIMIT") + integer
        limit_parameter.setParseAction(lambda x: self.frame.update({'limit' : int(x[1])}))
        depth_parameter = CaselessLiteral("DEPTH") + integer
        depth_parameter.setParseAction(lambda x: self.frame.update({'depth' : int(x[1])}))
        order_parameter = CaselessLiteral("ORDER") + CaselessLiteral("BY") + limited_string
        order_parameter.setParseAction(lambda x: self.frame.update({'order' : {x[2] : 'asc'}}))
        offset_parameter = CaselessLiteral("SKIP") + integer
        offset_parameter.setParseAction(lambda x : self.frame.update({'offset' : int(x[1])}))
        query_parameter = limit_parameter | order_parameter | offset_parameter

        #--------------------------------------------------------------------------------------
        # <search-query>      ::= "SEARCH" <field> (<range-query> | <term-query> | <time-query> | <geo-query>) "FROM" <index-name> [<query-parameter>]*
        # <collection-query>  ::= "IN <collection-id>"
        # <association-query> ::= "BELONGS TO" <resource-id> [ <depth-parameter> ]
        # <query>             ::= <search-query> | <association-query> | <collection-query>
        #--------------------------------------------------------------------------------------
        search_query = CaselessLiteral("SEARCH") + field + (range_query | term_query | time_query | geo_query) + CaselessLiteral("FROM") + index_name + query_parameter*(0,None)
        # Add the field to the frame object
        search_query.setParseAction(lambda x : self.frame.update({'field' : x[1]}))
        collection_query = CaselessLiteral("IN") + collection_id
        collection_query.setParseAction(lambda x : self.frame.update({'collection': x[1]}))
        association_query = CaselessLiteral("BELONGS") + CaselessLiteral("TO") + resource_id + Optional(depth_parameter)
        # Add the association to the frame object
        association_query.setParseAction(lambda x : self.frame.update({'association':x[2]}))
        query = search_query | association_query | collection_query

        #--------------------------------------------------------------------------------------
        # <primary-query>  ::= <query> [<query-filter>]
        # <atom>           ::= <query>
        # <intersection>   ::= "AND" <atom>
        # <union>          ::= "OR" <atom>
        # <sentence>       ::= <primary-query> [<intersection>]* [<union>]*
        #--------------------------------------------------------------------------------------
        primary_query = query + Optional(query_filter)
        # Set the primary query on the json_query to the frame and clear the frame
        primary_query.setParseAction(lambda x : self.push_frame())
        atom = query
        intersection = CaselessLiteral("AND") + atom
        # Add an AND operation to the json_query and clear the frame
        intersection.setParseAction(lambda x : self.and_frame())
        union = CaselessLiteral("OR") + atom
        # Add an OR operation to the json_query and clear the frame
        union.setParseAction(lambda x : self.or_frame())

        self.sentence = primary_query + (intersection ^ union)*(0,None)

    def push_frame(self):
        self.json_query['query'] = self.frame
        self.frame = dict()

    def and_frame(self):
        if self.json_query.has_key('and'):
            self.json_query['and'].append(self.frame)
        else:
            self.json_query['and'] = [self.frame]
        self.frame = dict()

    def or_frame(self):
        if self.json_query.has_key('or'):
            self.json_query['or'].append(self.frame)
        else:
            self.json_query['or'] = [self.frame]
        self.frame = dict()

    def range_frame(self):
        if not 'range' in self.frame:
            self.frame['range'] = {}
        if 'from' in self.frame:
            self.frame['range']['from'] = float(self.frame['from'])
            del self.frame['from']
        if 'to' in self.frame:
            self.frame['range']['to'] = float(self.frame['to'])
            del self.frame['to']
            
    def time_frame(self):
        if not 'time' in self.frame:
            self.frame['time'] = {}
        if 'from' in self.frame:
            self.frame['time']['from'] = self.frame['from']
            del self.frame['from']
        if 'to' in self.frame:
            self.frame['time']['to'] = self.frame['to']
            del self.frame['to']
            

    def parse(self, s):
        '''
        Parses string s and returns a json_query object, self.tokens is set to the tokens
        '''
        self.json_query = {'query':{}, 'and': [], 'or': []}
        self.frame = {}
        try:
            self.tokens = self.sentence.parseString(s)
        except ParseException as e:
            raise BadRequest('%s' % e)

        return self.json_query

    #=========================================
    # Methods for checking the requests
    #=========================================
    @classmethod
    def query_is_term_search(cls, query=None):
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('index') and query.has_key('field') and query.has_key('value'):
            return True
        return False
    @classmethod
    def query_is_range_search(cls,query=None):
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('range') and isinstance(query['range'], dict) and query.has_key('index') and query.has_key('field'):
            return True
        return False
    @classmethod
    def query_is_geo_distance_search(cls,query=None):
        if not (query and isinstance(query,dict)):
            return False
        if query.has_key('dist') and query.has_key('lat') and query.has_key('lon') and query.has_key('units') and query.has_key('index') and query.has_key('field'):
            return True
        return False
    @classmethod
    def query_is_geo_bbox_search(cls,query=None):
        if not (query and isinstance(query,dict)):
            return False
        if query.has_key('top_left') and query.has_key('bottom_right') and isinstance(query['top_left'],list) and len(query['top_left'])==2 and isinstance(query['bottom_right'],list) and len(query['bottom_right'])==2 and query.has_key('field'):
            return True
        return False
    @classmethod
    def query_is_association_search(cls,query=None):
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('association'):
            return True
        return False
    @classmethod
    def query_is_collection_search(cls, query=None):
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('collection'):
            return True
        return False

    @classmethod
    def query_is_time_search(cls,query=None):
        if not query:
            return False
        if not isinstance(query,dict):
            return False
        if query.has_key('time') and isinstance(query['time'], dict) and query.has_key('index') and query.has_key('field'):
            return True
        return False

    @classmethod
    def match(cls, event = None, query = None):

        field_val = getattr(event,query['field'])

        if cls.query_is_term_search(query):
            # This is a term search - always a string

            #@todo implement using regex to mimic lucene...

            if str(field_val) == query['value']:
                return True

        elif cls.query_is_range_search(query):
            # always a numeric value - float or int
            if (field_val >=  query['range']['from']) and (field_val <= query['range']['to']):
                return True
            else:
                return False

        elif cls.query_is_geo_distance_search(query):
            #@todo - wait on this one...
            pass

        elif cls.query_is_geo_bbox_search(query):
            #@todo implement this now.

#            self.assertTrue(retval == {'and':[], 'or':[], 'query':{'field':'location', 'top_left':[0.0, 40.0],
#                                                                       'bottom_right': [40.0, 0.0], 'index':'index'}})

#            if field_val

            pass
        else:
            raise BadRequest("Missing parameters value and range for query: %s" % query)

    @classmethod
    def evaluate_condition(cls, event = None, query_dict = {} ):

        query = query_dict['query']
        or_queries= query_dict['or']
        and_queries = query_dict['and']

        # if any of the queries in the list of 'or queries' gives a match, publish an event
        if or_queries:
            for or_query in or_queries:
                if cls.match(event, or_query):
                    return True

        # if an 'and query' or a list of 'and queries' is provided, return if the match returns false for
        # any one of them
        if and_queries:
            for and_query in and_queries:
                if not cls.match(event, and_query):
                    return False
        return cls.match(event, query)

