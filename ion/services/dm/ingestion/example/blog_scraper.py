#!/usr/bin/env python

'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/ingestion/example/blog_scrapper.py
@description an ingestion utility to stream blog data
'''


import json, urllib2
from interface.objects import BlogPost, BlogAuthor, BlogComment
from pyon.ion.endpoint import StreamPublisher
from pyon.ion.streamproc import StreamProcess
import threading
from pyon.net.endpoint import Publisher

class FeedFormatter(object):
    '''
    A feed formatting class for handling blog queries
    '''
    def __init__(self, blog='saintsandspinners'):
        self.blog = blog
    def query(self):
        url = self.url_formatter(self.blog,'posts')

        request = urllib2.Request(url,None,{'Referer':'http://lukecampbell.github.com/'})
        return urllib2.urlopen(request)
    def query_comments(self,postid):
        url = self.url_formatter(self.blog,'%s/comments' % postid)
        request = urllib2.Request(url,None,{'Referer':'http://lukecampbell.github.com/'})
        return urllib2.urlopen(request)

    def url_formatter(self,blog,post):
        url = 'http://%s.blogspot.com/feeds/%s/default?alt=json' %(blog,post)
        return url


class FeedStreamer(StreamProcess):
    '''
    A streaming process that publishes multiple streams of a blog containing posts and comments

    '''

    entries = []
    def on_start(self):
        '''
        Sets the name, the blog and loads the form feeder (URL and JSON Query Object)
        '''
        self.name = self.CFG.get('name','feed_streamer')
        blog = self.CFG.get('process',{}).get('blog','saintsandspinners')
        self.xp = self.CFG.get('process',{}).get('exchange_point','science_data')
        self.feed = FeedFormatter(blog=blog)

        # Start the thread
        self.run(blog)


    def _on_done(self):
        '''
        Callback for the thread when the query is complete.
          Iterate through the entries and publish each post and comment(s) on an independent stream
        '''
        num=0
        for entry in self.entries:

            p = StreamPublisher(name=(self.xp,'%s.%s' %(self.name,num)),process=self,node=self.container.node)
            p.publish(msg=entry['post'])
            for comment in entry['comments']:
                p.publish(msg=comment)
            num+=1


    def run(self, blog):
        '''
        Initiate the thread to query, organize and publish the data
        '''
        self.production = threading.Thread(target=self._grab,kwargs={'blog':blog,'callback':lambda : self._on_done()})
        self.production.start()

    def _grab(self, blog, callback):
        ''' Threaded query
        Queries the blog
        Organizes the JSON into BlogPosts, BlogAuthors and BlogComments
        pushes each of these onto a queue

        When the query is complete (>6s)
        Call the callback
        '''
        data = json.load(self.feed.query())

        for field in data['feed']['entry']:
            entry = {'post':None, 'comments':[]}

            ######################################
            # Form the object
            ######################################

            title = field['title']['$t']
            id = field['id']['$t'].split('post-')[1]
            aname = field['author'][0]['name']['$t']
            aemail = field['author'][0]['email']['$t']
            updated = field['updated']['$t']
            content = field['content']['$t']
            author = BlogAuthor(aname,aemail)

            ######################################
            # Wrap it
            ######################################

            post = BlogPost(id,title,author,updated,content)
            entry['post'] = post

            ######################################
            # Handle Comments
            ######################################

            comments = json.load(self.feed.query_comments(id))
            if 'entry' in comments['feed']:
                for comment in comments['feed']['entry']:
                    ######################################
                    # Form the object
                    ######################################
                    ref_id = id
                    aname = comment['author'][0]['name']['$t']
                    aemail = comment['author'][0]['email']['$t']
                    author = BlogAuthor(aname,aemail)
                    updated = comment['updated']['$t']
                    content = comment['content']['$t']

                    ######################################
                    # Wrap it
                    ######################################

                    comment = BlogComment(ref_id,author,updated,content)
                    entry['comments'].append(comment)

            ######################################
            # Push entry on queue
            ######################################
            self.entries.append(entry)
        callback()
