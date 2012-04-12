from HTMLParser import HTMLParser
import urllib

class AnchorParser(HTMLParser):

    _link_names = []
    _in_file_link = False
    _directory_names = []
    _in_dir_link = False
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

def download(url, path_name):
    """Copy the contents of a file from a given URL
     to a local file.
     """
    import os

    if not os.path.exists(path_name):
        os.mkdir(path_name)

    webFile = urllib.urlopen(url)
    if not os.path.exists(path_name + url.split('/')[-1]):
        localFile = open(path_name + url.split('/')[-1], 'w')
        localFile.write(webFile.read())
        localFile.close()
    else:
        localUUID = ''
        localFile = open(path_name + url.split('/')[-1], 'r')
        for line in localFile.readlines():
            if line.startswith('%UUID:'):
                localUUID = line.partition(':')[2]
                break;
        localFile.close()

        webUUID = ''
        for line in webFile.readlines():
            if line.startswith('%UUID:'):
                webUUID = line.partition(':')[2]
                break;

        if not localUUID == webUUID:
            os.remove(path_name + url.split('/')[-1])
            localFile = open(path_name + url.split('/')[-1], 'w')
            localFile.write(webFile.read())
            localFile.close()

    webFile.close()



if __name__ == '__main__':
    import sys
    if len(sys.argv) == 2:
        try:
            parser = AnchorParser()
            data = urllib.urlopen(sys.argv[1]).read()
            parser.feed(data)
            directory_names = parser._directory_names
            for dir_name in directory_names:
                parser2 = AnchorParser()
                data2 = urllib.urlopen(sys.argv[1] + '/' + dir_name).read()
                parser2.feed(data2)

                file_names = parser2._link_names
                for file_name in file_names:
                    try:
                        url = sys.argv[1] + dir_name + file_name
                        #print url
                        #print dir_name + url.split('/')[-1]
                        download(sys.argv[1] + dir_name + file_name, dir_name)
                    except IOError:
                        pass #don't do anything, just go to the next file
        except IOError:
            print 'Filename not found.'
    else:
        import os
        print 'usage: %s http://server.com/path/to/filename' % os.path.basename(sys.argv[0])