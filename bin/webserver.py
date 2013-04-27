#!/usr/bin/env python

import os
import sys
import SimpleHTTPServer
import SocketServer
import logging
import cgi

PORT = 8000
QUERY_FILE = "../data/www/in/query"

class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def do_GET(self):
#        logging.error(self.headers)
        SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)

    def do_POST(self):
#        logging.error(self.headers)
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })
        if 'q' in form:
            self.write_new_query(form['q'].value)

    def write_new_query(self, query):
        f = open(QUERY_FILE, 'w')
        f.write(query)
        f.close()

    def log_message(self, format, *args):
        pass

    def log_request(self, format, *args):
        pass

    def log_error(self, format, *args):
        pass

def main():
    if( len(sys.argv) == 2 ):
        os.chdir( sys.argv[1] )
        Handler = ServerHandler
        httpd = SocketServer.TCPServer(("", PORT), Handler)
        print "serving at port", PORT
        httpd.serve_forever()
    else:
        print "usage:"
        print sys.argv[0], "<www_root>"

if __name__ == "__main__":
   main()
