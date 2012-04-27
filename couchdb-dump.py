#!/usr/bin/env python

"""
CouchDB DB Dumper

Tested On:

  * Python 2.6
  * Ubuntu 10.04 LTS
  * CouchDB 1.1.1

Requirements:

  $ sudo apt-get install libyajl-dev libyajl1 yajl-tools
  $ sudo pip install ijson
"""

import sys
import logging
import urllib2
import urllib
from optparse import OptionParser

import ijson


logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


def main():
    """
    Main
    """
    usage = 'usage: %prog SRC_URL'
    parser = OptionParser(usage=usage)
    options, args = parser.parse_args()
    if len(args) > 1:
        parser.error('Invalid arguments.')
    elif len(args) == 0:
        parser.error('SRC_URL argument is required.')
    d = Dump(*args)
    d.run()
    return 0


class RequestWithMethod(urllib2.Request):
    """
    Allows setting method in urllib2

    See: http://abhinandh.com/post/2383952338
         /making-a-http-delete-request-with-urllib2
    """

    def __init__(self, url, method, data=None, headers={},
                 origin_req_host=None, unverifiable=False):
        """
        Init
        """
        self._method = method
        urllib2.Request\
               .__init__(self, url, data, headers,
                         origin_req_host, unverifiable)

    def get_method(self):
        """
        Get-Method hook
        """
        if self._method:
            return self._method
        else:
            return urllib2.Request.get_method(self)


class Requester(object):
    """
    Url Fetching Abstraction
    """

    def get(self, url, params=None):
        """
        GET request
        """
        return self.request(url, 'GET', params)

    def request(self, url, method, params=None):
        """
        Run a request
        """
        u = url
        p = None
        if params is None:
            params = {}
        if method in ('GET', 'DELETE'):
            if (len(params) > 0) and ('?' not in url):
                parms = urllib.urlencode(params)
                u = '?'.join((url, parms))
        elif method in ('PUT', 'POST'):
            p = urllib.urlencode(params)
        else:
            raise Exception('Method %s not supported' % method)
        req = RequestWithMethod(u, method)
        try:
            res = urllib2.urlopen(req, p)
        except urllib2.URLError:
            raise Exception('Cannot reach server!')
        return res


class Dump(object):
    """
    Main db dumper
    """

    _src_url = None
    _chunk_size = 3

    def __init__(self, src_url):
        """
        Init
        """
        self._src_url = src_url.rstrip('/')

    def run(self):
        """
        Runner
        """
        url = self._path('_all_docs')
        params = {
            'limit': self._chunk_size
        }
        r = Requester().get(url, params)
        ijson.items(r, 'rows.item')
        print r.read()
        return True

    def _path(self, *args):
        """
        Get a path
        """
        return '/'.join([self._src_url] + list(args))


if __name__ == '__main__':
    sys.exit(main())
