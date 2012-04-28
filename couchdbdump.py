#!/usr/bin/env python

"""
CouchDB DB Dumper

Tested On:

  * Python 2.6
  * Ubuntu 10.04 LTS
  * CouchDB 1.1.1

Installing:

  $ sudo apt-get install libyajl-dev libyajl1
  $ sudo pip install ijson
  $ sudo pip install CouchDB
"""

import time
import sys
import urllib2
import urllib
import base64
from optparse import OptionParser

import ijson

from couchdb.multipart import write_multipart as couchdb_write_multipart
from couchdb import json as couchdb_json


def main():
    """
    Main
    """
    try:
        _go()
    except KeyboardInterrupt:
        sys.stderr.write("\nStopping.\n")
        return 1
    else:
        return 0


def _go():
    """
    Go
    """
    usage = 'usage: %prog SRC_URL'
    parser = OptionParser(usage=usage)
    options, args = parser.parse_args()
    if len(args) > 1:
        parser.error('Invalid arguments.')
    elif len(args) == 0:
        parser.error('SRC_URL argument is required.')
    couchdb_json.use('simplejson')
    d = Dump(*args)
    start_time = time.time()
    start_clock = time.clock()
    d.run()
    end_time = time.time()
    end_clock = time.clock()
    sys.stderr.write("Finished.\n")
    sys.stderr.write("Elapsed Time: %f\n" % (end_time - start_time))
    sys.stderr.write("Elapsed Clock: %f\n" % (end_clock - start_clock))
    return True


class RequestWithMethod(urllib2.Request):
    """
    Allows setting method in urllib2

    See: http://abhinandh.com/post/2383952338
         /making-a-http-delete-request-with-urllib2
    """

    def __init__(self, url, method, data=None, headers=None,
                 origin_req_host=None, unverifiable=False):
        """
        Init
        """
        if headers is None:
            headers = {}
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

    def get(self, url, params=None, headers=None):
        """
        GET request
        """
        return self.request(url, 'GET', params, headers)

    def request(self, url, method, params=None, headers=None):
        """
        Run a request
        """
        u = url
        p = None
        if params is None:
            params = {}
        if headers is None:
            headers = {}
        if method in ('GET', 'DELETE'):
            if (len(params) > 0) and ('?' not in url):
                parms = urllib.urlencode(params)
                u = '?'.join((url, parms))
        elif method in ('PUT', 'POST'):
            p = urllib.urlencode(params)
        else:
            raise Exception('Method %s not supported' % method)
        req = RequestWithMethod(u, method, headers=headers)
        try:
            res = urllib2.urlopen(req, p)
        except urllib2.URLError:
            raise Exception('Cannot reach server or bad url!')
        return res


class Dump(object):
    """
    Main db dumper
    """

    _chunk_size = 10000
    _progress_interval = None
    _src_url = None

    def __init__(self, src_url):
        """
        Init
        """
        self._progress_interval = self._chunk_size / 20
        self._src_url = src_url.rstrip('/')

    def _run_chunk(self, envelope, skip=0):
        """
        Runner
        """
        url = self._path('_all_docs')
        r = Requester().get(url,
                            params={'limit': self._chunk_size,
                                    'skip': skip,
                                    'include_docs': 'true',
                                    'attachments': 'true'},
                            headers={'accept': 'application/json'})
        rows = ijson.items(r, 'rows.item')
        i = 0
        sys.stderr.write("\n")
        for row in rows:
            self._process_row(envelope, row['doc'])
            if i % self._progress_interval == 0:
                sys.stderr.write('.')
            i += 1
        sys.stderr.write("\n")
        return i

    def run(self):
        url = self._path()
        res = Requester().get(url, headers={'accept': 'application/json'})
        res_data = res.read()
        res.close()
        doc = couchdb_json.decode(res_data)
        doc_count = doc['doc_count']
        with couchdb_write_multipart(sys.stdout, boundary=None) as envelope:
            done = 0
            sys.stderr.write("Doc Count: %d\n" % doc_count)
            sys.stderr.write("Chunk Size: %d\n" % self._chunk_size)
            while done < doc_count:
                batch_size = self._run_chunk(envelope, done)
                if batch_size == 0:
                    break
                else:
                    done += batch_size
                    sys.stderr.write("%.2f%% %d/%d\n" % \
                                     ((float(done) / float(doc_count) * 100),
                                      done, doc_count))
            sys.stderr.write("\n")
        pass

    def _process_row(self, envelope, doc):
        """
        Processes a row

        See: couchdb.tools.dump
        """
        attachments = doc.pop('_attachments', {})
        jsondoc = couchdb_json.encode(doc)
        if attachments:
            parts = envelope.open({
                'Content-ID': doc['_id'],
                'ETag': '"%s"' % doc['_rev']
            })
            parts.add('application/json', jsondoc)
            for name, info in attachments.items():
                content_type = info.get('content_type')
                ## CouchDB < 0.8
                if content_type is None:
                    content_type = info.get('content-type')
                parts.add(content_type, base64.b64decode(info['data']), {
                    'Content-ID': name
                })
            parts.close()
        else:
            envelope.add('application/json;charset=utf-8', jsondoc, {
                'Content-ID': doc['_id'],
                'ETag': '"%s"' % doc['_rev']
            })
        return True

    def _path(self, *args):
        """
        Get a path
        """
        return '/'.join([self._src_url] + list(args))


if __name__ == '__main__':
    sys.exit(main())
