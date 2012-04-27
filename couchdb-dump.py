#!/usr/bin/env python

"""
CouchDB DB Dumper

Tested On:

  * Python 2.6
  * Ubuntu 10.04 LTS
  * CouchDB 1.1.1

Requirements:

  $ sudo apt-get install libyajl-dev libyajl1
  $ sudo pip install ijson
  $ sudo pip install requests
  $ sudo pip install CouchDB
"""

import sys
import urllib2
import urllib
import base64
from optparse import OptionParser

import requests
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
    d.run()
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

    _chunk_size = 500
    _progress_interval = None
    _src_url = None

    def __init__(self, src_url):
        """
        Init
        """
        self._progress_interval = self._chunk_size / 10
        self._src_url = src_url.rstrip('/')

    def _run_chunk(self, envelope, skip=0):
        """
        Runner
        """
        url = self._path('_all_docs')
        r = Requester().get(url,
                            params={'limit':self._chunk_size,
                                    'skip':skip},
                            headers={'accept':'application/json'})
        rows = ijson.items(r, 'rows.item')
        sess = requests.session()
        i = 0
        sys.stderr.write("\n")
        for row in rows:
            self._fetch_row(envelope, sess, row)
            if i % self._progress_interval == 0:
                sys.stderr.write('.')
            i += 1
        sys.stderr.write("\n")
        return i

    def run(self):
        url = self._path()
        res = requests.get(url, headers={'accept':'application/json'})
        doc = couchdb_json.decode(res.text)
        doc_count = doc['doc_count']
        envelope = couchdb_write_multipart(sys.stdout, boundary=None)
        done = 0
        sys.stderr.write("Doc Count: %d\n" % doc_count)
        sys.stderr.write("Chunk Size: %d\n" % self._chunk_size)
        while done < doc_count:
            batch_size = self._run_chunk(envelope, done)
            if batch_size == 0:
                break
            else:
                done += batch_size
                sys.stderr.write("%d/%d\n" % (done, doc_count))
        sys.stderr.write("\n")
        sys.stderr.write("Done.\n")
        envelope.close()
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

    def _fetch_row(self, envelope, sess, row):
        """
        Fetches a row
        """
        url = self._path(row['id'])
        res = sess.get(url,
                       params={'attachments':'true'},
                       headers={'accept':'application/json'})
        doc = couchdb_json.decode(res.text)
        self._process_row(envelope, doc)
        return True

    def _path(self, *args):
        """
        Get a path
        """
        return '/'.join([self._src_url] + list(args))


if __name__ == '__main__':
    sys.exit(main())
