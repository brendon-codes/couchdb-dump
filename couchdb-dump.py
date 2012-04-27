#!/usr/bin/env python

import sys
import logging
from optparse import OptionParser

import requests


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


class Dump(object):

    src_url = None

    def __init__(self, src_url):
        """
        Init
        """
        self.src_url = src_url

    def run(self):
        """
        Runner
        """
        logger.info(self._path('_all_docs'))
        return True

    def _path(self, *args):
        """
        Get a path
        """
        return '/'.join([self.src_url] + list(args))


if __name__ == '__main__':
    sys.exit(main())
