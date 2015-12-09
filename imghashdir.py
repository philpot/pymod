#!/usr/bin/python
# Filename: imghashdir.py

'''
imghashdir
@author: Andrew Philpot
@version 0.5

trbot/wat imghashdir module
directory-level hashing of images
suitable to be called at YYYYMMDD dir root
Usage: python imghashdir.py <dir>
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
\t-r, --repo:\trepository root
'''

import logging
from watlog import watlog
logger = watlog("wat.imghash.imghashdir")
logger.info('wat.imghash.imghashdir initialized')

import sys
import getopt
import os
import time
import Image

from imghash import Imghash, Error, InputError, MissingInputFile, CorruptInputFile, ProcessingError, FailedHashAttemptError, IntegrityError, SizeMismatchError, REPO

import util

VERSION = '0.5'

# defaults
VERBOSE = True
REVISION = '$Revision: 22997 $'.replace('$','')

class Imghashdir(object):
    def __init__(self, root, repo=REPO, verbose=VERBOSE, owner=None):
        """calls imghash on all image files below ROOT"""
        self.root = root
        self.repo = repo
        self.verbose = verbose
        self.owner = owner

    def hashdir(self):
        seen = 0
        hashed = 0
        t0 = time.time()
        for (dirpath, dirnames, filenames) in os.walk(self.root, followlinks=True):
            for name in filenames:
                fullpath = os.path.join(dirpath, name)
                if os.path.exists(fullpath) and os.path.isfile(fullpath):
                    try:
                        seen += 1
                        logger.debug('PROCESS file %s' % fullpath)
                        ih = Imghash(fullpath, repo=self.repo, verbose=self.verbose, owner=self.owner)
                        ih.process()
                        hashed +=1
                    except IOError:
                        logger.debug('SKIPPED unreadable %s' % fullpath)
        t1 = time.time()
        logger.info("DIR: %s; SEEN: %s; HASHED: %s; ELAPSED: %s sec" % (self.root, seen, hashed, t1-t0))

    def process(self):
        self.hashdir()

def main(argv=None):
    '''this is called if run from command line'''
    # process command line arguments
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(argv[1:], "hvr:", 
				   ["echo=", "help", "verbose"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # default options
    my_verbose = VERBOSE
    my_repo = REPO
    # process options
    for o,a in opts:
        if o in ("-h","--help"):
            print __doc__
            sys.exit(0)
        if o in ("--echo", ):
            print a
        if o in ("-v", "--verbose", ):
            my_verbose = True
        if o in ("-r", "--repo", ):
            my_repo = a
    imgdir = args[0]
    ihd = Imghashdir(imgdir, repo=my_repo)
    ihd.process()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of imghashdir.py
