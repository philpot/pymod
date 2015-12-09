#!/usr/bin/python
# Filename: imghash.py

'''
imghash
@author: Andrew Philpot
@version 0.13

trbot/wat imghash module
file-level hashing of image.  Not concerned with 
maintaining database pointers
Usage: python imghash.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
\t-r, --repo:\trepository root
'''

import logging
from watlog import watlog
logger = watlog("wat.imghash")
logger.info('wat.imghash initialized')

import sys
import getopt
import os
import shutil
import errno
# import time
# import datetime
import Image

import util
from util import safeHex

VERSION = '0.13'

# defaults
VERBOSE = True
REVISION = '$Revision: 22996 $'.replace('$','')

REPO = "/nfs/studio-data/wat/repo/image"

ZEROS = "0" * 40
# CORRUPT = "C" * 40
CORRUPT = ZEROS
# MISSING = "F" * 40
MISSING = ZEROS

## WHAT TO DO IF IMAGE IS MISSING, CORRUPT, etc. 
## IDEA: USE EMPTY IMAGE (pseudohash = all zeros)
## IDEA: USE (branded) text image(s) 
## indicating "IMAGE CORRUPTED", "IMAGE MISSING" 

# See http://stackoverflow.com/questions/273192/python-best-way-to-create-directory-if-it-doesnt-exist-for-file-write#273208

def ensurePathExists(path):
    d = os.path.dirname(path)
    try:
        os.makedirs(d)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class InputError(Error):
    """Exception raised for errors in the input."""
    pass

class MissingInputFile(InputError):
    pass

class CorruptInputFile(InputError):
    pass

class ProcessingError(Error):
    pass

class FailedHashAttemptError(ProcessingError):
    pass

class IntegrityError(Error):
    pass

class SizeMismatchError(IntegrityError):
    pass

class Imghash(object):
    def __init__(self, imageFile, repo=REPO, verbose=VERBOSE, owner=None):
        """imageFile is name of existing jpg or other image file located 
in the file system outside of the repo.  Ensure the file exists 
in the repo, preserving ownership/permissions if possible (TBD).  
Replace image file with link to repo image.  return path to 
repo image file. Optionally (currently non-optional), create 
(update) symbolic link to count and to size metadata.  owner
is used only to contextualize error messages"""
        self.origImage = imageFile
        self.repo = repo
        self.verbose = verbose
        self.owner = owner
        try:
            self.im = Image.open(self.origImage)
        except IOError:
            raise

    def __unicode__(self):
        origImage = hasattr(self, "origImage") and self.origImage
        return '<Imghash %s>' % (os.path.join(os.path.basename(os.path.dirname(origImage)),os.path.basename(origImage)))

    def __str__(self):
        return self.__unicode__()

    def __repr__(self):
        return self.__unicode__()

    mapping = {'JPEG': 'jpg',
               'PNG': 'png',
               'GIF': 'gif'}

    def conventionalExt(self, fmt):
        return Imghash.mapping.get(fmt,fmt.lower())

    def destPath(self):
        p = os.path.join(self.repo, self.hex[0:1], self.hex[0:2], self.hex[0:3], self.hex)
        logger.debug('dest path %s' % p)
        return p

    def destImgFile(self):
        return os.path.join(self.destPath(), """img.%s""" % (self.conventionalExt(self.im.format)))

    def destCountFile(self):
        return os.path.join(self.destPath(), """.%s_%s""" % (self.im.format,"count"))

    def destSizeFile(self):
        return os.path.join(self.destPath(), """.%s_%s""" % (self.im.format,"size"))

    def computehex(self):
        verbose = self.verbose
        self.contents = None
        self.hex = ZEROS
        try:
            stream = file(self.origImage)
            try:
                self.contents = stream.read()
                self.hex = safeHex(self.contents)
            except:
                self.contents = None
                self.hex = corrupt
                raise CorruptInputFile(self.origImage, self.owner)
        except:
            ## can we change the wget crawler to download these
            ## files (referenced from js/img src=...?)
            self.contents = None
            self.hex = MISSING
            raise MissingInputFile(self.origImage, self.owner)

    def hashimage(self):
        # will also create self.contents
        self.computehex()
        # WARNING:
        # this implementation doesn't concern itself with 
        # atomicity and race conditions 
        dest = self.destImgFile()
        ensurePathExists(dest)
        if os.path.exists(dest) and os.path.isfile(dest):
            # EXISTING HASH: link to it
            logger.debug('IMGHASH/REJOIN %s as %s' % (self.origImage, dest))
            logger.debug('REJOIN %s as %s' % (self.origImage, dest))
            os.remove(self.origImage)
            os.symlink(dest, self.origImage)
            # update count metadata
            countFile = self.destCountFile()
            oldCount = int(os.readlink(countFile))
            os.remove(countFile)
            newCount = oldCount + 1
            os.symlink(str(newCount),countFile)
            logger.debug('UPDATECOUNT %s to %s' % (countFile, newCount))
            # check that size metadata still correct
            sizeFile = self.destSizeFile()
            oldSize = int(os.readlink(sizeFile))
            newSize = len(self.contents)
            if oldSize == newSize:
                logger.debug('MATCHSIZE %s as %s' % (sizeFile, oldSize))
                return 0
            else:
                logger.error('MISMATCHSIZE %s: old %s, new %s' % (sizeFile, oldSize, newSize))
                raise SizeMismatchError("At least two %s files of different sizes are hashed to %s" % (self.im.format, dest))
        elif not os.path.exists(dest):
            # NEW HASH: create (move) into repo
            # dest file does not exist
            # rename (mv) src
            logger.debug('IMGHASH/ENROLL %s as %s' % (self.origImage, dest))
            logger.debug('ENROLL %s as %s' % (self.origImage, dest))
            shutil.move(self.origImage, dest)
            os.symlink(dest, self.origImage)
            # broken symlink for count metadata initialized to 1
            countFile = self.destCountFile()
            logger.debug('INITCOUNT %s to %s' % (countFile, 1))
            os.symlink('1',countFile)
            # broken symlink for size metadata initialized to len(self.contents)
            sizeFile = self.destSizeFile()
            size = str(len(self.contents))
            logger.debug('INITSIZE %s to %s' % (sizeFile, size))
            os.symlink(str(size), sizeFile)
            return 1
        else:
            raise FailedHashAttemptError

    def process(self):
        return self.hashimage()

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
    imgfile = args[0]
    ih = Imghash(imgfile, repo=my_repo)
    ih.process()

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of imghash.py
