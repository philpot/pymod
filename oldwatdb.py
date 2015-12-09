#!/usr/bin/python
# Filename: watdb.py

'''
watdb nee trbotdb
@author: Andrew Philpot
@version 0.10

Usage: python watdb.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
'''

## major functionality in this module beginning trbot/wat merge (0.6)
##
## 1. layer above web.py's web.database or raw MySQLdb, using jsonconf
## 2. query support layer, tools for query assert/fetch/update invariants
##
## 

import sys
import getopt
import jsonconf
import os
import util
from util import iterChunks
import re

## v 0.10
import web
assert web.__version__ == '0.37'

# _orig_interpolate=web.db._interpolate # needed?

def _interpolate_ignore_dollar_sign(format):
    # print "enter _interpolate_ignore_dollar_sign"
    return [(0, format)]

web.db._interpolate = _interpolate_ignore_dollar_sign

## end v 0.10

from web.db import sqlquote
from collections import defaultdict

from watlog import watlog
logger = watlog("wat.watdb")
logger.info('wat.watdb initialized')

# WE HAVE TWO ENGINES: MySQLdb and webpy
# note that MySQLdb is a zipped python egg and needs to be be able to
# uncompress into a python-eggs directory.  For generality when
# running as a web server, I placed a directive in httpd.conf, but one
# could also do something like 
# os.environ['PYTHON_EGG__CACHE'] = '/tmp/python-eggs'
import MySQLdb
import web
web.config.debug = False

VERSION = '0.10'
REVISION = "$Revision: 21852 $"
VERBOSE = True

COMMA = ", "
BACKSLASH="\x5c"
SINGLEQUOTE="\x27"
# Must prefix NUL, C-Z, single quote/apostrophe, backslash with backslash 
ESCAPABLE="\x00\x1a\x27\x5c"

ENGINE = 'webpy'
CONF = 'test'

INSERTED=1
FETCHED=2
FETCHFAILED=3
SOLOINSERTNOOP=4
TESTING=5

# def kwoteValue(v):
#     if str(v).upper() == "NULL":
#         return "NULL"
#     else:
#         return (SINGLEQUOTE
#                 + 
#                 "".join([BACKSLASH + c if c in ESCAPABLE else c for c in str(v)])
#                 +
#                 SINGLEQUOTE)

def kwoteValue(v):
    """Hmm, I would prefer using '\'' instead of reverting to " quotes"""
    return str(sqlquote(v))

def wh(column_name, value, rel='='):
    """is sqlquote good enough to prevent SQL injection?"""
    if value:
        return """(`%s` %s %s)"""  % (column_name, rel, kwoteValue(value))
    else:
        raise ValueError

# 16 January 2013 by Philpot
# Intended as context manager for web.config.debug
# 17 January 2013 by Philpot
# I now believe web.config.debug only works when you do it at the beginning, 
# before instantiating anything

class EngineVerbosity():
    """Seems that this should work, but not being reflected in calls to web.db.query"""
    def __init__(self, setting):
        # self.setting = setting
        #print "before, it was %s" % web.config.debug
        #print "init to %s" % setting
        self._setting = setting
    def __enter__(self):
        # self.setting.save()
        self._old = web.config.debug
        web.config.debug = self._setting
        # return self.setting
        #print "set to %s" % self._setting
        #print "wcd %s" % web.config.debug
        return web.config.debug
    def __exit__(self, type, value, traceback):
        # self.setting.restore()
        #print "restore to %s" % self._old
        web.config.debug = self._old
        #del(self._old)
        #del(self._setting)

watdbClassNames = defaultdict(lambda : "Watdb",
                              webpy = "WebpyWatdb",
                              MySQLdb = "MySQLdbWatdb")

def watdbClassName(engine):
    return watdbClassNames[engine]

def watdbClass(engine):
    className = watdbClassName(engine)
    return globals().get(className)

class Watdb(object):
    def __init__(self, verbose=VERBOSE, conf=CONF, engine=ENGINE, test=False):
        self.verbose = verbose
        self.cfg = None
        self.conf = conf
        self.test = test
        if self.conf:
            self.readConfig()
        self.specialize(engine or self.cfg.get('engine'))

    def __unicode__(self):
        engine = hasattr(self, "engine") and self.engine
        conf = hasattr(self, "conf") and self.conf
        return '<Watdb %s %s>' % (engine, conf)

    def __str__(self):
        return self.__unicode__()

    def __repr__(self):
        return self.__unicode__()

    def readConfig(self):
        root = os.path.join(sys.path[0], "conf")
        self.cfg = jsonconf.chooseJson(self.conf, 'db', root=root)
        self.engine = self.cfg['engine']

    def specialize(self, engine):
        try:
            self.__class__ = watdbClass(engine)
            self.engine = engine
        except:
            logger.warning("Failed to specialize %s to %s" % (self,engine))

    def testing(self):
        test = hasattr(self, "test") and self.test
        if test:
            if hasattr(test, "__call__"):
                return test()
            else:
                return True
        return False

    def insertionQuery(self, table, formals, values):
        """INSERT IGNORE is MySQL specific..."""
        return ("INSERT IGNORE INTO `%s`" % table +
                " (" + COMMA.join(["`%s`" % formal for formal in formals]) +") " +
                " VALUES (" + COMMA.join([self.kwote(value) for value in values]) + ")")

    def fetchQuery(self, table, *columnsAndValues):
        "untested"
        j = " AND ".join(["(`%s`=%s)" % (column,self.kwote(value)) 
                          for column,value 
                          in iterChunks(columnsAndValues, 2)])
        query = ("SELECT id FROM `%s`" % table 
                 +
                 (" WHERE " if columnsAndValues else "")
                 +
                 j)
        return query

    ## rest of methods unported
    # def maybeQuery(owner, sql):
    #     """assert SQL, return the record number (if succeed)"""
    #     if testing(owner):
    #         logger.info(sql)
    #         return -1
    #     else:
    #         db = owner.db
    #         cur = db.cursor()
    #         cur.execute(sql, ())
    #         return db.insert_id() or None

    # def maybeFetch(owner, sql):
    #     """assert SQL, return the record number (if succeed)"""
    #     if testing(owner):
    #         logger.info(sql)
    #         return []
    #     else:
    #         db = owner.db
    #         cur = db.cursor()
    #         cur.execute(sql, ())
    #         rows = cur.fetchall()
    #         return rows

    # def mqfi(owner, sql, table, *columnsAndValues):
    #     return maybeQuery(owner, sql) or fetchId(owner, table, *columnsAndValues)

    # def fetchId(owner, table, *columnsAndValues):
    #     if testing(owner):
    #         return -1
    #     else:
    #         sql = ("select id from `%s` where" % table +
    #                " and ".join(["(`%s`=%s)" % (column,kwote(owner,value)) 
    #                              for column,value 
    #                              in iterChunks(columnsAndValues, 2)]))
    #         db = owner.db
    #         cur = db.cursor()
    #         cur.execute(sql, ())
    #         return cur.fetchone()[0]

    # def updateFreq(owner, table, *columnsAndValues):
    #     if testing(owner):
    #         return -1
    #     else:
    #         sql = ("update " + table + " set `freq`=`freq`+1 where" +
    #                " and ".join(["(`%s`=%s)" % (column,kwote(owner,value)) 
    #                              for column,value 
    #                              in iterChunks(columnsAndValues, 2)]))
    #         db = owner.db
    #         cur = db.cursor()
    #         cur.execute(sql, ())

    # def insertionQuery(owner, table, formals, values):
    #     return ("INSERT IGNORE INTO `%s`" % table +
    #             " (" + COMMA.join(["`%s`" % formal for formal in formals]) +") " +
    #             " VALUES (" + COMMA.join([kwote(owner, value) for value in values]) + ")")

    # def fetchQuery(owner, table, *columnsAndValues):
    #     j = " AND ".join(["(`%s`=%s)" % (column,kwote(owner,value)) 
    #                       for column,value 
    #                       in iterChunks(columnsAndValues, 2)])
    #     query = ("SELECT id FROM `%s`" % table 
    #              +
    #              (" WHERE " if columnsAndValues else "")
    #              +
    #              j)
    #     return query

    # def ensureId(owner, insert, fetch):
    #     test = owner.test
    #     if testing(owner):
    #         logger.info(insert or "" + "\n" + fetch or "")
    #         return -1
    #     else:
    #         db = owner.db
    #         cur = db.cursor()
    #         insert = re.sub(r"""%""", "%%", insert)
    #         cur.execute(insert, ())
    #         id = db.insert_id() or None
    #         if id and id>0:
    #             return id
    #         else:
    #             if fetch:
    #                 fetch = re.sub(r"""%""", "%%", fetch)
    #                 cur.execute(fetch, ())
    #                 all = cur.fetchone()
    #                 return all[0] if all else None
    #             else:
    #                 logger.warning("solo insert was no-op")
    #                 return None

cxns = dict()

def findCxn(key):
    return cxns.get(key, None)

class WebpyWatdb(Watdb):
    def __init__(self, verbose=VERBOSE, engine=ENGINE, conf=CONF):
        logger.warning("Don't call directly")

    def connect(self):
        cfg = self.cfg
        key = ("mysql",cfg['user'],cfg['password'],cfg['dsname'],cfg['host'])
        found = findCxn(key)
        if found:
            self.cxn = found
            self.cursor = lambda: self.cxn
            return self
        else:
            self.cxn = web.database(dbn='mysql', user=cfg['user'], passwd=cfg['password'], db=cfg['dsname'], host=cfg['host'])
            self.cursor = lambda: self.cxn

            cxns[key] = self.cxn
            return self

    def disconnect(self):
        self.cxn = None
        return self.cxn

    def kwote(self, thing):
        return kwoteValue(thing)

    def maybeFetch(self, sql):
        """assumes connected.  assert SQL, return the rows"""
        if self.testing():
            logger.info(sql)
            return []
        else:
            with EngineVerbosity(self.verbose):
                rows = self.cxn.query(sql)
            return rows

    def maybeQuery(self, sql):
        """assumes connected.  assert SQL, return the record number (if succeed)"""
        if self.testing():
            logger.info(sql)
            return -1
        else:
            lid = None
            with EngineVerbosity(self.verbose):
                succeed = self.cxn.query(sql)
                if succeed:
                    lid = int(self.cxn.query('select last_insert_id() as id')[0].id)
                return lid

    def fetchId(self, table, *columnsAndValues):
        if self.testing():
            return -1
        else:
            sql = ("select id from `%s` where " % table +
                   " and ".join(["(`%s`=%s)" % (column,self.kwote(value)) 
                                 for column,value 
                                 in iterChunks(columnsAndValues, 2)]))
            with EngineVerbosity(self.verbose):
                rows = self.cxn.query(sql)
            return rows and int(rows[0].id)

    def mqfi(self, sql, table, *columnsAndValues):
        """SQL is probably insertionQuery"""
        return self.maybeQuery(sql) or self.fetchId(table, *columnsAndValues)

    # is this every used?  It should be
    def updateFreq(self, table, *columnsAndValues):
        if self.testing():
            return -1
        else:
            sql = ("update " + table + " set `freq`=`freq`+1 where " +
                   " and ".join(["(`%s`=%s)" % (column,self.kwote(value)) 
                                 for column,value 
                                 in iterChunks(columnsAndValues, 2)]))
            with EngineVerbosity(self.verbose):
                self.cxn.query(sql)

    def ensureId(self, insert, fetch):
        if self.testing():
            logger.info(insert or "" + "\n" + fetch or "")
            # logger.info("EXIT 0")
            return (-1, TESTING)
        else:
            # for LIKE comparisons?
            insert = re.sub(r"""%""", "%%", insert)
            lid = None
            with EngineVerbosity(self.verbose):
                # should wrap this in a transaction?
                succeed = self.cxn.query(insert)
                if succeed:
                    lid = self.cxn.query('select last_insert_id() as id')[0].id or None
                # id = db.insert_id() or None
                if lid and lid>0:
                    # Case I
                    # INSERTED
                    # return (id, True)
                    # logger.info("EXIT I")
                    return (int(lid), INSERTED)
                else:
                    if fetch:
                        fetch = re.sub(r"""%""", "%%", fetch)
                        all = self.cxn.query(fetch)[0]
                        if all:
                            # Case II
                            # already there, FETCHED
                            # return (id, False)
                            # logger.info("EXIT II")
                            return (int(all.id), FETCHED)
                        else:
                            # Case III
                            # tried to fetch, found nothing
                            # return (None, False)
                            logger.warning("FETCH query matched nothing")
                            # logger.info("EXIT III")
                            return (None, FETCHFAILED)
                    else:
                        logger.warning("solo insert was no-op")
                        # Case IV
                        # Fetch was not provided, i.e., insert was mandatory, but it failed
                        # logger.info("EXIT IV")
                        return (None, SOLOINSERTNOOP)


class MySQLdbWatdb(Watdb):
    def __init__(self, verbose=VERBOSE, engine=ENGINE, conf=CONF):
        logger.warning("Don't call directly")

    def connect(self):
        cfg = self.cfg
        self.cxn = MySQLdb.connect(passwd=cfg['password'], db=cfg['dsname'], user=cfg['user'], host=cfg['host'])
        self.cursor = self.cxn.cursor

    def disconnect(self):
        try:
            self.cxn.close()
        except MySQLdb.ProgrammingError, err:
            # don't stress closing a closed connection
            pass
        return self.cxn

    def kwote(self, thing):
        try:
            # to emulate old trbotdb, use MySQLdb handle's escape method
            return self.cxn.escape(util.emittable(thing),MySQLdb.converters.conversions)
        except AttributeError:
            # fall-through: use our own (like webpy case)
            return kwoteValue(thing)

    def maybeQuery(self, sql):
        """assumes connected.  assert SQL, return the record number (if succeed)"""
        if self.testing():
            logger.info(sql)
            return -1
        else:
            cur = self.cxn.cursor()
            cur.execute(sql, ())
            return self.cxn.insert_id() or None

    def maybeFetch(self, sql):
        """assumes connected.  assert SQL, return the rows"""
        if self.testing():
            logger.info(sql)
            return []
        else:
            cur = self.cxn.cursor()
            # this should work but is not; ### fix
            cur.execute(sql, ())
            rows = cur.fetchall()
            return rows


def main(argv=None):
    '''this is called if run from command line'''
    # process command line arguments
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hv",
                                   ["echo=", "help",
                                    "verbose"])
    except getopt.error, msg:
        print >> sys.stderr, msg
        print >> sys.stderr, "for help use --help"
        sys.exit(2)

    # default options
    my_verbose = VERBOSE
    # process options
    for o, a in opts:
        if o in ("-h","--help"):
            print __doc__
            sys.exit(0)
        if o in ("--echo", ):
            print a
        if o in ("-v", "--verbose", ):
            my_verbose = True
                                     
    if my_verbose:
        print >> sys.stderr, "ARGV is %s" % (argv)

    w1 = Watdb(conf='test')
    print w1
    
    w2 = Watdb(conf='test')
    print w2
    w2.connect()
    print w2.cxn
    # print list(w2.maybeFetch('select id,code,tier from markets limit 10'))
    import random
    i = w2.maybeQuery("insert ignore into markets(code,tier) values('abc',%s)" % (random.randint(0,100)))
    print i

    print "fetchId"
    j = w2.fetchId("markets", "tier", 100)
    j = w2.fetchId("phones", "retained", "A'B")
    print j

    print "\nVerbose"
    w2.verbose = True
    j = w2.fetchId("phones", "retained", "A'B")
    print "\nQuiet"
    w2.verbose = False
    j = w2.fetchId("phones", "retained", "A'B")
    exit(0)

    print "mqfi"
    iq = "insert ignore into markets(code,tier) values('xyz',102)"
    k = w2.mqfi(iq, "markets", "code", "xyz")
    print k

    # w3 = Watdb(conf='esc000__sigma', engine=False)
    # print w3
    # w3.connect()
    # print w3.cxn
    # print w3.maybeFetch('select source,market,city from posts limit 10')

    print "updateFreq"
    w2.updateFreq("phones", "phone", "3104488201")
    w2.updateFreq("phones", "phone", "3104488201")
    w2.updateFreq("phones", "phone", "3104488201")

    print "insertionQuery"
    import random
    iq = w2.insertionQuery("phones", ["phone"], [str(random.randint(1111111111,9999999999))])
    print iq

    print "fetchQuery"
    import random
    fq = w2.fetchQuery("phones", "phone", str(random.randint(1111111111,9999999999)),
                       "code", "A'C")
    print fq

 
# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of watdb.py
