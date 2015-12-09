#!/usr/bin/python
# Filename: watmeta.py

'''
watmeta
@author: Andrew Philpot
@version 0.8

wat watmeta module
Usage: python watmeta.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output

\t--application:\t = escort|johnboard, defaults to escort
\t--task:\t = crawl|extract, defaults to extract
\t--scope:\t = defaults to univ
\t--dsname:\t = db we are adding metadata to, defaults to 'esc501'
\t--source:\t = source id, defaults to 'backpage'
\t--market:\t = market (airport code), defaults to None/NULL
\t--sitekey:\t = sitekey, defaults to None/NULL
\t--datestamp:\t = 8digit YYYYMMDD to crawl/extract from
\t--host:\t = host, defaults to localhost

\t--retained:\t = as used in crawl/extract, default 'defaulted'
\t--likelihood:\t = as used in crawl/extract, default 1
\t--version:\t = as used in crawl/extract, defaults to watmeta version
\t--revision:\t = as used in crawl/extract, defaults to watmeta revision
\t--schema:\t = schema of watmeta, may not match crawl/extract

\t--prop:\t = major aspect of property to record
\t--facet:\t = opt minor aspect of property to record
\t--val:\t = val to record (limit 63 char)
'''

import sys
import getopt
import watdb
from watdb import Watdb
from collections import defaultdict
import util
from watlog import watlog
logger = watlog("wat.watmeta")
logger.info('wat.watmeta initialized')
import web
web.config.debug = False
import socket

version = '0.8'

# defaults
VERBOSE = False

TABLE = 'watmeta'

APPLICATION = 'escort'
TASK = 'extract'
SCOPE = 'wat'
DSNAME = 'esc501'
SOURCE = 'backpage'
MARKET = None
SITEKEY = None
DATESTAMP = '00000000'
HOST = socket.gethostname()

RETAINED = 'defaulted'
LIKELIHOOD = 1
VERSION = '0.8'
REVISION = "$Revision: 22618 $".replace('$','')
SCHEMA = "200"

watmetaClassNames = defaultdict(lambda : "Watmeta",
                                Schema200 = "Watmeta200")

def watmetaClassName(schema):
    return watmetaClassNames["Schema" + str(schema)]

def watmetaClass(schema):
    className = watmetaClassName(schema)
    return globals().get(className)

# 0.5 adds functional interface

def recordMeta(prop, facet, val, **keywords):
    """Would like to set these defaults in param list, but then if a defaulted arg is also passed, we have atype"""
    keywords.setdefault('verbose', VERBOSE)
    keywords.setdefault('application', APPLICATION)
    keywords.setdefault('task', TASK)
    keywords.setdefault('scope', SCOPE)
    keywords.setdefault('dsname', DSNAME)
    keywords.setdefault('source', SOURCE)
    keywords.setdefault('market', MARKET)
    keywords.setdefault('sitekey', SITEKEY)
    keywords.setdefault('datestamp', DATESTAMP)
    keywords.setdefault('host', HOST)
    keywords.setdefault('schema', SCHEMA)
    wm = Watmeta(**keywords)
    wm.setup()
    wm.recordMeta(prop, facet, val)
    wm.teardown()

class Watmeta(object):
    def __init__(self, verbose=VERBOSE,
                       application=APPLICATION,
                       task=TASK,
                       scope=SCOPE,
                       dsname=DSNAME,
                       source=SOURCE,
                       market=MARKET,
                       sitekey=SITEKEY,
                       datestamp=DATESTAMP,
                       host=HOST,
                       retained=RETAINED,
                       likelihood=LIKELIHOOD,
                       version=VERSION,
                       revision=REVISION,
                       schema=SCHEMA):
        '''create Watmeta'''
        self.verbose = verbose
        # must exist
        self.application = application
        self.task = task
        self.scope = scope
        self.dsname = dsname
        self.source = source
        self.datestamp = datestamp
        self.host = host
        self.market = market
        self.sitekey = sitekey
        # metadata
        self.retained = retained
        self.likelihood = likelihood
        self.version = version
        self.revision = revision
        self.specialize(schema)

    # possibly interesting boilerplate from watdb
    # def __init__(self, verbose=VERBOSE, conf=CONF, engine=ENGINE, test=False):
    #     self.verbose = verbose
    #     self.cfg = None
    #     self.conf = conf
    #     self.test = test
    #     if self.conf:
    #         self.readConfig()
    #     self.specialize(engine or self.cfg.get('engine'))

    # def __unicode__(self):
    #     engine = hasattr(self, "engine") and self.engine
    #     conf = hasattr(self, "conf") and self.conf
    #     return '<Watdb %s %s>' % (engine, conf)

    # def __str__(self):
    #     return self.__unicode__()

    # def __repr__(self):
    #     return self.__unicode__()

    # def readConfig(self):
    #     root = os.path.join(sys.path[0], "conf")
    #     self.cfg = jsonconf.chooseJson(self.conf, 'db', root=root)
    #     self.engine = self.cfg['engine']

    def specialize(self, schema):
        try:
            self.__class__ = watmetaClass(schema)
            self.schema = schema
        except:
            logger.warning("Failed to specialize %s to %s" % (self,schema))

class Watmeta200(Watmeta):
    def __init__(self):
        logger.warning("Don't call directly")

    def recordMetaFormals(self):
        """schema 200"""
        return ["application",
                "task",
                "scope",
                "dsname",
                "source",
                "market",
                "sitekey",
                "datestamp",
                "host",

                "retained",
                "likelihood",
                "version",
                "revision",
                "schema"]

    def recordMetaActuals(self):
        """schema 200"""
        return [self.application,
                self.task,
                self.scope,
                self.dsname,
                self.source,
                self.market,
                self.sitekey,
                self.datestamp,
                self.host,

                self.retained,
                self.likelihood,
                self.version,
                self.revision,
                self.schema]

    def setup(self):
        self.mdb = Watdb(conf='watmeta')
        self.mdb.connect()
        self.test = False

    def teardown(self):
        self.mdb = self.mdb.disconnect()

    def recordMeta(self, prop, facet, val):
        """schema 200"""
        formals = self.recordMetaFormals()
        formals.extend(["prop", "facet", "val"])
        values = self.recordMetaActuals()
        values.extend([str(prop), str(facet), str(val)])
        table = "watmeta"
        insert = self.mdb.insertionQuery(table, formals, values)
        self.mid = self.mdb.maybeQuery(insert)
        return self.mid

def main(argv=None):
    '''this is called if run from command line'''
    # process command line arguments
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hv", 
				   ["echo=", "help", "verbose",
                                    "task=",
                                    "application=",
                                    "scope=",
                                    "dsname=",
                                    "source=",
                                    "market=",
                                    "sitekey=",
                                    "datestamp=",
                                    "host=",
                                    "retained=",
                                    "likelihood=",
                                    "version=",
                                    "revision=",
                                    "schema=",
                                    "prop=",
                                    "facet=",
                                    "val="])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # default options
    my_verbose = VERBOSE
    my_application = APPLICATION
    my_task = TASK
    my_scope = SCOPE
    my_dsname = DSNAME
    my_source = SOURCE
    my_market = MARKET
    my_sitekey = SITEKEY
    my_datestamp = DATESTAMP
    my_host = HOST
    my_retained = RETAINED
    my_likelihood = LIKELIHOOD
    my_version = VERSION
    my_revision = REVISION
    my_schema = SCHEMA
    my_prop = None
    my_facet = None
    my_val = None

    # process options
    for o,a in opts:
        if o in ("-h","--help"):
            print __doc__
            sys.exit(0)
        if o in ("--echo", ):
            print a
        if o in ("-v", "--verbose", ):
            my_verbose = True

        if o in ("--application",):
            my_application = a
        if o in ("--task",):
            my_task = a
        if o in ("--scope",):
            my_scope = a
        if o in ("--dsname",):
            my_dsname = a
        if o in ("--source"):
            my_source = a
        if o in ("--market"):
            my_market = a
        if o in ("--sitekey"):
            my_sitekey = a
        if o in ("--datestamp"):
            my_datestamp = a
        if o in ("--host"):
            my_host = a
        if o in ("--retained"):
            my_retained = a
        if o in ("--likelihood"):
            my_likelihood = a
        if o in ("--version"):
            my_version = a
        if o in ("--revision"):
            my_revision = a
        if o in ("--schema"):
            my_schema = a
        if o in ("--prop"):
            my_prop = a
        if o in ("--facet"):
            my_facet = a
        if o in ("--val"):
            my_val = a

    d=dict()
    d['verbose']=my_verbose
    d['application']=my_application
    d['task']=my_task
    d['scope']=my_scope
    d['dsname']=my_dsname
    d['source']=my_source
    d['market']=my_market
    d['sitekey']=my_sitekey
    d['datestamp']=my_datestamp
    d['host']=my_host
    d['retained']=my_retained
    d['likelihood']=my_likelihood
    d['version']=my_version
    d['revision']=my_revision
    d['schema']=my_schema
    recordMeta(my_prop, my_facet, my_val, **d)

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of watmeta.py
