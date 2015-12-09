#!/usr/bin/python
# Filename: boutique.py

### REDESIGN of market.py
### (1) use web.py
### generator-based, yields web.storage from which you can whatever you need (url, sitekey)

'''
boutique
@author: Andrew Philpot
@version 0.6

WAT boutique module
Usage: python boutique.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
\t-s, --source:\tsource default backpage
\t-a, --application:\tapplication default escort (johnboard)
\t-c, --city:\tcity, must be quoted and include state, e.g., 'San Jose, CA', no default
\t-m, --market:\tFAA airport code used to designate market, default LAX
\t-t, --tier:\tsee wataux.markettiers, integer 1-99, no default
\t-r, --region:\4-digit region code or 5-char region desig, see wataux.marketregions, no default
'''

import sys
import getopt
# import trbotdb
import watdb
import util
import re
import web
web.config.debug = False

# import logging
from watlog import watlog
logger = watlog("wat.boutique")
logger.info('wat.boutique initialized')

VERSION = '0.6'
REVISION = "$Revision: 22999 $"

# defaults
VERBOSE = True

SOURCE = 'backpage'
APPLICATION = 'escort'
# MARKET = 'LAX'
MARKET = None
CODE = MARKET
CITY = None
SITEKEY = None
TIER = None
REGION = None
# REGIONID = None

boutiqueClassNames = {"backpage": "BackpageBoutique",
                      "cityvibe": "CityvibeBoutique",
                      "eros": "ErosBoutique",
                      "humaniplex": "HumaniplexBoutique",
                      "myredbook": "MyredbookBoutique",
                      "sugardaddy": "SugardaddyBoutique"}

def boutiqueClassName(source):
    return boutiqueClassNames.get(source, "Boutique")

def boutiqueClass(source):
    className = boutiqueClassName(source)
    return globals().get(className)

# moved here from crawl.py

def interpretMarket(desig):
    '''market designator could be:
AAA: three letters means faa airport code, use key "market"
RGAAA: five letters means region designator, use key "region"
1111: four digits means region code, use key "region"
11: one or two digits means tier code, use key "tier"
other string with space or comma in it: city name, use key "city"
any other string: site key, use key "sitekey"
'''
    try:
        i = int(desig)
        if 1000<=i and i<=9999:
            # region code
            return ("region", i)
        elif 0<=i and i<=99:
            # tier code
            return ("tier", i)
    except ValueError:
        pass
    if re.search('^RG[A-Z]{3}', desig):
        # region designator
        return ("region", desig)
    if re.search('^[A-Z]{3}', desig):
        # FAA airport code
        return ("market", desig)
    if " " in desig or "," in desig:
        return ("city", desig)
    return ("sitekey", desig)

# Let's consider that the idea is to get tuples keyed to sitekeys including
# source (backpage, etc.)
# market anchor city/location (San Francisco, CA)
# application (escort, johnboard)
# code (airport code of the anchor city, SFO)
# regionid 4001/region (RGSFO), a grouping of markets
# tier (1=FBI focus cities, etc.)/tiername

class Boutique(object):
    '''create Boutique'''
    def __init__(self, verbose=VERBOSE, application=APPLICATION, 
                 code=CODE, city=CITY, sitekey=SITEKEY,
                 tier=TIER,
                 region=REGION):
        self.verbose = verbose
        self.application = application
        self.code = code if code else None
        self.city = city if city else None
        self.sitekey = sitekey if sitekey else None
        self.tier = tier if tier else None
        (self.region, self.regionid) = (None, None)
        try:
            self.regionid = int(region)
        except:
            self.region = region

    def genRows(self):
        db = watdb.Watdb(conf='wataux', engine=None)
        db.connect()

        required = []
        if self.application:
            required.append(wh('application', self.application))
        else:
            raise ValueError("Must supply application")
        if self.source:
            required.append(wh('source', self.source))
        else:
            raise ValueError("Must supply source")

        options = []
        if self.code:
            options.append(wh('code', self.code))
        if self.city:
            options.append(wh('city', self.city))
        if self.sitekey:
            options.append(wh('sitekey', self.sitekey))
        if self.tier:
            options.append(wh('tier', self.tier))
        if self.region:
            options.append(wh('region', self.region))
        if self.regionid:
            options.append(wh('regionid', self.regionid))
        # logger.info("options = %s", options)
        if options:
            pass
        else:
            raise ValueError("Must supply at least one option: code,city,sitekey,tier,region,regionid")
        
        wheres=required
        wheres.extend(options)

        where = ' and '.join(wheres)
        # logger.info(where)
        
        empty = True
        # formerly db.select('sites_master', where=where):
        sql = 'select * from sites_master where %s' % where
        # logger.info("sql = %s", sql)
        for row in db.maybeFetch(sql):
            empty = False
            yield row

        # am trusting that this causes the db connection to be freed
        db = db.disconnect()
        
        if empty:
            if self.verbose:
                print >> sys.stderr, "No rows were generated for %s" % wheres
            logger.warn("No rows were generated for %s" % wheres)

    def fetchBoutique(self, source, desig):
        """This should take a source such as 'backpage' and a desig such as a sitekey or city name and return a code?"""
        rows = list(self.genRows())
        logger.info("There should be one row, in fact there are %s: %s", len(rows), rows)
        return []

    fetchMarket = fetchBoutique

def wh(column_name, value, rel='='):
    """is sqlquote good enough to prevent SQL injection?"""
    if value:
        return """(`%s` %s %s)"""  % (column_name, rel, watdb.sqlquote(str(value)))
    else:
        raise ValueError

class BackpageBoutique(Boutique):
    def __init__(self, verbose=VERBOSE, application=APPLICATION, 
                 code=CODE, city=CITY, sitekey=SITEKEY,
                 tier=TIER,
                 region=REGION):
        '''create BPM'''
        Boutique.__init__(self, verbose=verbose, application=application, 
                                code=code, city=city, sitekey=sitekey,
                                tier=tier,
                                region=region)
        self.source = 'backpage'

class CityvibeBoutique(Boutique):
    def __init__(self, verbose=VERBOSE, application=APPLICATION, 
                 code=CODE, city=CITY, sitekey=SITEKEY,
                 tier=TIER,
                 region=REGION):
        '''create CVM'''
        Boutique.__init__(self, verbose=verbose, application=application, 
                                code=code, city=city, sitekey=sitekey,
                                tier=tier,
                                region=region)
        self.source = 'cityvibe'

class MyredbookBoutique(Boutique):
    def __init__(self, verbose=VERBOSE, application=APPLICATION, 
                 code=CODE, city=CITY, sitekey=SITEKEY,
                 tier=TIER,
                 region=REGION):
        '''create MRBM'''
        Boutique.__init__(self, verbose=verbose, application=application, 
                                code=code, city=city, sitekey=sitekey,
                                tier=tier,
                                region=region)
        self.source = 'myredbook'

class HumaniplexBoutique(Boutique):
    def __init__(self, verbose=VERBOSE, application=APPLICATION, 
                 code=CODE, city=CITY, sitekey=SITEKEY,
                 tier=TIER,
                 region=REGION):
        '''create HXM'''
        Boutique.__init__(self, verbose=verbose, application=application, 
                                code=code, city=city, sitekey=sitekey,
                                tier=tier,
                                region=region)
        self.source = 'humaniplex'

class ErosBoutique(Boutique):
    def __init__(self, verbose=VERBOSE, application=APPLICATION, 
                 code=CODE, city=CITY, sitekey=SITEKEY,
                 tier=TIER,
                 region=REGION):
        '''create ERM'''
        Boutique.__init__(self, verbose=verbose, application=application, 
                                code=code, city=city, sitekey=sitekey,
                                tier=tier,
                                region=region)
        self.source = 'eros'

class SugardaddyBoutique(Boutique):
    def __init__(self, verbose=VERBOSE, application=APPLICATION, 
                 code=CODE, city=CITY, sitekey=SITEKEY,
                 tier=TIER,
                 region=REGION):
        '''create SDM'''
        Boutique.__init__(self, verbose=verbose, application=application, 
                                code=code, city=city, sitekey=sitekey,
                                tier=tier,
                                region=region)
        self.source = 'sugardaddy'

# 0.5 functional interface

def genSiteKeys(source=SOURCE,
                verbose=VERBOSE, application=APPLICATION, 
                market=MARKET, city=CITY, sitekey=SITEKEY,
                tier=TIER,
                region=REGION):
    return boutiqueClass(source)(verbose=verbose,
                                 application=application,
                                 code=market,
                                 city=city,
                                 sitekey=sitekey,
                                 tier=tier,
                                 region=region).genRows()

def main(argv=None):
    '''this is called if run from command line'''
    # process command line arguments
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(argv[1:], "hvs:a:c:m:t:r:", 
				   ["echo=", "help",
				    "source=", "application=", "city=", "market=", "tier=", "region="])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # default options
    my_verbose = VERBOSE
    my_source = SOURCE
    my_application = APPLICATION
    my_city = CITY
    my_market = MARKET
    my_tier = TIER
    my_region = REGION
    # process options
    for o,a in opts:
        if o in ("-h","--help"):
            print __doc__
            sys.exit(0)
        if o in ("--echo", ):
            print a
        if o in ("-v", "--verbose", ):
            my_verbose = True
        if o in ("-s", "--source", ):
            my_source = a
        if o in ("-a", "--application", ):
            my_application = a
        if o in ("-c", "--city", ):
            my_city = a
        if o in ("-m", "--market", ):
            my_market = a
        if o in ("-t", "--tier", ):
            my_tier = a
        if o in ("-r", "--region", ):
            my_region = a
    mktClass = boutiqueClass(my_source)
    print mktClass
    mkt = mktClass(verbose=my_verbose, 
                   application=my_application, city=my_city, code=my_market,
                   tier=my_tier, region=my_region)
    for row in mkt.genRows():
        print row.source, row.application, row.tier, row.region, row.code, row.sitekey, row.url

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of boutique.py
