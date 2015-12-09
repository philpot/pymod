#!/usr/bin/python
# Filename: market.py

'''
market
@author: Andrew Philpot
@version 0.3

trbot market module
Usage: python market.py
Options:
\t-h, --help:\tprint help to STDOUT and quit
\t-v, --verbose:\tverbose output
\t-s, --source:\tsource default backpage
'''

import sys
import getopt
import trbotdb
import util

VERSION = '0.3'
REVISION = "$Revision: 21781 $"

# defaults
VERBOSE = True
SOURCE = 'backpage'
CONNECTKEY='trbaux'

marketClassNames = {"backpage": "BackpageMarket",
		    "cityvibe": "CityvibeMarket",
		    "eros": "ErosMarket",
		    "humaniplex": "HumaniplexMarket",
		    "myredbook": "MyredbookMarket",
                    "sugardaddy": "SugardaddyMarket"}

def marketClassName(source):
    return marketClassNames.get(source, "Market")

def marketClass(source):
    className = marketClassName(source)
    return globals().get(className)

class Market(object):
    def __init__(self, verbose=VERBOSE):
        '''create Market'''
        self.verbose = verbose

    def fetchSiteKeys(self, source, desig):
        """All site keys for market or submarket indicated by desig.
If desig indicates a market, return all sitekeys.  Otherwise, try
to delegate to canonSiteKey"""

        # sql = "select ms.sitekey from marketsites ms, markets m, marketnames mn where ms.code=m.code and m.code=mn.code and mn.name=%s and ms.source=%s" 
    
        # seems better, but still not perfectly normalized
        # but for a market with multiple site keys, returns all
        db = trbotdb.connect(CONNECTKEY)
        c = db.cursor()

        # desig is a market indicator, e.g., IAD, miami
        sql = "select distinct ms.sitekey from marketsites ms, markets m, marketnames mn where ms.code=m.code and m.code=mn.code and (mn.name=%s or mn.code=%s) and ms.source=%s"
        c.execute(sql, (desig,desig,source))
        v = [row[0] for row in c.fetchall()]
        if v:
            trbotdb.disconnect(db)
            return v

        canon = self.canonSiteKey(source, desig)
        if canon:
            return canon

        return []

    def canonSiteKey(self, source, desig):
        """If desig is a sitekey itself, return just it as singleton list (possibly canonicalized)"""

        # desig is a sitekey, e.g., nova, ftlauderdale
        sql = "select distinct ms.sitekey from marketsites ms where ms.sitekey=%s and ms.source=%s"
        db = trbotdb.connect(CONNECTKEY)
        c = db.cursor()
        c.execute(sql, (desig,source))
        v = [row[0] for row in c.fetchall()]
        if v:
            trbotdb.disconnect(db)
            # always only one value
            return [v[0]]

        return []

class BackpageMarket(Market):
    def __init__(self, verbose=VERBOSE):
        '''create BPM'''
        self.source='backpage'
        self.verbose = verbose

    def fetchMarket(self, source, desig):

        sql = "select mn.code from trbaux.marketnames mn where lower(mn.name)=lower(%s)" 
        db = trbotdb.connect(CONNECTKEY)
        c = db.cursor()
        c.execute(sql, (desig,))
        for row in c.fetchall():
            code = row[0]
            if code:
                return code
    
        # next, look at marketsites (neighborhoods/download roots)

        nospaces = desig.replace(' ','')
        sql = "select code from trbaux.marketsites where source=%s and (sitekey=%s or city=%s or replace(city,' ','')=%s)"
        c.execute(sql, (self.source, desig, desig, nospaces))

        for row in c.fetchall():
            code = row[0]
            if code:
                return code

        print >> sys.stderr,  "Warning: s=%s market d=%s not found" % (self.source, desig)
        return desig

class CityvibeMarket(Market):
   def __init__(self, verbose=VERBOSE):
        '''create Market'''
        self.verbose = verbose
        self.source = 'cityvibe'

   def fetchMarket(self, source, desig):

        sql = "select mn.code from trbaux.marketnames mn where lower(mn.name)=lower(%s)" 
        db = trbotdb.connect(CONNECTKEY)
        c = db.cursor()
        c.execute(sql, (desig,))
        for row in c.fetchall():
            code = row[0]
            if code:
                return code
    
        # next, look at marketsites (neighborhoods/download roots)

        sql = "select code from trbaux.marketsites where source=%s and (sitekey=%s or city=%s)"
        c.execute(sql, (source, desig, desig))
        for row in c.fetchall():
            code = row[0]
            if code:
                return code

        print >> sys.stderr,  "Warning: s=%s market d=%s not found" % (self.source, desig)
        return desig


class ErosMarket(Market):
   def __init__(self, verbose=VERBOSE):
        '''create Market'''
        self.verbose = verbose
        self.source = 'eros'

   def fetchMarket(self, source, desig):
       sql = "select mn.code from trbaux.marketnames mn where lower(mn.name)=lower(%s)"
       db = trbotdb.connect(CONNECTKEY)
       c = db.cursor()
       c.execute(sql, (desig))
       for row in c.fetchall():
           code = row[0]
           if code:
               return code

       sql = "select code from trbaux.marketsites where source=%s and sitekey=%s"
       db = trbotdb.connect(CONNECTKEY)
       c = db.cursor()
       c.execute(sql, (source, desig))
       for row in c.fetchall():
           code = row[0]
           if code:
               return code
           
       print >> sys.stderr,  "Warning: s=%s market d=%s not found" % (self.source, desig)
       return desig

class HumaniplexMarket(Market):
   def __init__(self, verbose=VERBOSE):
        '''create Market'''
        self.verbose = verbose
        self.source = 'humaniplex'

   def fetchMarket(self, source, desig):

       sql = "select mn.code from trbaux.marketnames mn where lower(mn.name)=lower(%s)"
       db = trbotdb.connect(CONNECTKEY)
       c = db.cursor()
       c.execute(sql, (desig))
       for row in c.fetchall():
           code = row[0]
           if code:
               return code

       sql = "select code from trbaux.marketsites where source=%s and (sitekey=%s or city=%s)"
       db = trbotdb.connect(CONNECTKEY)
       c = db.cursor()
       c.execute(sql, (source, desig, desig))
       for row in c.fetchall():
           code = row[0]
           if code:
               return code

       print >> sys.stderr,  "Warning: s=%s market d=%s not found" % (self.source, desig)
       return desig
        
class MyredbookMarket(Market):
   def __init__(self, verbose=VERBOSE):
        '''create Market'''
        self.verbose = verbose
        self.source = 'myredbook'

   def fetchMarket(self, source, desig):

       sql = "select mn.code from trbaux.marketnames mn where lower(mn.name)=lower(%s)"
       db = trbotdb.connect(CONNECTKEY)
       c = db.cursor()
       c.execute(sql, (desig))
       for row in c.fetchall():
           code = row[0]
           if code:
               return code

       sql = "select code from trbaux.marketsites where source=%s and (sitekey=%s or city=%s)"
       db = trbotdb.connect(CONNECTKEY)
       c = db.cursor()
       c.execute(sql, (source, desig, desig))
       for row in c.fetchall():
           code = row[0]
           if code:
               return code

       print >> sys.stderr,  "Warning: s=%s market d=%s not found" % (self.source, desig)
       return desig


# 0.3 functional interface

def fsk(source, desig, verbose=VERBOSE):
    mktClass = marketClass(source)
    mkt = mktClass(verbose=verbose)
    # print "mktClass=%s, mkt=%s" % (mktClass, mkt)
    return mkt.fetchSiteKeys(source, desig)

def csk(source, desig, verbose=VERBOSE):
    mktClass = marketClass(source)
    mkt = mktClass(verbose=verbose)
    # print "mktClass=%s, mkt=%s" % (mktClass, mkt)
    return mkt.canonSiteKey(source, desig)

def fm(source, desig, verbose=VERBOSE):
    mktClass = marketClass(source)
    mkt = mktClass(verbose=verbose)
    return mkt.fetchMarket(source, desig)

def main(argv=None):
    '''this is called if run from command line'''
    # process command line arguments
    if argv is None:
        argv = sys.argv
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvs:", 
				   ["echo=", "help",
				    "source"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # default options
    my_verbose = VERBOSE
    my_source = SOURCE
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
    mktClass = marketClass(my_source)
    mkt = mktClass(verbose=my_verbose)
    for arg in args:
        print "arg %s" % arg
        print "fetchSiteKeys(%s,%s) = %s" % (my_source, arg, mkt.fetchSiteKeys(my_source, arg))
        print "fsk(%s,%s) = %s" % (my_source, arg, fsk(my_source, arg))
        print "csk(%s,%s) = %s" % (my_source, arg, csk(my_source, arg))
        print "fetchMarket(%s,%s) = %s" % (my_source, arg, mkt.fetchMarket(my_source, arg))


# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of market.py
