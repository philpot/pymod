#!/usr/bin/python
# Filename: wattok.py

'''
wattok
@author: Andrew Philpot
@version 0.2

Usage: python wattok.py <file>
'''

import sys
import nltk
import re
import util

VERSION = '0.2'
__version__ = VERSION
REVISION = "$Revision: 21934 $"

## TODO
## Break .. and ... ellipses off other tokens
## Detect sentence/abbrev end punctuation lacking space
##   eyes.My    places.Sexy 
## but not for some other special cases
##   gmail.com
## should / and __ be cases for breaking tokens?

class Tokenizer(object):
    def __init__(self, input):
        '''create Wattok'''        
        self.input = input

    def ensureText(self):
        if not getattr(self,"text",None):
            self.extractText()
        return self.text
    def extractText(self):
        try:
            self.text = util.slurpAsciiEntitified(self.input)
        except IOError:
            self.text = self.input
        return self.text
        
    entityRE = r"(?:&#\d{2,4};|&gt;|&lt;|&quot;|&apos;)"

    # need maximal segments of entityRE\s* 
    def genSegments(self, s):
        while len(s) > 0:
            m = re.search(r"\s*%s(?:\s*%s)*" % (Tokenizer.entityRE, Tokenizer.entityRE), s)
            if m:
                if m.start(0) == 0:
                    yield (True, m.group(0))
                    s = s[m.end(0):]
                else:
                    yield (False, s[0:m.start(0)])
                    yield (True, m.group(0))
                    s = s[m.end(0):]
            else:
                yield (False, s)
                s = ""

    def genTokens(self):
        text = self.ensureText()
        # set off w/space any entities that are butted up to preceding data
        text = re.sub(r'(?<!\s)(?P<entityref>%s)' % Tokenizer.entityRE, 
                      ' \g<entityref>',
                      text)
        # set off w/space any entities that are butted up to following data
        text = re.sub(r'(?P<entityref>%s)(?!\s)' % Tokenizer.entityRE,
                      '\g<entityref> ',
                      text)
        for (entities, segment) in self.genSegments(text):
            # print "SEGMENT: [%s %r]" % (entities,segment)
            segment = segment.strip()
            if entities:
                for entity in re.split(r'\s+',segment):
                    # print " ENTITY: [%s]" % entity;
                    yield entity
            else:
                sentences = nltk.sent_tokenize(segment)
                # correct for any embedded newlines (irrelevant?)
                sentences = [re.sub(r'[\n\t]+', ' ', sent).strip() for sent in sentences]
                # inexplicably, NLTK thinks big,red should be a single token
                sentences = [re.sub(r'\b,\b', ', ', sent) for sent in sentences]
                for sentence in sentences:
                    # print "  SENTENCE: [%s]" % sentence
                    for tok in nltk.word_tokenize(sentence):
                        # print "    TOK: [%s]" % tok
                        yield tok
        
def main(argv=None):
    '''this is called if run from command line'''

    n = Tokenizer((sys.argv and sys.argv[1]) or "/tmp/test.txt")
    # print n
    print [tok for tok in n.genTokens()]

# call main() if this is run as standalone
if __name__ == "__main__":
    sys.exit(main())

# End of wattok.py
