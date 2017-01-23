import nltk
from nltk.book import *
from nltk.corpus import gutenberg

files = nltk.corpus.gutenberg.fileids()
print files

print "\nShow relative frequencies of words:"
cfd = nltk.ConditionalFreqDist(
	(fileid, word)
	for fileid in gutenberg.fileids()
	for word in gutenberg.words(fileid))
modals = ['can', 'could', 'may', 'might', 'will', 'would', 'should']
cfd.tabulate(conditions=files, samples=modals)

print "\nConvert text to lower case:"

cfd = nltk.ConditionalFreqDist(
	(fileid, word.lower())
	for fileid in gutenberg.fileids()
	for word in gutenberg.words(fileid))
cfd.tabulate(conditions=files, samples=modals)

print "\nShow the concordances of the modals:\n"
text1 = Text(gutenberg.words('bible-kjv.txt'))
text1.concordance("may")
text1.concordance("will")

print "\n"
text2 = Text(gutenberg.words('blake-poems.txt'))
text2.concordance("may")
text2.concordance("will")