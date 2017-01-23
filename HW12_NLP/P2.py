from nltk.book import *
from nltk.corpus import wordnet as wn

V = set(text4)
long_words = [w for w in V if len(w) > 7]
fdist = FreqDist(text4)
print sorted(long_words, key=lambda x: fdist[x], reverse=True)

print sorted([w for w in set(text4) if len(w) > 7 and fdist[w] > 85])

words = ['government', 'citizens', 'constitution', 'american', 'national', 'congress', 'interests', 'political', 'principles', 'progress']

for word in words:
	print "\n%s:" %word
	sum = 0
	for synset in wn.synsets(word):
		print synset.lemma_names()
		sum += len(synset.lemma_names())
	print "Total number of synonyms: %d" %sum    

for word in words:
	print "\n%s:" %word
	sum = 0
	for synset in wn.synsets(word):
		types = synset.hyponyms()
		names = [lemma.name() for synset in types for lemma in synset.lemmas()]
		print sorted(names)
		sum += len(names)
	print "Total number of hyponyms: %d" %sum    