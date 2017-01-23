from collections import Counter
from sets import Set 
import re
        
filename = '/Users/hqiu/Documents/Virtual Machines.localized/VM_shared/part-r-00000'

with open(filename) as f:
    lines = f.readlines()

stopwords = Set(['the', 'a', 'an', 'and', 'or', 'of', 'to',
			'about', 'above', 'after', 'all', 'are', 'be', 'but', 'he', 'she', 'by', 'can\'t', 'for', 'do', 'i',
			'has', 'don\'t', 'her', 'is', 'in', 'our', 'his', 'with', 'that', 'you', 'it', 'was', 'on', 'him'])

cnt = Counter()

for line in lines:
	pairs = line.split('\t')
	words = re.match(r"[a-zA-Z]+", pairs[0])
	if words is None:
		continue

	word = words.group(0).lower()
	if word in stopwords:
		continue
	num = pairs[1]

	cnt[word] += int(num)

print cnt.most_common(200)

