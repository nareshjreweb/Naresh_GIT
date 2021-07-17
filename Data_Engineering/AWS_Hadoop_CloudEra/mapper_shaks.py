import sys
import re

for line in sys.stdin:
    words = re.split("\W+",line)
    for word in words:
	word = word.lower()
	if word != '':
        	print("%s\t%d"%(word,1))
