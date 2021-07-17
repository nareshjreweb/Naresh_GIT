import sys

cur_word = ""
cur_count = 0
for line in sys.stdin:
    word, count = line.strip().split('\t')
    count = int(count)
    if word == cur_word:
        cur_count += count
    else:
        if cur_word != "":
            print("%s\t%d"%(cur_word,cur_count))
        cur_word = word
        cur_count = count
            
print("%s\t%d"%(cur_word,cur_count))

