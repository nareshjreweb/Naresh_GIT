import sys
for line in sys.stdin:
    line = line.strip()
    line = line.split(",")
    if line[0] == "ID":
        continue
    year = line[1]
    arrdel = line[15]
    depdel = line[16]
    if arrdel == "NA" or depdel == "NA":
        continue
    arrdel = int(arrdel)
    depdel = int(depdel)
    if arrdel+depdel>0:
        total_del = arrdel+depdel
        print("{}\t1\t{}".format(year, total_del))