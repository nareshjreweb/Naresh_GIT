import sys
flight = {}
flight_count, del_sum = 0, 0
for line in sys.stdin:
line = line.strip()
year, count, delay = line.split("\t")
count, delay = int(count), int(delay)
if year in flight:
flight[year][1] += delay
flight[year][0] += count
else:
flight[year] = []
flight[year].append(count)
flight[year].append(delay)
for year in flight.keys():
average = float(flight[year][1])/float(flight[year][0])
print("{}\t{}".format(year, average))