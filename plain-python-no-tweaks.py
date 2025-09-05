import time

t0 = time.perf_counter()
file = "data/measurements.txt"
with open(file) as inputfile:
    lines = inputfile.readlines()
    pass
t1 = time.perf_counter()

stats = {}  # minimum, max, sum

for line in lines:
    city, temp_str = line.split(";")
    temp = float(temp_str)
    if city in stats:
        # if min
        if temp < stats[city][0]:
            stats[city][0] = temp
        if temp > stats[city][1]:
            stats[city][1] = temp
        stats[city][2] += temp
        stats[city][3] += 1
    else:
        stats[city] = [temp, temp, temp, 1]  # minimum, max, sum


t2 = time.perf_counter()
print("Read:", t1 - t0, "s")
print("Parse and Aggreagate", t2 - t1, "s")
