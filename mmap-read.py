import time
import sys
import mmap

file = "data/measurements.txt"

t0 = time.perf_counter()
with open(file, "r+b") as inputfile:
    mm = mmap.mmap(inputfile.fileno(), 0, access=mmap.ACCESS_READ)
    lines = []
    for line in iter(mm.readline, b""):
        lines.append(line.rstrip(b"\n").decode("utf-8"))
    mm.close()

t1 = time.perf_counter()

stats = {}  # city -> [minimum, max, sum, count]


def parse_temp(s: str) -> int:
    """Parse temperature like '12.3' or '-7.8' into integer tenths."""
    neg = s[0] == "-"
    i = 1 if neg else 0
    # Find the decimal point
    dot_index = s.find(".", i)
    # Convert whole part and first decimal digit to integer tenths
    temp = int(s[i:dot_index]) * 10 + int(s[dot_index + 1])
    if neg:
        temp = -temp
    return temp


for line in lines:
    city, temp_str = line.split(";")
    city = sys.intern(city)  # Intern repeated city strings, should be faster
    # negative case
    temp = parse_temp(temp_str)
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
