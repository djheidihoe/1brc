import time
import sys
import mmap

file = "data/measurements.txt"

stats = {}  # city -> [minimum, max, sum, count]


def parse_temp_bytes(b: bytes) -> int:
    """Parse dd.d or -dd.d directly from bytes"""
    neg = b[0] == 45
    i = 1 if neg else 0
    # Find the decimal point
    dot_index = b.find(46, i)  # ASCII '.' is 46
    # Convert ASCII bytes to integers
    tens = b[i] - 48  # '0' is 48
    ones = b[i + 1] - 48
    tenths = b[dot_index + 1] - 48
    temp = tens * 100 + ones * 10 + tenths
    if neg:
        temp = -temp
    return temp


t0 = time.perf_counter()
with open(file, "r+b") as inputfile:
    mm = mmap.mmap(inputfile.fileno(), 0, access=mmap.ACCESS_READ)
    t1 = time.perf_counter()  # end of "read"

    for line in iter(mm.readline, b""):
        line = line.rstrip(b"\n")
        city_bytes, temp_bytes = line.split(b";")
        # city = sys.intern(city_bytes)  # Intern repeated city strings, should be faster
        temp = parse_temp_bytes(temp_bytes)

        stat = stats.get(city_bytes)
        if stat:
            # stat[0] = min(stat[0], temp)
            if temp < stat[0]:
                stat[0] = temp
            # stat[1] = max(stat[1], temp)
            if temp > stat[1]:
                stat[1] = temp
            stat[2] += temp
            stat[3] += 1
        else:
            stats[city_bytes] = [temp, temp, temp, 1]  # minimum, max, sum
    mm.close()


t2 = time.perf_counter()
print("Read:", t1 - t0, "s")
print("Parse and Aggreagate", t2 - t1, "s")
