import time
import mmap

file = "data/measurements.txt"

stats = {}  # city_bytes -> [min, max, sum, count]

t0 = time.perf_counter()
with open(file, "r+b") as f:
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    t1 = time.perf_counter()  # end of "read"

    for line in iter(mm.readline, b""):
        # Find separator
        sep = line.find(b";")
        city = line[:sep]
        temp_bytes = line[sep + 1 : -1]  # assume line ends with b'\n'

        # Inline parse temp
        neg = temp_bytes[0] == 45  # b'-'
        i = 1 if neg else 0
        # Find decimal point
        dot_index = temp_bytes.find(46, i)  # b'.'
        # Convert to integer tenths
        tens = temp_bytes[i] - 48
        ones = temp_bytes[i + 1] - 48
        tenths = temp_bytes[dot_index + 1] - 48
        temp = tens * 100 + ones * 10 + tenths
        if neg:
            temp = -temp

        # Aggregate
        stat = stats.get(city)
        if stat:
            if temp < stat[0]:
                stat[0] = temp
            if temp > stat[1]:
                stat[1] = temp
            stat[2] += temp
            stat[3] += 1
        else:
            stats[city] = [temp, temp, temp, 1]

    mm.close()

t2 = time.perf_counter()
print("Read:", t1 - t0, "s")
print("Parse and Aggregate:", t2 - t1, "s")
