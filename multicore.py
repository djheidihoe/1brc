import mmap
import multiprocessing as mp
import os
import time

file = "data/measurements.txt"


def process_chunk(start: int, end: int, file_path: str):
    """Process a byte range of the file and return stats dict."""
    stats = {}  # city_bytes -> [min, max, sum, count]

    with open(file_path, "r+b") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        # move start to next newline if not at 0
        if start != 0:
            while mm[start] != 10:  # b'\n'
                start += 1
            start += 1

        # extend end to include the last full line
        file_size = mm.size()
        if end < file_size:
            while mm[end] != 10:
                end += 1

        pos = start
        while pos < end:
            line_end = mm.find(b"\n", pos, end)
            if line_end == -1:
                line_end = end
            line = mm[pos:line_end]

            sep = line.find(b";")
            if sep == -1:
                pos = line_end + 1
                continue  # skip malformed lines

            city = line[:sep]
            temp_bytes = line[sep + 1 :]

            # parse temperature in integer tenths (e.g., 12.3 -> 123)
            neg = temp_bytes[0] == 45  # b'-'
            i = 1 if neg else 0
            dot_index = temp_bytes.find(46, i)  # b'.'

            # support 1 or 2 digit whole numbers
            if dot_index - i == 2:
                tens = temp_bytes[i] - 48
                ones = temp_bytes[i + 1] - 48
            else:
                tens = 0
                ones = temp_bytes[i] - 48

            tenths = temp_bytes[dot_index + 1] - 48
            temp = tens * 100 + ones * 10 + tenths
            if neg:
                temp = -temp

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

            pos = line_end + 1

        mm.close()

    return stats


def merge_stats(all_stats):
    """Merge list of stats dicts into one."""
    final_stats = {}
    for s in all_stats:
        for city, stat in s.items():
            fs = final_stats.get(city)
            if fs:
                fs[0] = min(fs[0], stat[0])
                fs[1] = max(fs[1], stat[1])
                fs[2] += stat[2]
                fs[3] += stat[3]
            else:
                final_stats[city] = stat
    return final_stats


if __name__ == "__main__":
    t0 = time.perf_counter()

    file_size = os.path.getsize(file)
    num_workers = mp.cpu_count()
    chunk_size = file_size // num_workers

    chunks = []
    for i in range(num_workers):
        start = i * chunk_size
        end = file_size if i == num_workers - 1 else (i + 1) * chunk_size
        chunks.append((start, end, file))

    with mp.Pool(num_workers) as pool:
        results = pool.starmap(process_chunk, chunks)

    final_stats = merge_stats(results)

    t1 = time.perf_counter()
    print(f"Processed {file_size} bytes with {num_workers} cores in {t1 - t0:.3f} s")
    print("Example city stats (first 5):")
    for i, (city, stat) in enumerate(final_stats.items()):
        if i >= 5:
            break
        # decode for display only
        print(city.decode("utf-8"), stat)
