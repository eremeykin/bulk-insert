import csv
import datetime
import time

import psutil


def float_timestamp_to_millis(timestamp):
    return int(timestamp * (10 ** 3))


def flatten(lst):
    if not lst:
        return lst
    if isinstance(lst[0], list):
        return flatten(lst[0]) + flatten(lst[1:])
    return lst[:1] + flatten(lst[1:])


class Capture:
    def __init__(self):
        self.captured_time = datetime.datetime.now()
        timestamp = self.captured_time.replace(tzinfo=datetime.timezone.utc).timestamp()
        self.epoch_millis = float_timestamp_to_millis(timestamp)
        self.cpu = psutil.cpu_percent(percpu=True)
        self.mem = psutil.virtual_memory()
        self.net = psutil.net_io_counters(pernic=True)


base_capture = Capture()
up_if = {if_name for if_name, value in psutil.net_if_stats().items() if value.isup}
field_extractors = flatten([
    ("TIME", lambda capture: capture.captured_time.isoformat()),
    ("EPOCH_MILLIS", lambda capture: capture.epoch_millis),
    [
        (f"CPU_CORE_{i + 1}", lambda capture: capture.cpu[i]) for i, cpu in enumerate(base_capture.cpu)
    ],
    [
        (f"MEM_{attr.upper()}", lambda capture: getattr(capture.mem, attr)) for attr in dir(base_capture.mem) if
        not attr.startswith('_')
    ],
    [
        [(f"NET_{nif.upper()}_{attr.upper()}", lambda capture: getattr(capture.net[nif], attr)) for attr in dir(snetio)
         if not attr.startswith('_')] for nif, snetio in base_capture.net.items() if nif in up_if
    ]
])
start_time = time.monotonic()
interval = 0.1

with open('monitor_log.csv', 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=[field_name for field_name, func in field_extractors])
    writer.writeheader()
    while True:
        c = Capture()
        time.sleep(interval - ((time.monotonic() - start_time) % interval))
        writer.writerow({
            field_name: func(c) for field_name, func in field_extractors
        })
        csv_file.flush()
