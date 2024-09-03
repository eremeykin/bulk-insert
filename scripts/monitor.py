import csv
import datetime
import psutil
import time


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


def get_cpu_core(cpu_index):
    def get_cpu(capture):
        return capture.cpu[cpu_index]

    return get_cpu


def get_mem_attr(attr):
    def get_mem(capture):
        return getattr(capture.mem, attr)

    return get_mem


def get_net_if_attr(nif, attr):
    def get_net(capture):
        return getattr(capture.net[nif], attr)

    return get_net


def is_good_attr(attr, obj):
    return not attr.startswith('_') and 'method' not in str(getattr(obj, attr))

base_capture = Capture()
up_if = {if_name for if_name, value in psutil.net_if_stats().items() if value.isup}
field_extractors = flatten([
    ("TIME", lambda capture: capture.captured_time.isoformat()),
    ("EPOCH_MILLIS", lambda capture: capture.epoch_millis),
    [
        (f"CPU_CORE_{i + 1}", get_cpu_core(i)) for i, cpu in enumerate(base_capture.cpu)
    ],
    [
        (f"MEM_{attr.upper()}", get_mem_attr(attr)) for attr in dir(base_capture.mem)
        if is_good_attr(attr, base_capture.mem)
    ],
    [
        [(f"NET_{nif.upper()}_{attr.upper()}", get_net_if_attr(nif, attr))
         for attr in dir(snetio) if is_good_attr(attr, snetio)]
        for nif, snetio in base_capture.net.items() if nif in up_if
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
