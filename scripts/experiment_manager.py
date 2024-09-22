import asyncio
import itertools
import os.path
import subprocess


class Parameter:

    def __init__(self, name, values):
        self.name = name
        self.values = values


class Setup:

    def __init__(self, table_type, reader_type, source_file, threads, batch_size, writer_type):
        self.table_type = table_type
        self.reader_type = reader_type
        self.source_file = source_file
        self.threads = threads
        self.batch_size = batch_size
        self.writer_type = writer_type

    def __str__(self):
        return str(self.__dict__)

    def file_prefix(self):
        return f"{self.table_type}_{self.reader_type}_{self.source_file}_{self.threads}_{self.batch_size}_{self.writer_type}"


def run(setup):
    cmd = [
        "/bin/bash",
        "-c",
        " ".join([
            "java",
            "-Dspring.shell.interactive.enabled=false",
            f"-Dbulkinsert.batch.load.chunk-size={setup.batch_size}",
            f"-Dbulkinsert.executor.max-pool-size={setup.threads}",
            "-jar",
            "../build/libs/bulk-insert-0.0.1-SNAPSHOT.jar",
            f"bl ../{setup.source_file} {setup.reader_type} {setup.writer_type} {setup.table_type}",
        ])
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    return result.stdout


def clean():
    cmd = [
        "/bin/bash",
        "-c",
        " ".join([
            "java",
            "-Dspring.shell.interactive.enabled=false",
            "-jar",
            "../build/libs/bulk-insert-0.0.1-SNAPSHOT.jar",
            "tr",
        ])
    ]
    subprocess.run(cmd, stdout=subprocess.PIPE)


parameters = [
    Parameter("table_type", values=[
        "NO_PK",
        "PK",
    ]),
    Parameter("reader_type", values=[
        "REAL",
        "FAKE",
    ]),
    Parameter("source_file", values=[
        # "test",
        "test10"
    ]),
    Parameter("threads", values=[
        1,
        3,
        5,
        8
    ]),
    Parameter("batch_size", values=[
        10 ** 3,
        10 ** 4,
        10 ** 5,
        10 ** 6
    ]),
    Parameter("writer_type", values=[
        "INSERTS_DEFAULT",
        "INSERTS_ADVANCED",
        "COPY_SYNC",
        "COPY_ASYNC",
        "COPY_NON_BATCH",
    ]),
]


async def run_experiment():
    for element in itertools.product(*[x.values for x in parameters]):
        setup = Setup(*element)
        java_out_file_name = f"./data/{setup.file_prefix()}_java_out.csv"
        if os.path.exists(java_out_file_name):
            continue
        clean()
        await asyncio.sleep(4)
        print(f"start: {setup}")
        monitor = await asyncio.create_subprocess_shell(
            f'/home/eremeykin/projects/personal/bulk-insert/venv/bin/python3 monitor.py ./data/{setup.file_prefix()}_monitor_log.csv',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await asyncio.sleep(4)
        sout = run(setup=setup)
        with open(java_out_file_name, 'w', newline='') as experiment_log:
            print("start,end,table_type,reader_type,source_file,threads,batch_size,writer_type", file=experiment_log)
            print(sout.decode(), end='', file=experiment_log)
            experiment_log.flush()
        await asyncio.sleep(8)
        monitor.terminate()
        subprocess.run(['/bin/bash', '-c', 'kill $(pgrep -f "python3 monitor.py")'], stdout=subprocess.PIPE)
        await monitor.wait()
        print("stop")

asyncio.run(run_experiment())
