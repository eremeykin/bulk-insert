import itertools
import subprocess
import time


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
    Parameter("table_type", values=["NO_PK", "PK"]),
    Parameter("reader_type", values=["REAL", "FAKE"]),
    Parameter("source_file", values=["test", "test10"]),
    Parameter("threads", values=[1, 3, 5, 8]),
    Parameter("batch_size", values=[10 ** 3, 10 ** 4, 10 ** 5, 10 ** 6]),
    Parameter("writer_type", values=[
        "COPY_ASYNC",
        "COPY_SYNC",
        "COPY_NON_BATCH",
        "INSERTS_DEFAULT",
        "INSERTS_ADVANCED",
    ]),
]

count = 1

with open('experiment_log.csv', 'w', newline='') as experiment_log:
    for element in itertools.product(*[x.values for x in parameters]):
        setup = Setup(*element)
        print(setup)
        clean()
        time.sleep(8)
        sout = run(setup=setup)
        print(sout.decode(), end='', file=experiment_log)
        experiment_log.flush()
        time.sleep(8)
