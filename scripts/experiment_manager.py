class ExperimentParameter:

    def __init__(self, name, values):
        self.name = name
        self.values = values


parameters = [
    ExperimentParameter("table type", values=["NO_PK", "PK"]),
    ExperimentParameter("source file", values=["test", "test10"]),
    ExperimentParameter("threads", values=[1, 3, 5, 8]),
    ExperimentParameter("batch size", values=[10 ** 3, 10 ** 4, 10 ** 5, 10 ** 6]),
    ExperimentParameter("writer type", values=[
        "COPY_ASYNC",
        "COPY_SYNC",
        "COPY_NON_BATCH",
        "INSERTS_DEFAULT",
        "INSERTS_ADVANCED",
    ]),
]

count = 1

for param in parameters:
    count = count * len(param.values)

print(f"count = {count}")
