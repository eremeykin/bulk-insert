import matplotlib.pyplot as plt
import os
import pandas

names = ["start", "end", "table_type", "source_file", "threads",
         "batch_size", "reader_type", "writer_type"]

rows = []
for subdir, dirs, files in os.walk("./data"):
    for file in files:
        if "java_out" in file:
            rows += pandas.read_csv(f"./data/{file}").to_dict(orient='records')


def label_experiment(row):
    table_type = row['table_type']
    threads = row['threads']
    batch_size = str(int(row['batch_size'] / 1000)) + "k"
    reader_type = row['reader_type']
    writer_type = row['writer_type']
    return f"{table_type:>6} - {reader_type:5} - {threads:2} - {batch_size:>6} - {writer_type:>17}"


elog = pandas.DataFrame(rows).rename(columns={'reader_type': 'source_file', 'source_file': 'threads',
                                              'threads': 'batch_size', 'batch_size': 'reader_type'})
# elog = elog[
#     (elog['reader_type'] == 'REAL') |
#     (False)
#     ]
# elog = elog[
#     (elog['table_type'] == 'PK') |
#     (False)
#     ]

# elog = elog[
#     (elog['threads'] == 8) |
#     (False)
#     ]

# elog = elog[
#     (elog['writer_type'] == 'INSERTS_ADVANCED') |
#     (False)
#     ]


elog["time"] = (elog["end"] - elog["start"]) / 1000
elog["label"] = elog.apply(label_experiment, axis=1)
elog = elog[["time", "label"]]
elog = elog.sort_values(by='time')
plt.rcParams["font.family"] = "monospace"
elog.plot.barh(
    figsize=(40, 60),
    y='time',
    x='label',
)
plt.grid()
plt.show()
