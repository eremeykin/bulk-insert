import matplotlib.pyplot as plt
import numpy as np
import os
import pandas
from matplotlib.ticker import FormatStrFormatter

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
    if writer_type == 'INSERTS_DEFAULT':
        return "INSERTs\n(reWriteBatchedInserts=false)"
    if writer_type == 'INSERTS_ADVANCED':
        return "INSERTs\n(reWriteBatchedInserts=true)"
    if writer_type == 'COPY_SYNC':
        return "CopyItemWriter\n(synchronous)"
    if writer_type == 'COPY_ASYNC':
        return "CopyItemWriter\n(asynchronous)"
    if writer_type == 'COPY_NON_BATCH':
        return "COPY\n(tasklet)"
    # return f"{table_type:>6} - {reader_type:5} - {threads:2} - {batch_size:>6} - {writer_type:>17}"


elog = pandas.DataFrame(rows).rename(columns={'reader_type': 'source_file', 'source_file': 'threads',
                                              'threads': 'batch_size', 'batch_size': 'reader_type'})
elog = elog[
    (elog['reader_type'] == 'REAL') |
    (False)
    ]

elog = elog[
    (elog['table_type'] == 'NO_PK') |
#     (elog['table_type'] == 'PK') |
    (False)
    ]

print(elog.info())

elog = elog[
    # (elog['threads'] == 1) |
    # (elog['threads'] == 3) |
    (elog['threads'] == 5) |
    # (elog['threads'] == 8) |
    (False)
    ]

elog = elog[
    # (elog['batch_size'] == 1000) |
    (elog['batch_size'] == 100_000) |
    (False)
    ]


elog["time"] = (elog["end"] - elog["start"]) / 1000
elog["label"] = elog.apply(label_experiment, axis=1)
elog = elog.sort_values(by='time')
elog = elog[["time", "label"]]
plt.rcParams["font.family"] = "monospace"
plt.rcParams["figure.autolayout"] = True

ax = elog.plot.barh(
    figsize=(10, 5),
    y='time',
    x='label',
    color='orange'
)

for bar in ax.patches:
    width = bar.get_width()
    ax.text(width / 2, bar.get_y() + bar.get_height()/2,
            f'{width:.0f}s',
            ha='center', va='center',
            fontsize=10, color='black')

for bar, label in zip(ax.patches, elog['label']):
    width = bar.get_width()
    ax.text(-15.8, bar.get_y() + bar.get_height()/2,
            f'{label}',
            ha='center', va='center',
            fontsize=10, color='black')


ax.set_ylabel('method', fontsize=14)
ax.set_xlabel('loading time, s', fontsize=14)
ax.get_legend().remove()
ax.set_axisbelow(True)
ax.tick_params(axis='y', colors='white')
ax.set_title('Total loading time for 5 threads, 100k items/chunk, 10M rows', fontsize=14)
plt.xticks(np.arange(0, 85.01, 5), minor=True)
plt.grid(which='minor', alpha=0.9)
plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%ds'))
plt.grid()
plt.show()
