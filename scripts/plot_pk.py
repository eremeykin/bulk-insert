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
    table_type = row['table_type'].replace("_", " ")
    threads = row['threads']
    batch_size = str(int(row['batch_size'] / 1000)) + "k"
    reader_type = row['reader_type']
    writer_type = row['writer_type']
    if writer_type == 'INSERTS_DEFAULT':
        writer_type = "INSERTs\n(reWriteBatchedInserts=false)"
    if writer_type == 'COPY_SYNC':
        writer_type = "CopyItemWriter\n(synchronous)"
    if writer_type == 'INSERTS_ADVANCED':
        writer_type = "INSERTs\n(reWriteBatchedInserts=true)"
    if writer_type == 'COPY_ASYNC':
        writer_type = "CopyItemWriter\n(asynchronous)"
    if writer_type == 'COPY_NON_BATCH':
        writer_type = "COPY\n(tasklet)"
    return f"{writer_type}"
    # return f"{table_type:>6} - {reader_type:5} - {threads:2} - {batch_size:>6} - {writer_type:>17}"


elog = pandas.DataFrame(rows).rename(columns={'reader_type': 'source_file', 'source_file': 'threads',
                                              'threads': 'batch_size', 'batch_size': 'reader_type'})
elog = elog[
    (elog['reader_type'] == 'REAL') |
    (False)
    ]

elog = elog[
    (elog['threads'] == 5) |
    (False)
    ]

elog = elog[
    (elog['batch_size'] == 100000) |
    (False)
    ]

elog["time"] = (elog["end"] - elog["start"]) / 1000
elog["label"] = elog.apply(label_experiment, axis=1)
elog["index"] = 0

elog.loc[elog['writer_type'] == 'INSERTS_DEFAULT', "index"] = 5
elog.loc[elog['writer_type'] == 'INSERTS_ADVANCED', "index"] = 4
elog.loc[elog['writer_type'] == 'COPY_SYNC', "index"] = 3
elog.loc[elog['writer_type'] == 'COPY_ASYNC', "index"] = 2
elog.loc[elog['writer_type'] == 'COPY_NON_BATCH', "index"] = 1

elog = elog.sort_values(by='index')

# elog = elog[["time", "label"]]
plt.rcParams["font.family"] = "monospace"
plt.rcParams["figure.autolayout"] = True

fig, ax = plt.subplots(figsize=(10, 5))

# Plotting bars
elog_pk = elog[(elog['table_type'] == 'PK')]
elog_no_pk = elog[(elog['table_type'] == 'NO_PK')]

bar_pk = ax.barh(elog_pk['label'], elog_pk['time'], 0.5, label="With PK", color="skyblue")
bar_no_pk = ax.barh(elog_no_pk['label'], elog_no_pk['time'], 0.5, label="Without PK", color="orange")



# ax = elog.plot.barh(
#     figsize=(10, 7),
#     y='time',
#     x='label',
#     color='orange'
# )
xmax = 85

rect_y_to_min_max = dict()

for rect in ax.patches:
    x, y = rect.xy
    current = rect_y_to_min_max.get(y)
    if current is None:
        rect_y_to_min_max[y] = (rect.get_width(), rect.get_width())
    else:
        rect_y_to_min_max[y] = (min(current[0], rect.get_width()), max(current[0], rect.get_width()))

# bar_add_pk = ax.barh(elog_no_pk['label'], [4 for x in elog_no_pk['time']], 0.5,
#                      left=[rect_y_to_min_max[y][0] for y in rect_y_to_min_max],
#                      linestyle='--',
#                      label="Add PK after loading (+4s)", color="none", edgecolor='gray', linewidth=1)
print(rect_y_to_min_max)

for bar in bar_pk:
    bar.set_linestyle((0, (3, 3)))

for bar in ax.patches:
    width = bar.get_width()
    x, y = bar.xy
    min_w, max_w = rect_y_to_min_max[y]
    if width == max_w:
        diff = (max_w - min_w)
        diff_p = (diff / min_w) * 100
        text_x = min_w + diff / 2
        ax.text(text_x, bar.get_y() + bar.get_height() / 2,
                f'+{diff_p:.0f}%',
                ha='center', va='center',
                fontsize=10, color='black',
                bbox=dict(facecolor='skyblue', edgecolor='skyblue', boxstyle='round,pad=0.1')
                )
        ax.text(max_w + 1, bar.get_y() + bar.get_height() / 2,
                f'{width:.0f}s',
                ha='left', va='center',
                fontsize=10, color='black',
                bbox=dict(facecolor='white', edgecolor='white', boxstyle='round,pad=0.1'))
    elif width == min_w:
        text_x = width / 2
        ax.text(text_x, bar.get_y() + bar.get_height() / 2,
                f'{width:.0f}s',
                ha='center', va='center',
                fontsize=10, color='black')

for bar, label in zip(ax.patches, elog_pk['label']):
    width = bar.get_width()
    ax.text(-16, bar.get_y() + bar.get_height() / 2,
            f'{label}',
            ha='center', va='center',
            fontsize=10, color='black')

ax.set_ylabel('method', fontsize=14)
ax.set_xlabel('loading time, s', fontsize=14)
ax.legend()
ax.set_axisbelow(True)
ax.tick_params(axis='y', colors='white')
ax.set_title('Total loading time for 5 threads, 100k items/chunk, 10M rows', fontsize=14)
plt.xticks(np.arange(0, xmax + 0.01, 5), minor=True)
plt.grid(which='minor', alpha=0.9)
plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%ds'))
plt.grid()
plt.show()
