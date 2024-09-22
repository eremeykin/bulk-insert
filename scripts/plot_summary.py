import matplotlib.pyplot as plt
import pandas

pg_nif = "BR-814FC035D114"


def parse_nif_name(column_name):
    start = column_name.find('NET_') + len('NET_')
    end = column_name.find('_', start)
    return column_name[start:end]


def calculate_nif_speed(df):
    nif_list = set([parse_nif_name(net_col) for net_col in df.columns if 'NET' in net_col])
    df_shift = df.shift()
    ms = df['EPOCH_MILLIS']
    ms_shift = df_shift['EPOCH_MILLIS']
    for nif in nif_list:
        for type in ["SENT", "RECV"]:
            bytes = df[f'NET_{nif}_BYTES_{type}']
            bytes_shift = df_shift[f'NET_{nif}_BYTES_{type}']
            # millis / 10^3 = seconds
            # bytes / 1000 / 1000 = megabyte (not mebibyte /1024/1024)
            # megabytes * 8 = megabits
            df[f'NET_{nif}_{type}_MEGABITS_PER_SEC'] = (
                    (((bytes - bytes_shift) / ((ms - ms_shift) / (10.0 ** 3))) / 1000.0 / 1000.0).fillna(0) * 8)


df = pandas.read_csv("monitor_log.csv")
calculate_nif_speed(df)
elog = pandas.read_csv("experiment_log.csv", header=None, names=["start", "end", "table_type", "source_file", "threads",
                                                                 "batch_size", "reader_type", "writer_type"])

for index, row in elog.iterrows():
    start, end, table_type, source_file, threads, batch_size, reader_type, writer_type = row
    start += 3*60*60*1000
    end += 3*60*60*1000 + 1000
    after_start = start < df['EPOCH_MILLIS']
    before_end = df['EPOCH_MILLIS'] < end
    cdf = df[after_start & before_end].copy()
    cdf['EPOCH_MILLIS'] = (cdf['EPOCH_MILLIS'] - start) / 1000
    ax = cdf.plot(x='EPOCH_MILLIS',
            y=[
                f"NET_{pg_nif}_SENT_MEGABITS_PER_SEC",
                f"NET_{pg_nif}_RECV_MEGABITS_PER_SEC",
            ])
    formatted_batch_size = "{:,}".format(batch_size)
    ax.set_title(f"{table_type}, {source_file}, {threads}, {formatted_batch_size}, {reader_type}, {writer_type}")
    # ax.set_xlabel("my x-label")
    # ax.set_ylabel("my y-label")
    plt.grid()
    plt.show()

exit(1)
