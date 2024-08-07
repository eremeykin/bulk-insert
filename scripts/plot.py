import matplotlib.pyplot as plt
import pandas

df = pandas.read_csv("monitor_log.csv")

nif = "WLP9S0"
shift_df = df.shift()
sent_bytes = df[f'NET_{nif}_BYTES_SENT']
sent_ms = df['EPOCH_MILLIS']
sent_bytes_shift = shift_df[f'NET_{nif}_BYTES_SENT']
sent_ms_shift = shift_df['EPOCH_MILLIS']

recv_bytes = df[f'NET_{nif}_BYTES_RECV']
recv_ms = df['EPOCH_MILLIS']
recv_bytes_shift = shift_df[f'NET_{nif}_BYTES_RECV']
recv_ms_shift = shift_df['EPOCH_MILLIS']

# millis / 10^3 = seconds
# bytes / 1000 / 1000 = megabyte (not mebibyte /1024/1024)
# megabytes * 8 = megabits
df[f'NET_{nif}_BYTES_SENT_DELTA'] = (((sent_bytes - sent_bytes_shift) / ((sent_ms - sent_ms_shift) / (10.0**3))) / 1000.0 / 1000.0).fillna(0) * 8
df[f'NET_{nif}_BYTES_RECV_DELTA'] = (((recv_bytes - recv_bytes_shift) / ((recv_ms - recv_ms_shift) / (10.0**3))) / 1000.0 / 1000.0).fillna(0) * 8

df.plot(x='EPOCH_MILLIS',
        y=[
            f"NET_{nif}_BYTES_SENT_DELTA",
            f"NET_{nif}_BYTES_RECV_DELTA",
        ])
plt.show()
