import pandas as pd

def read_next_chunk(gdrive_url, last_index, chunk_size=1000):
    skip = range(1, last_index + 1)
    df = pd.read_csv(gdrive_url, skiprows=skip, nrows=chunk_size)
    return df
