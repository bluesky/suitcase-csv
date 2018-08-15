import pandas
import numpy as np

def generate_csv(fh, num_rows=1, num_columns=1, delimiter=','):
    """Writes a csv file to the given file handle.
    """
    df = pandas.DataFrame(np.random.randn(num_rows, num_columns))
    df.to_csv(path_or_buf=fh, sep=delimiter)
