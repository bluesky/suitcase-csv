# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.
from bluesky.plans import count
import json
import tempfile
from suitcase.csv import export
import pandas
import numpy as np


def generate_csv(fh, num_rows=1, num_columns=1, delimiter=','):
    """Writes a csv file to the given file handle.
    """
    df = pandas.DataFrame(np.random.randn(num_rows, num_columns))
    df.to_csv(path_or_buf=fh, sep=delimiter)


def test_export(RE, hw):
    collector = []

    def collect(name, doc):
        collector.append((name, doc))

    RE.subscribe(collect)
    RE(count([hw.det], 5))

    with tempfile.NamedTemporaryFile(mode='w') as f:
        # We don't actually need f itself, just a filepath to template on.
        meta, *csvs = export(collector, f.name)
    csv, = csvs

    docs = (doc for name, doc in collector)
    start, descriptor, *events, stop = docs
    with open(meta) as f:
        actual = json.load(f)
    expected = {'start': start, 'stop': stop, 'descriptors': [descriptor]}
    assert actual.keys() == expected.keys()
    table = pandas.read_csv(csv, header=None)
    assert actual['stop'] == expected['stop']
    assert table.shape == (5, 2)
