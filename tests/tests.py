# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.
from bluesky.plans import count
import json
import tempfile
from suitcase.csv import export
import pandas
from pandas.util.testing import assert_frame_equal
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

    expected = {}
    expected_dict = {'data': {'det': []}, 'time': []}
    for event in events:
        expected_dict['data']['det'].append(event['data']['det'])
        expected_dict['time'].append(event['time'])

    expected['events'] = pandas.DataFrame(expected_dict['data'],
                                          index=expected_dict['time'])
    expected['events'].index.name = 'time'

    with open(meta) as f:
        actual = json.load(f)
    # This next section is used to convert lists to tuples for the assert below
    for dims in actual['start']['hints']['dimensions']:
        new_dims = []
        for dim in dims:
            if type(dim) is list:
                new_dims.append(tuple(dim))
            else:
                new_dims.append(dim)
        actual['start']['hints']['dimensions'] = [tuple(new_dims)]

    expected.update({'start': start, 'stop': stop,
                     'descriptors': {'primary': [descriptor]}})
    actual['events'] = pandas.read_csv(csv, index_col=0)
    assert actual.keys() == expected.keys()
    assert actual['start'] == expected['start']
    assert actual['descriptors'] == expected['descriptors']
    assert actual['stop'] == expected['stop']
    assert_frame_equal(expected['events'], actual['events'])
