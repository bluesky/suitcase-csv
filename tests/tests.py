# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.
from bluesky.plans import count
import tempfile
from suitcase.csv import export
from .utils import generate_csv
import hashlib
import json
import unittest
from typing import List
from fabio import edfimage
import numpy as np
from suitcase import csv
import os
import pandas


def test_export(RE, hw):
    collector = []

    def collect(name, doc):
        collector.append((name, doc))

    RE.subscribe(collect)
    RE(count([hw.det], 5))

    with tempfile.NamedTemporaryFile(mode='w') as f:
        # We don't actually need f itself, just a filepath to template on.
        generate_csv(f)
        filepaths = export(collector, f.name)
        print(filepaths)


suitcase = csv


class TestRheology(unittest.TestCase):

    def setUp(self):
        """Generate test files and headers"""
        csvpath = 'test.csv'
        self.header = [('start', {'time': 1534432676.1674924, 'uid': '4becc62f-8f0b-477f-a772-f1c66d6ca70b'}), (
            'descriptor',
            {'data_keys': {'image': {'source': 'file', 'dtype': 'array', 'shape': (128, 128)}},
             'time': 1534432676.1675735,
             'uid': 'eba6ce45-1f7c-457a-8c63-9d5db3374270', 'start': '4becc62f-8f0b-477f-a772-f1c66d6ca70b'}),
                       ('event', {
                           'data': {'data1': np.random.random(128).tolist()},
                           'timestamps': {'image': 1534432660.8167932},
                           'time': 1534432660.8167932, 'uid': '4bff5f16-8183-4bfe-8984-7558d263a259',
                           'descriptor': 'eba6ce45-1f7c-457a-8c63-9d5db3374270', 'EDF_DataBlockID': '0.Image.Psd',
                           'EDF_BinarySize': '131072', 'EDF_HeaderSize': '512', 'ByteOrder': 'LowByteFirst',
                           'DataType': 'DoubleValue',
                           'Dim_1': '128', 'Dim_2': '128', 'Image': '0', 'HeaderID': 'EH:000000:000000:000000',
                           'Size': '131072',
                           'test': '1'}), ('stop', {'exit_status': 'success', 'time': 1534432676.1710825,
                                                    'uid': '97cf57c1-eb4f-4263-af67-799ddeb10453',
                                                    'start': '4becc62f-8f0b-477f-a772-f1c66d6ca70b'})]

        pandas.DataFrame(np.random.random((128, 128))).to_csv(csvpath)

        self.paths = [csvpath]

    def tearDown(self):
        for path in self.paths:
            os.remove(path)

    def test_forward_rheology(self):
        """
        Translate native format to Event Model; check that reversing the translation gives back a file with the same
        checksum.

        """

        # Generate initial checksum
        pre_transform_checksum = hash_files(self.paths)

        # Transform forward/back
        args = [self.paths]
        kwargs = {}

        header = suitcase.ingest(*args, **kwargs)
        export_paths = suitcase.export(header, basename='test')

        # Generate final checksum
        post_transform_checksum = hash_files(export_paths)

        assert post_transform_checksum == pre_transform_checksum

    def test_reverse_rheology(self):
        """
        Translate from an Event Model header to a native format; check that reversing the translation gives back an
        Event Model header with the same checksum.

        """

        # Generate initial checksum
        pre_transform_checksum = hash_object(self.header)

        # Transform forward/back
        export_paths = suitcase.export(self.header, basename='test')
        args = [export_paths]
        kwargs = {}
        header = suitcase.ingest(*args, **kwargs)

        # Generate final checksum
        post_transform_checksum = hash_object(list(header))

        assert pre_transform_checksum == post_transform_checksum


def hash_files(paths: List[str]):
    """
    MD5 checksum a list of file paths
    """
    return map(hash_file, paths)


def hash_file(path: str):
    """
    MD5 checksum a file
    """
    return hashlib.md5(path).hexdigest()


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)


def hash_object(d: object) -> str:
    return hashlib.md5(json.dumps(d, sort_keys=True, cls=JSONEncoder).encode('utf-8')).hexdigest()


if __name__ == '__main__':
    unittest.main()
