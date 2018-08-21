# from ._version import get_versions
#
# __version__ = get_versions()['version']
# del get_versions

# Suitcase subpackages must follow strict naming and interface conventions. The
# public API should include some subset of the following. Any functions not
# implemented should be omitted, rather than included and made to raise
# NotImplementError, so that a client importing this library can immediately
# know which portions of the suitcase API it supports without calling any
# functions.
#
from collections import defaultdict
import itertools
import json
import pandas
from typing import Iterator, Tuple, List, Generator
import uuid
import time
import os


def export(gen: Iterator[Tuple[str, dict]], basename: str) -> List[str]:
    """
    Export a stream of documents to CSV file(s) and one JSON file of metadata.

    Creates {filepath}_meta.json and then {filepath}_{stream_name}.csv
    for every Event stream.

    The structure of the json is::

        {'start': {...},
        'descriptors':
            {'<stream_name>': [{...}, {...}, ...],
            ...},
        'stop': {...}}

    Both event and bulk_event are supported, though only independently.

    Parameters
    ----------
    gen : generator
        expected to yield (name, document) pairs
    filepath : str

    Returns
    -------
    dest : List[str]
        filepaths of generated files
    """
    meta = {}  # to be exported as JSON at the end
    meta['descriptors'] = defaultdict(list)  # map stream_name to descriptors
    files = {}  # map descriptor uid to file handle of CSV file
    desc_counters = defaultdict(itertools.count)
    try:
        streamdata = {}
        indices = []
        for name, doc in gen:
            if name == 'start':
                if 'start' in meta:
                    raise RuntimeError("This exporter expects documents from "
                                       "one run only.")
                meta['start'] = doc
            elif name == 'stop':
                meta['stop'] = doc
            elif name == 'descriptor':
                stream_name = doc.get('name')
                meta['descriptors'][stream_name].append(doc)
                # Open a file object for the stream
                filepath = f"{basename}_{stream_name}_{next(desc_counters[doc['uid']])}.csv"
                files[doc['uid']] = open(filepath, 'w+')
            elif name == 'event':
                for field in doc['data']:
                    if not doc['descriptor'] in streamdata: streamdata[doc['descriptor']] = {}
                    if not field in streamdata[doc['descriptor']]: streamdata[doc['descriptor']][field] = []
                    streamdata[doc['descriptor']][field] = streamdata[doc['descriptor']].get(field, []).append(
                        doc['data'][field])
                    indices.append(doc['time'])
            elif name == 'bulk_event':
                streamdata[doc['descriptor']] = list(doc['data'].values())[0]

        for uid in files:
            df = pandas.DataFrame(streamdata[uid], index=indices or None)
            df.to_csv(files[uid])

    finally:
        for f in files.values():
            f.close()
    with open(f"{basename}_meta.json", 'w') as f:
        json.dump(meta, f)
    return [f.name for f in files.values()]


def ingest(paths: Iterator[str], *args, **kwargs) -> Generator[Tuple[str, dict], None, None]:
    # Generate start doc
    startuuid = str(uuid.uuid4())
    start = {'time': time.time(),
             'uid': startuuid,
             }
    yield 'start', start

    for path in paths:
        df = pandas.read_csv(path, *args, **kwargs)

        # Generate descriptor doc
        descriptoruuid = str(uuid.uuid4())
        descriptor = {'data_keys':
                          {'data': {'source': 'file', 'dtype': 'array', 'shape': df.shape}, },
                      'time': time.time(),
                      'uid': descriptoruuid,
                      'start': startuuid,
                      }
        yield 'descriptor', descriptor

        # Generate bulk_event doc
        mtime = os.path.getmtime(path)
        bulk_event = {'data': {'data': df},
                      'timestamps': {'data': mtime, },
                      'time': mtime,
                      'uid': str(uuid.uuid4()),
                      'descriptor': descriptoruuid,
                      }

        yield 'bulk_event', bulk_event

    # Generate stop doc
    stop = {'exit_status': 'success',
            'time': time.time(),
            'uid': str(uuid.uuid4()),
            'start': startuuid,
            }
    yield 'stop', stop


if __name__ == "__main__":
    path = '/home/rp/data/xas/Fe_L_DB_29733.txt'
    skiprows = 15
    header = 0
    print(list(ingest([path], skiprows=skiprows, header=header, sep='\t')))
