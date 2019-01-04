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
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def export(gen, filepath, **kwargs):
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
        the filepath and filename suffix to use in the output files.

    **kwargs : kwargs
        kwargs to be passed to pandas.Dataframe.to_csv.

    Returns
    -------
    dest : tuple
        filepaths of generated files
    """
    meta = {}  # to be exported as JSON at the end
    meta['descriptors'] = defaultdict(list)  # map stream_name to descriptors
    files = {}  # map descriptor uid to file handle of CSV file
    desc_counters = defaultdict(itertools.count)
    has_header = set()  # a set of uids indicating if the file has a header

    kwargs.setdefault('header', True)
    initial_header_kwarg = kwargs['header']  # used later to set the headers
    kwargs.setdefault('index_label', 'time')
    kwargs.setdefault('mode', 'a')

    try:
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
                filepath_ = (f"{filepath}_{stream_name}_"
                             f"{next(desc_counters[doc['uid']])}.csv")
                files[doc['uid']] = open(filepath_, 'w+')
            elif name == 'event' or name == 'bulk_event':
                # NOTE: this also works for bulk_events, it relies on
                # doc['data']['some_key'] and doc['time'] both being either a
                # value or a list of the same length.
                if name == 'event':
                    index = [doc['time']]
                else:
                    index = doc['time']
                event_data = pandas.DataFrame(doc['data'], index=index)

                if initial_header_kwarg:
                    kwargs['header'] = doc['descriptor'] not in has_header

                event_data.to_csv(files[doc['descriptor']], **kwargs)
                has_header.add(doc['descriptor'])

    finally:
        for f in files.values():
            f.close()
    with open(f"{filepath}_meta.json", 'w') as f:
        json.dump(meta, f)
    return (f.name,) + tuple(f.name for f in files.values())
