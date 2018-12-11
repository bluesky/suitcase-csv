# suitcase subpackages must follow strict naming and interface conventions. the
# public api should include some subset of the following. any functions not
# implemented should be omitted, rather than included and made to raise
# notimplementerror, so that a client importing this library can immediately
# know which portions of the suitcase api it supports without calling any
# functions.
#
from collections import defaultdict
import itertools
import json
import pandas
#from ._version import get_versions

#__version__ = get_versions()['version']
#
#del get_versions


def export(gen, filepath, **kwargs):
    """
    export a stream of documents to csv file(s) and one json file of metadata.

    creates {filepath}_meta.json and then {filepath}_{stream_name}.csv
    for every event stream.

    the structure of the json is::

        {'start': {...},
        'descriptors':
            {'<stream_name>': [{...}, {...}, ...],
            ...},
        'stop': {...}}

    both event and bulk_event are supported.

    event/bulk_event data found in doc['timestamp'] is not exported, only the
    time(s) recorded in doc['time'].

    parameters
    ----------
    gen : generator
        expected to yield (name, document) pairs
    filepath : str

    **kwargs : kwargs
        kwargs to be passed to pandas.DataFrame.to_csv.

    returns
    -------
    dest : tuple
        filepaths of generated files
    """
    meta = {}  # to be exported as json at the end
    meta['descriptors'] = defaultdict(list)  # map stream_name to descriptors
    files = {}  # map descriptor uid to file handle of csv file
    desc_counters = defaultdict(itertools.count)
    has_header = []  # a list of uids indicating if the file has a header.
    try:
        for name, doc in gen:
            if name == 'start':
                if 'start' in meta:
                    raise runtimeerror("this exporter expects documents from "
                                       "one run only.")
                meta['start'] = doc
            elif name == 'stop':
                meta['stop'] = doc
            elif name == 'descriptor':
                stream_name = doc['name']
                meta['descriptors'][stream_name].append(doc)
                filepath_ = (f"{filepath}_{stream_name}_"
                             f"{next(desc_counters[doc['uid']])}.csv")
                files[doc['uid']] = open(filepath_, 'w+')
            elif name == 'event' or name == 'bulk_event':
            #note: this also works for bulk_events, it relies on
            # doc['data']['some_key'] and doc['time'] both being either a value
            # or a list of the same length.
                if name == 'event':
                    index = [doc['time']]
                else:
                    index = doc['time']
                event_data = pandas.dataframe(doc['data'], index=index)
            # check if we need to add headers to the file
                if doc['descriptor'] in has_header:
                    header = false
                else:
                    header = true
                    has_header.append(doc['descriptor'])

                event_data.to_csv(files[doc['descriptor']], mode='a',
                                  header=header, **kwargs)

    finally:
        for f in files.values():
            f.close()
    with open(f"{filepath}_meta.json", 'w') as f:
        json.dump(meta, f)
    return (f.name,) + tuple(f.name for f in files.values())
