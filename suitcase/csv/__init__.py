import event_model
import numpy
import pandas
from pathlib import Path
import suitcase.utils
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def export(gen, directory, file_prefix='{start[uid]}-', **kwargs):
    """
    Export a stream of documents to a series of csv files.

    This creates a set of files named:
    ``<directory>/<file_prefix>{stream_name}.csv``
    for every Event stream and field that contains 1D 'tabular' data.

    .. warning::

        This process explicitly ignores all data that is not 1D and does not
        include any metadata in the output file.

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    gen : generator
        expected to yield ``(name, document)`` pairs

    directory : string, Path or Manager.
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.

        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        interface. See the suitcase documentation
        (http://nsls-ii.github.io/suitcase/) for details.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in
        ``{start[proposal_id]}-{start[sample_name]}-``,
        which are populated from the RunStart document. The default value is
        ``{start[uid]}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    **kwargs : kwargs
        kwargs to be passed to ``pandas.DataFrame.to_csv``.

    Returns
    -------
    artifacts : dict
        Maps 'labels' to lists of artifacts (e.g. filepaths)


    Examples
    --------

    Generate files with unique-identifier names in the current directory.

    >>> export(gen, '')

    Generate files with more readable metadata in the file names.

    >>> export(gen, '', '{start[plan_name]}-{start[motors]}-')

    Include the experiment's start time formatted as YYYY-MM-DD_HH-MM.

    >>> export(gen, '', '{start[time]:%Y-%m-%d_%H:%M}-')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    with Serializer(directory, file_prefix, **kwargs) as serializer:
        for item in gen:
            serializer(*item)

    return serializer.artifacts


class Serializer(event_model.DocumentRouter):
    """
    Serialize a stream of documents to a set of csvs.

    This creates a file named:
    ``<directory>/<file_prefix>{stream_name}.csv``
    for every Event stream that contains 1D 'tabular like' data.

    .. warning::

        This process explicitly ignores all data that is not 1D and does not
        include any metadata in the output file.


    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    directory : string, Path or Manager.
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.

        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        inferface. See the suitcase documentation
        (http://nsls-ii.github.io/suitcase/) for details.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in
        ``{start[proposal_id]}-{start[sample_name]}-``,
        which are populated from the RunStart document. The default value is
        ``{start[uid]}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    flush : boolean
        Flush the file to disk after each document. As a consequence, writing
        the full document stream is slower but each document is immediately
        available for reading. False by default.

    **kwargs : kwargs
        kwargs to be passed to ``pandas.Dataframe.to_csv``.

    Examples
    --------

    Generate files with unique-identifier names in the current directory.

    >>> export(gen, '')

    Generate files with more readable metadata in the file names.

    >>> export(gen, '', '{start[plan_name]}-{start[motors]}-')

    Include the experiment's start time formatted as YYYY-MM-DD_HH-MM.

    >>> export(gen, '', '{start[time]:%Y-%m-%d_%H:%M}-')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    def __init__(self, directory, file_prefix='{start[uid]}-', flush=False,
                 **kwargs):

        if isinstance(directory, (str, Path)):
            self._manager = suitcase.utils.MultiFileManager(directory)
        else:
            self._manager = directory

        self._streamnames = {}  # maps descriptor uids to stream_names
        self._files = {}  # maps stream_name to file
        self._file_prefix = file_prefix
        self._templated_file_prefix = ''
        self._start_found = False

        self._has_header = set()  # a set of uids to tell a file has a header

        kwargs.setdefault('header', True)
        self._initial_header_kwarg = kwargs['header']  # to set the headers
        kwargs.setdefault('index_label', 'time')
        kwargs.setdefault('mode', 'a')
        self._flush = flush
        self._kwargs = kwargs

    @property
    def artifacts(self):
        # The manager's artifacts attribute is itself a property, and we must
        # access it a new each time to be sure to get the latest content.
        return self._manager.artifacts

    def start(self, doc):
        '''Extracts `start` document information for formatting file_prefix.

        This method checks that only one `start` document is seen and formats
        `file_prefix` based on the contents of the `start` document.

        Parameters:
        -----------
        doc : dict
            RunStart document
        '''

        # raise an error if this is the second `start` document seen.
        if self._start_found:
            raise RuntimeError(
                "The serializer in suitcase.csv expects documents from one "
                "run only. Two `start` documents where sent to it")
        else:
            self._start_found = True

        # format self._file_prefix
        self._templated_file_prefix = self._file_prefix.format(start=doc)

    def descriptor(self, doc):
        '''Use `descriptor` doc to map stream_names to descriptor uid's.

        This method usess the descriptor document information to map the
        stream_names to descriptor uid's.

        Parameters:
        -----------
        doc : dict
            EventDescriptor document
        '''
        # extract some useful info from the doc
        streamname = doc.get('name')
        self._streamnames[doc['uid']] = streamname

    def event_page(self, doc):
        '''Add event page document information to a ".csv" file.

        This method adds event_page document information to a ".csv" file,
        creating it if nesecary.

        .. warning::

            All non 1D 'tabular' data is explicitly ignored.

        .. note::

            The data in Events might be structured as an Event, an EventPage,
            or a "bulk event" (deprecated). The DocumentRouter base class takes
            care of first transforming the other repsentations into an
            EventPage and then routing them through here, so no further action
            is required in this class. We can assume we will always receive an
            EventPage.

        Parameters:
        -----------
        doc : dict
            EventPage document
        '''
        event_model.verify_filled(doc)
        streamname = self._streamnames[doc['descriptor']]
        valid_data = {}
        for field in doc['data']:
            # check that the data is 1D, if not ignore it
            if numpy.asarray(doc['data'][field]).ndim == 1:
                # create a file for this stream and field if required
                if streamname not in self._files.keys():
                    filename = (f'{self._templated_file_prefix}'
                                f'{streamname}.csv')
                    f = self._manager.open('stream_data', filename, 'xt')
                    self._files[streamname] = f
                # add the valid data to the valid_data dict
                valid_data[field] = doc['data'][field]

        if valid_data:
            event_data = pandas.DataFrame(
                valid_data, index=doc[self._kwargs['index_label']])
            event_data['seq_num'] = doc['seq_num']

            if self._initial_header_kwarg:
                self._kwargs['header'] = streamname not in self._has_header

            file = self._files[streamname]
            event_data.to_csv(file, **self._kwargs)
            if self._flush:
                file.flush()
            self._has_header.add(streamname)

    def stop(self, doc):
        self.close()

    def close(self):
        '''Close all of the files opened by this Serializer.
        '''
        self._manager.close()

    def __enter__(self):
        return self

    def __exit__(self, *exception_details):
        self.close()
