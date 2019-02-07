from . import export
import event_model
import numpy
import pandas


def create_expected(collector):
    '''collects the run data into a `pandas.dataframe` for comparison tests.'''
    streamnames = {}
    events_dict = {}
    expected = {}
    for name, doc in collector:
        if name == 'descriptor':
            streamnames[doc['uid']] = doc.get('name')
        elif name == 'event':
            streamname = streamnames[doc['descriptor']]
            if streamname not in events_dict.keys():
                events_dict[streamname] = []
            events_dict[streamname].append(doc)
        elif name == 'bulk_events':
            for key, events in doc.items():
                for event in events:
                    streamname = streamnames[event['descriptor']]
                    if streamname not in events_dict.keys():
                        events_dict[streamname] = []
                    events_dict[streamname].append(event)
        elif name == 'event_page':
            for event in event_model.unpack_event_page(doc):
                streamname = streamnames[event['descriptor']]
                if streamname not in events_dict.keys():
                    events_dict[streamname] = []
                events_dict[streamname].append(event)

    for streamname, event_list in events_dict.items():
        expected_dict = {}
        for event in event_list:
            for field in event['data']:
                if numpy.asarray(event['data'][field]).ndim in [1, 0]:
                    if 'seq_num' not in expected_dict.keys():
                        expected_dict['seq_num'] = []
                        expected_dict['time'] = []
                    if field not in expected_dict.keys():
                        expected_dict[field] = []
                    expected_dict[field].append(event['data'][field])
            if expected_dict:
                expected_dict['seq_num'].append(event['seq_num'])
                expected_dict['time'].append(event['time'])

        if expected_dict:
            expected[streamname] = pandas.DataFrame(expected_dict)
    return expected


def test_export(tmp_path, example_data):
    ''' runs a test using the `example_data` pytest.fixture.

    Runs a test using the `suitcase.utils.tests.conftest` fixture
    `example_data`.

    ..note::

        Due to the `example_data` `pytest.fixture` this will run multiple tests
        each with a range of detectors and a range of event_types. see
        `suitcase.utils.tests.conftest` for more info

    '''

    collector = example_data(ignore=[])
    expected_dict = create_expected(collector)
    artifacts = export(collector, tmp_path, file_prefix='')

    for filename in artifacts['stream_data']:
        streamname = str(filename).split('/')[-1].split('.')[0]

        actual = pandas.read_csv(filename)
        expected = expected_dict[streamname][list(actual.columns.values)]

        pandas.testing.assert_frame_equal(actual, expected)
