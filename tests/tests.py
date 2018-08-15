# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.
from bluesky.tests.conftest import RE
from ophyd.tests.conftest import hw
from bluesky.plans import count
import tempfile
from suitcase.csv import export


def test_export(RE, hw):
    collector = []

    def collect(name, doc):
        collector.append((name, doc))

    RE.subscribe(collect)
    RE(count([hw.det]))

    with tempfile.NamedTemporaryFile() as f:
        # We don't actually need f itself, just a filepath to template on.
        filepaths = export(collector, f.name)
        print(filepaths)
