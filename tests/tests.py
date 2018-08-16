# Tests should generate (and then clean up) any files they need for testing. No
# binary files should be included in the repository.
from bluesky.tests.conftest import RE
from ophyd.tests.conftest import hw
from bluesky.plans import count
import tempfile
from suitcase.csv import export
from .utils import generate_csv


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
