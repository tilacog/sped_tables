"""
Dataflow to:
  1. download reference tables for Sped documents
  2. transform and insert downloaded tables into a PostgreSQL database
"""
import luigi
from pathlib import Path

from sped_soap import download

THIS_PATH = Path(__file__).parent.absolute()


class DownloadFiles(luigi.Task):
    """Fetch files from a SOAP service.
    """

    def run(self):
        "downloads lots of files asynchronously"

        # create destination directory
        dest_dir = Path(self.output().path)
        dest_dir.mkdir(exist_ok=True)

        # download files into directory
        download(to=dest_dir.as_posix())

    def output(self):
        "a directory"
        return luigi.LocalTarget((THIS_PATH / 'downloaded_tables').as_posix())

    def completed(self):
        "output directory must not be empty"
        dest_dir = Path(self.output().path)
        return dest_dir.glob('*') == []
