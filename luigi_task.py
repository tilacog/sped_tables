"""
Dataflow to:
  1. fetch table references (metadata, urls, etc) into a json
  2. download table data into files
  3. transform and insert downloaded tables into a PostgreSQL database
"""
import luigi
import json
from pathlib import Path

from sped_soap import download, fetch_all_table_data

THIS_PATH = Path(__file__).parent.absolute()


class FetchTableData(luigi.Task):

    def output(self):
        return luigi.LocalTarget('tables_to_download.json')

    def run(self):
        tables = fetch_all_table_data()
        with self.output().open('w') as fh:
            json.dump(tables, fh)


class DownloadFiles(luigi.Task):
    "Fetch files from a SOAP service."

    def requires(self):
        return FetchTableData()

    def parsed_input(self):
        with self.input().open() as fh:
            return json.load(fh)

    def run(self):
        "downloads lots of files asynchronously"

        # create destination directory
        dest_dir = Path(self.output().path)
        dest_dir.mkdir(exist_ok=True)

        # download files into directory
        download(
            tables=self.parsed_input(),
            to=dest_dir.as_posix()
        )

    def output(self):
        "a directory"
        return luigi.LocalTarget((THIS_PATH / 'downloaded_tables').as_posix())

    def completed(self):
        "output directory must not be empty"
        dest_dir = Path(self.output().path)
        return dest_dir.glob('*') == []


class InsertIntoDatabase(luigi.Task):
    # TODO
    pass
