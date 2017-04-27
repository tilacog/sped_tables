"""
Dataflow to:
  1. fetch table references (metadata, urls, etc) into a json
  2. download table data into files
  3. transform and insert downloaded tables into a PostgreSQL database
"""
import json
from pathlib import Path

import luigi

from sped_soap import download, fetch_all_table_data, make_database_record

THIS_PATH = Path(__file__).parent.absolute()


class FetchTableData(luigi.Task):

    def output(self):
        "a directory"
        return luigi.LocalTarget(
            (THIS_PATH / 'downloaded_tables_metadata').as_posix()
        )

    def run(self):
        "will write one json file with metadata for each unit/table"

        destination = Path(self.output().path)
        destination.mkdir(exist_ok=True)

        tables = fetch_all_table_data()
        for table in tables:
            fname = (destination / (table['basename'] + '.json')).as_posix()
            with open(fname, 'w') as fh:
                json.dump(table, fh)


class DownloadFiles(luigi.Task):
    "Fetch files from a SOAP service."

    def requires(self):
        return FetchTableData()

    def parsed_input(self):

        directory = Path(self.input().path)
        files = directory.glob('*.json')

        data = []
        for f in files:
            with f.open() as fh:
                data.append(json.load(fh))

        return data

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

    def complete(self):
        "output directory must not be empty"
        dest_dir = Path(self.output().path)
        if not dest_dir.exists():
            return False
        return list(dest_dir.glob('*')) != []


class InsertIntoDatabase(luigi.Task):
    "Generate records from a single file and inserts them into a database"

    table_metadata_file = luigi.Parameter()
    remapped_sped_names = {
        'SpedFiscal': 'efd',
        'SpedPisCofins': 'efd_contribuicoes',
        'SpedContabil': 'ecd',
        'SpedEcf': 'ecf',
    }

    def requires(self):
        return DownloadFiles()

    def output(self):
        "touched file (like a ticket) with database insertion result"
        dest_dir = THIS_PATH / 'insertion_results'
        dest_dir.mkdir(exist_ok=True)

        input_file = Path(self.table_metadata_file)
        basename = input_file.name.split('.', maxsplit=1)

        ticket = 'db-insert-results-for-%s' % basename
        return luigi.LocalTarget((dest_dir / ticket).as_posix())

    def mark_ticket(self, result: str):
        with self.output().open('w') as f:
            f.write(result)

    def run(self):
        with open(self.table_metadata_file) as fh:
            metadata = json.load(fh)

        # find local table file
        source_dir = Path(self.input().path)
        basename = metadata.pop('basename')
        table_file = source_dir / basename

        # build database record
        sped_name = self.remapped_sped_names[metadata.pop('sped-name')]

        try:
            database_record = make_database_record(
                sped_name=sped_name,
                table_data=metadata,
                filepath=table_file.as_posix()
            )
        except RuntimeError:  # table is empty
            self.mark_ticket('EMPTY')
            return

        self.insert(database_record)

    def insert(self, database_record: dict) -> bool:
        # TODO
        pass


class UpdateDatabase(luigi.Task):

    def run(self):

        fetch_table_data_task = yield FetchTableData()
        table_data_dir = Path(fetch_table_data_task.path)

        dependent_tasks = [InsertIntoDatabase(table_metadata_file=x.as_posix())
                           for x in table_data_dir.glob('*.json')]
        yield dependent_tasks
