"""
Dataflow to:
  1. fetch table references (metadata, urls, etc) into a json
  2. download table data into files
  3. transform and insert downloaded tables into a PostgreSQL database
"""
import json
import os
from contextlib import contextmanager
from pathlib import Path

import luigi
from psycopg2 import IntegrityError
from psycopg2.extras import Json
from psycopg2.pool import SimpleConnectionPool

from sped_soap import download, fetch_all_table_data, make_database_record

THIS_PATH = Path(__file__).parent.absolute()
DOWNLOADED_FILES_DIR = THIS_PATH / 'downloaded_tables'

pool = SimpleConnectionPool(minconn=1, maxconn=1,
                            dbname=os.environ['DB_NAME'],
                            user=os.environ['DB_USER'],
                            password=os.environ['DB_PASSWD'])


@contextmanager
def getconn():
    conn = pool.getconn()
    yield conn
    pool.putconn(conn)


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
        return luigi.LocalTarget(DOWNLOADED_FILES_DIR.as_posix())

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

    def output(self):
        "touched file (like a ticket) with database insertion result"
        dest_dir = THIS_PATH / 'insertion_results'
        dest_dir.mkdir(exist_ok=True)

        input_file = Path(self.table_metadata_file)
        basename, *_ = input_file.name.split('.', maxsplit=1)

        ticket = 'db-insert-results-for-%s' % basename
        return luigi.LocalTarget((dest_dir / ticket).as_posix())

    def mark_ticket(self, result: str):
        with self.output().open('w') as f:
            f.write(result)

    def run(self):
        with open(self.table_metadata_file) as fh:
            metadata = json.load(fh)

        # find local table file
        basename = metadata.pop('basename')
        table_file = DOWNLOADED_FILES_DIR / basename

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

        except FileNotFoundError:
            self.mark_ticket('FILE NOT FOUND')
            return

        self.insert(database_record)

    def insert(self, database_record: dict) -> bool:

        # adapt json objects
        for key in ['meta', 'data']:
            database_record[key] = Json(database_record[key])

        with getconn() as conn:
            try:
                conn.cursor().execute("""
                insert into public.external_data
                (document_type, name, meta, data)
                values
                (%(document_type)s, %(name)s, %(meta)s, %(data)s);
                """, database_record)
            except IntegrityError:
                conn.rollback()
                self.mark_ticket('FAILED')
            else:
                conn.commit()
                self.mark_ticket('SUCCES')


class UpdateDatabase(luigi.Task):
    "a wrapper task with dynamic requirements"

    def run(self):

        yield DownloadFiles()
        fetch_table_data_task = yield FetchTableData()
        table_data_dir = Path(fetch_table_data_task.path)

        dependent_tasks = [InsertIntoDatabase(table_metadata_file=x.as_posix())
                           for x in table_data_dir.glob('*.json')]
        yield dependent_tasks
