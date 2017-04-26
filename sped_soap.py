import asyncio
import xml.etree.ElementTree as ET
from functools import lru_cache
from itertools import count
from pathlib import Path

import aiohttp
from slugify import slugify
from zeep import Client

# available sped codes
sped_names = ('SpedFiscal', 'SpedPisCofins', 'SpedContabil', 'SpedEcf')


def get_service_info(sped_code: str) -> (list, str):
    client = Client('http://www.sped.fazenda.gov.br'
                    '/spedtabelas/WsConsulta/WsConsulta.asmx?WSDL')

    client_return = (
        client.service
        .consultarVersoesTabelasExternas(codigoSistema=sped_code)
    )

    # base endpoint for later downloading individual files
    base_url = client_return['urlDownloadArquivo']

    # data about individual files
    xml_table_data = ET.fromstring(client_return['metadadosXml'])
    table_data = [node.attrib for node in xml_table_data.iter()
                  if node.tag.endswith('tabela')]

    return (table_data, base_url)


@lru_cache()
def fetch_all_table_data() -> list:
    "streamlines table data while inserting extra data into units"
    results = []
    for sped_name in sped_names:
        tables, base_url = get_service_info(sped_name)
        for table in tables:
            table['url'] = make_url(base_url, table)
            table['slug'] = slugify(table['desc'])
            table['basename'] = ('{n}-{d[id]}-{d[versao]}-{d[slug]}.tbl'
                                .format(n=sped_name.lower(), d=table))
            results.append(table)
    return results


def make_url(base: str, data: dict) -> str:
    from urllib.parse import urlencode
    q = {'idTabela': data['id'], 'versao': data['versao']}
    return base + '?' + urlencode(q)


async def fetch(url: str, local_fname: str):
    resp = await aiohttp.get(url)
    with open(local_fname, 'wb') as f_handle:
        while True:
            chunk = await resp.content.read(1024)
            if not chunk:
                break
            f_handle.write(chunk)
    return await resp.release()


def download(tables: list, to='.') -> None:

    destination = Path(to)
    destination.mkdir(exist_ok=True)

    tasks = []
    for table in tables:
        fname = (destination / table['basename']).as_posix()
        tasks.append(asyncio.ensure_future(
            fetch(url=table['url'], local_fname=fname)
        ))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))


def generate_records_from_file(bytestream: bytes) -> dict:
    "generates records (dicts) from a given fileinput (in bytes)"
    text = bytestream.decode('latin1')
    first_line, *content = [line.strip() for line in text.splitlines()]

    if not content:
        raise RuntimeError('empty table')

    _, *headers_string = first_line.split(maxsplit=1)

    # use callables(lambdas) to handle either present or missing headers
    headers = (
        (lambda: headers_string[0].split())
        if headers_string
        else lambda: ('field-{:0>2}'.format(i) for i in count(start=1))
    )

    # build records and yield them
    for line in content:
        values = line.split('|')
        yield dict(zip(headers(), values))


def generate_database_records(sped_name: str, table_data: dict,
                              filepath: str) -> dict:
    "returns a database record (dict) ready for insertion into a database"
    with open(filepath, 'rb') as fh:
        return {
            'document_type': sped_name,
            'name': (slugify(table_data['desc'])
                     or 'table-%s' % (table_data['id'],)),
            'meta': table_data,
            'data': list(generate_records_from_file(fh))
        }


if __name__ == '__main__':
    download(tables=fetch_all_table_data(),
             to='downloaded_tables')
