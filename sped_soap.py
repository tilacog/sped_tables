import asyncio
import xml.etree.ElementTree as ET
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


def persist_to_csv(data: dict) -> None:
    "for debugging purposes"
    import csv

    fieldnames = list(data[0].keys())

    with open('tables.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def make_filename(table_data: dict, sped_name: str) -> str:
    return (
        '{n}-{d[id]}-{d[versao]}-{s}.tbl'
        .format(n=sped_name, d=table_data, s=slugify(table_data['desc']))
    )


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


def download(to='.') -> None:
    tasks = []

    destination = Path(to)
    destination.mkdir(exist_ok=True)

    for sped_name in sped_names:
        tables, base_url = get_service_info(sped_name)
        for table in tables:
            final_url = make_url(base_url, table)
            filename = make_filename(table, sped_name)
            tasks.append(asyncio.ensure_future(
                fetch(final_url, (destination / filename).as_posix())
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
    download(to='downloaded_tables')
