import asyncio
import aiohttp
import xml.etree.ElementTree as ET

from zeep import Client
from slugify import slugify


# available sped codes
sped_names = ('SpedFiscal', 'SpedPisCofins', 'SpedContabil', 'SpedEcf')


def get_service_info(sped_code):
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


def persist_to_csv(data):
    "for debugging purposes"
    import csv

    fieldnames = list(data[0].keys())

    with open('tables.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def make_filename(table_data, sped_name):
    return (
        '{n}-{d[id]}-{d[versao]}-{s}.tbl'
        .format(n=sped_name, d=table_data, s=slugify(table_data['desc']))
    )


def make_url(base, data):
    from urllib.parse import urlencode
    q = {'idTabela': data['id'], 'versao': data['versao']}
    return base + '?' + urlencode(q)


async def fetch(url, local_fname):
    resp = await aiohttp.get(url)
    with open(local_fname, 'wb') as f_handle:
        while True:
            chunk = await resp.content.read(1024)
            if not chunk:
                break
            f_handle.write(chunk)
    return await resp.release()

if __name__ == '__main__':

    tasks = []
    for sped_name in sped_names:
        tables, base_url = get_service_info(sped_name)
        for table in tables:
            final_url = make_url(base_url, table)
            filename = make_filename(table, sped_name)
            tasks.append(asyncio.ensure_future(fetch(final_url, filename)))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
