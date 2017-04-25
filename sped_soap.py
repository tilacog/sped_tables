import asyncio
import xml.etree.ElementTree as ET

from zeep import Client

# available sped codes
codigos_sistema = {'SpedFiscal', 'SpedContribuicoes', 'SpedContabil', 'SpedECF'}

def get_table_data(sped_code):
    client = Client('http://www.sped.fazenda.gov.br/spedtabelas/WsConsulta/WsConsulta.asmx?WSDL')
    client_return = client.service.consultarVersoesTabelasExternas(codigoSistema=sped_code)

file_url = client_return['urlDownloadArquivo']

root = ET.fromstring(client_return['metadadosXml'])
table_data = [node.attrib for node in root.iter() if node.tag.endswith('tabela')]
