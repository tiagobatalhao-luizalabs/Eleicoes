# -*- coding: utf-8 -*-

import requests
import zipfile
import io
import os
import fnmatch
from datetime import datetime

def url_patterns():
    url_base = 'http://agencia.tse.jus.br/estatistica/sead/odsele/'
    urls_download = {
        'candidatos_lista': 'consulta_cand/consulta_cand_{ano}.zip',
        'candidatos_bens': 'bem_candidato/bem_candidato_{ano}.zip',
        'candidatos_coligacoes': 'consulta_coligacao/consulta_coligacao_{ano}.zip',
        'candidatos_vagas': 'consulta_vagas/consulta_vagas_{ano}.zip',
        'candidatos_cassacoes': 'motivo_cassacao/motivo_cassacao_{ano}.zip',
        'eleitorado_perfil': 'perfil_eleitorado/perfil_eleitorado_{ano}.zip',
        'eleitorado_deficiencia': 'perfil_eleitor_deficiente/perfil_eleitor_deficiencia_{ano}.zip',
        'eleitorado_secao': 'perfil_eleitor_secao/perfil_eleitor_secao_{ano}_{estado}.zip',
        'votacao_zona_nominal': 'votacao_candidato_munzona/votacao_candidato_munzona_{ano}.zip',
        'votacao_zona_partido': 'votacao_partido_munzona/votacao_partido_munzona_{ano}.zip',
        'votacao_zona_detalhe': 'detalhe_votacao_munzona/detalhe_votacao_munzona_{ano}.zip',
        'votacao_secao': 'votacao_secao/votacao_secao_{ano}_{estado}.zip',
        'votacao_secao_detalhe': 'detalhe_votacao_secao/detalhe_votacao_secao_{ano}.zip',
        'pesquisa_lista': 'pesquisa_eleitoral/pesquisa_eleitoral_{ano}.zip',
        'pesquisa_notasfiscais': 'pesquisa_eleitoral/nota_fiscal_{ano}.zip',
        'pesquisa_questionarios': 'pesquisa_eleitoral/questionario_pesquisa_{ano}.zip',
        'pesquisa_locais': 'pesquisa_eleitoral/bairro_municipio_{ano}.zip',
    }
    urls = {x: url_base + y for x,y in urls_download.items()}
    return urls

def list_zip_files(anos=None):
    """
    Lista possíveis zip files para download
    """
    if anos==None:
        anos = [2018 - 2*x for x in range(9)]
    estados = [
        'AP', 'AM', 'RR', 'PA', 'AP', 'RO', 'TO',
        'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA',
        'MG', 'ES', 'RJ', 'SP',
        'MT', 'MS', 'GO', 'DF',
        'PR', 'SC', 'RS',
    ]
    estados += ['BR', 'ZZ', 'VT']
    urls = {}
    for label, url in url_patterns().items():
        for ano in anos:
            if "{estado}" in url:
                for estado in estados:
                    params = {
                        'ano': ano,
                        'estado': estado,
                        'label': label,
                    }
                    label_new = '{label}_{ano}_{estado}'.format(**params)
                    url_new = url.format(**params)
                    urls[label_new] = url_new
            else:
                params = {
                    'ano': ano,
                    'label': label,
                }
                label_new = '{label}_{ano}'.format(**params)
                url_new = url.format(**params)
                urls[label_new] = url_new
    return urls


def download_file(base_folder, label, url):
    """
    Download and save file
    """
    req = requests.get(url)
    if req.ok:
        folder = os.path.join(base_folder, label.replace('_','/'))
        try:
            os.makedirs(folder)
        except OSError:
            pass
        os.chdir(folder)
        zipp = zipfile.ZipFile(io.BytesIO(req.content))
        zipp.extractall()

def download_files(folder_save, wildcard_pattern, download_secao=False):
    now = lambda : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    base_folder = os.path.abspath(folder_save)
    files = list_zip_files()
    labels = fnmatch.filter(files.keys(), wildcard_pattern)
    for label in labels:
        if (download_secao or '_secao' not in label):
            print('[{}] Downloading {} ...'.format(now(), label))
            download_file(base_folder, label, files[label])
            print('[{}] Downloaded {} .'.format(now(), label))

if __name__=='__main__':
    download_files('./Arquivos', 'candidatos_*_2018', False)
