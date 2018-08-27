# -*- coding: utf-8 -*-

import re
import pandas
import numpy as np
from datetime import datetime
import utils
import dateparser
import os
import csv

def print_log(message):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[{}]: {}'.format(now, message))

def limpa_string(s):
    if not isinstance(s, str):
        return ''
    elif s=='nan':
        return ''
    elif s.startswith('#n') or s.startswith('#N'):
        return ''
    else:
        return s.strip()


class Arquivo_votacao_nominal_zona():

    def ler(self, arquivo):

        expression = re.compile('20[0-9]{2}')
        ano = int(expression.search(arquivo).group())
        if ano < 2018:
            header = 0
        elif ano == 2018:
            header = 1
        dado = pandas.read_csv(
            arquivo,
            encoding='latin1',
            sep=';',
            header=header,
        )
        if ano <= 2012:
            rename = {
                0: 'Geracao_data',
                1: 'Geracao_hora',
                2: 'Eleicao_ano',
                3: 'Eleicao_turno',
                4: 'Eleicao_descricao',
                5: 'Regiao_UF',
                6: 'Regiao_UE',
                7: 'Local_municipio',
                8: 'Nome_municipio',
                9: 'Local_zona',
                10: 'Cargo',
                12: 'Candidato_id',
                28: 'Votos',
            }
            size = 29
        else:
            rename = {
                0: 'Geracao_data',
                1: 'Geracao_hora',
                2: 'Eleicao_ano',
                3: 'Eleicao_turno',
                4: 'Eleicao_descricao',
                5: 'Regiao_UF',
                6: 'Regiao_UE',
                7: 'Local_municipio',
                8: 'Nome_municipio',
                9: 'Local_zona',
                10: 'Cargo',
                12: 'Candidato_id',
                28: 'Votos',
            }
            size = 30
        dado.columns = [str(x) for x in range(size)]
        dado = dado.rename(columns={str(x):y for x,y in rename.items()})
        dado = dado[list(rename.values())]
        return dado

    def parsear(self, dado):
        print_log('Iniciando')
        saida = pandas.DataFrame()
        tabela_depara = {}

        def convert(row):
            day = row['Geracao_data'][0:2]
            month = row['Geracao_data'][3:5]
            year = row['Geracao_data'][6:10]
            return '{}-{}-{} {}'.format(year,month,day,row['Geracao_hora'])
        
        saida['Geracao'] = dado.apply(
            # lambda row: datetime.strptime(row['Geracao_data']+row['Geracao_hora'],'%d/%m/%Y%H:%M:%S'),
            lambda row: convert(row),
            axis=1,
        )
        print_log('Feito: geracao')

        saida['Eleicao_ano'] = dado['Eleicao_ano'].astype(int)
        print_log('Feito: Eleicao_ano')

        saida['Eleicao_turno'] = dado['Eleicao_turno'].astype(int)
        print_log('Feito: Eleicao_turno')

        saida['Eleicao_descricao'] = dado['Eleicao_descricao'].apply(convert_eleicao_descricao)
        print_log('Feito: Eleicao_descricao')

        saida['Regiao_UE'] = dado['Regiao_UE'].astype(str)
        print_log('Feito: Regiao_UE')

        tabela_depara['Municipio'], coluna = normalizar_municipio(dado)
        saida['Local_municipio'] = coluna
        print_log('Feito: Local_municipio')

        saida['Local_zona'] = dado['Local_zona'].astype(int)
        print_log('Feito: Local_zona')

        saida['Cargo'] = dado['Cargo'].astype(int)
        print_log('Feito: Cargo')

        saida['Candidato_id'] = dado['Candidato_id'].astype(int)
        print_log('Feito: Candidato_id')

        saida['Votos'] = dado['Votos'].astype(int)
        print_log('Feito: Votos')

        return saida, tabela_depara


tabela_eleicao = pandas.read_csv('Parseados/utils_Eleicao_descricao.csv')
tabela_eleicao['norm'] = tabela_eleicao['Valor'].apply(
    utils.normalize_names
)
eleicao_dict = {x: y for x,y in zip(tabela_eleicao['norm'],tabela_eleicao['Codigo'])}
def convert_eleicao_descricao(value):
    comp = utils.normalize_names(limpa_string(value))
    ret = eleicao_dict.get(comp, 0)
    if not ret:
        print('Problema: {}'.format(value))
    return ret

def normalizar_municipio(dado):
    tabela = pandas.DataFrame()
    for ano in dado['Eleicao_ano'].unique():
        this = dado[dado['Eleicao_ano']==ano]
        depara1 = utils.tabela_depara(
            this,
            'Local_municipio',
            'Nome_municipio'
        )
        depara2 = utils.tabela_depara(
            this,
            'Local_municipio',
            'Regiao_UF'
        )
        depara = pandas.DataFrame()
        depara['Eleicao_ano'] = [ano]*len(depara1)
        depara['Regiao_UE_codigo'] = depara1['Local_municipio']
        depara['Regiao_UE_descricao'] = depara1['value'].apply(limpa_string)
        depara['Regiao_UF_sigla'] = depara2['value'].apply(limpa_string)
        tabela = pandas.concat([tabela, depara])
    tabela.sort_values(by=['Eleicao_ano','Regiao_UE_codigo'], inplace=True)
    return tabela, dado['Local_municipio']


# Crawler
def all_files(directory):
    for path, dirs, files in os.walk(directory):
        for f in files:
            yield os.path.join(path, f)
def main():
    csv_params = {
        'sep':',',
        'encoding':'utf-8',
        'quoting':csv.QUOTE_NONNUMERIC,
    }
    arquivos = [x for x in all_files('./Arquivos') \
        if '/votacao/zona/nominal' in x \
        and (x.endswith('txt') or x.endswith('csv'))
    ]
    func = Arquivo_votacao_nominal_zona().ler
    def read(arq):
        try:
            return func(arq)
        except IndexError as e:
            print('Problema com {}: e'.format(arq, e))
    dfs = [read(x) for x in arquivos]
    dff = pandas.concat(dfs)
    saida = Arquivo_votacao_nominal_zona().parsear(dff)

    folder = './Parseados'
    try:
        os.makedirs(folder)
    except FileExistsError:
        pass
    for ano in saida[0]['Eleicao_ano'].unique():
        this = saida[0][saida[0]['Eleicao_ano']==ano]
        fname = os.path.join(folder, 'Votacao_zona_{}.csv'.format(ano))
        this.to_csv(fname, index=False, **csv_params)
    for label, val in saida[1].items():
        fname = os.path.join(folder, 'utils_{}.csv'.format(label))
        val.to_csv(fname, index=False, **csv_params)

    return saida

if __name__ == '__main__':
    main()
