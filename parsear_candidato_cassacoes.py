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


class Arquivo_candidatos_cassacoes():

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
        if ano < 2018:
            colunas = [
                "DATA_GERACAO",
                "HORA_GERACAO",
                "unknown",
                "DESCRICAO_ELEICAO",
                "ANO_ELEICAO",
                "SIGLA_UF",
                "SIGLA_UE",
                "SQ_CANDIDATO",
                "MOTIVO",
            ]
            rename = {
                "DATA_GERACAO": 'Geracao_data',
                "HORA_GERACAO": 'Geracao_hora',
                "ANO_ELEICAO": 'Eleicao_ano',
                "DESCRICAO_ELEICAO": 'Eleicao_descricao',
                "SIGLA_UE": 'Regiao_UE',
                "SQ_CANDIDATO": 'Candidato_id',
                "MOTIVO": 'Motivo'
            }
        elif ano == 2018:
            colunas = [
                "DT_GERACAO",
                "HH_GERACAO",
                "ANO_ELEICAO",
                "CD_TIPO_ELEICAO",
                "NM_TIPO_ELEICAO",
                "CD_ELEICAO",
                "DS_ELEICAO",
                "SG_UF",
                "SG_UE",
                "NM_UE",
                "SQ_CANDIDATO",
                "DS_MOTIVO_CASSACAO",
            ]
            rename = {
                "DT_GERACAO": 'Geracao_data',
                "HH_GERACAO": 'Geracao_hora',
                "ANO_ELEICAO": 'Eleicao_ano',
                "DS_ELEICAO": 'Eleicao_descricao',
                "SG_UE": 'Regiao_UE',
                "SQ_CANDIDATO": 'Candidato_id',
                "DS_MOTIVO_CASSACAO": 'Motivo'
            }

        dado.columns = colunas
        dado = dado.rename(columns=rename)
        dado = dado[list(rename.values())]
        return dado

    def parsear(self, dado):
        print_log('Iniciando')
        saida = pandas.DataFrame()
        tabela_depara = {}

        saida['Geracao'] = dado.apply(
            lambda row: datetime.strptime(row['Geracao_data']+row['Geracao_hora'],'%d/%m/%Y%H:%M:%S'),
            axis=1,
        )
        print_log('Feito: geracao')

        saida['Eleicao_ano'] = dado['Eleicao_ano'].astype(int)
        print_log('Feito: Eleicao_ano')

        saida['Eleicao_descricao'] = dado['Eleicao_descricao'].apply(convert_eleicao_descricao)
        print_log('Feito: Eleicao_descricao')

        saida['Regiao_UE'] = dado['Regiao_UE'].astype(str)
        print_log('Feito: Regiao_UE')

        saida['Candidato_id'] = dado['Candidato_id'].astype(str)
        print_log('Feito: Candidato_id')

        tabela_depara['Motivo_cassacao'], coluna = normalizar_cassacao(dado)
        saida['Motivo_cassacao'] = coluna
        print_log('Feito: Motivo_cassacao')

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

def normalizar_cassacao(dado):
    tabela = pandas.DataFrame()
    colunas = []
    for ano in dado['Eleicao_ano'].unique():
        index = dado['Eleicao_ano']==ano
        this = dado[index]
        ls = sorted(this['Motivo'].apply(lambda x:x.strip()).unique())
        dic = {x: i+1 for i,x in enumerate(ls)}
        depara = pandas.DataFrame()
        depara['Eleicao_ano'] = [ano]*len(dic)
        depara['Codigo'] = range(1,1+len(ls))
        depara['Valor'] = ls
        tabela = pandas.concat([tabela, depara])
        colunas.append(this['Motivo'].apply(
            lambda x: dic.get(x.strip(), -2)
        ))
    coluna = pandas.concat(colunas)
    return tabela, coluna

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
        if '/candidatos/cassacoes' in x \
        and (x.endswith('txt') or x.endswith('csv'))
    ]
    func = Arquivo_candidatos_cassacoes().ler
    def read(arq):
        try:
            return func(arq)
        except:
            print('Problema com {}'.format(arq))
    dfs = [read(x) for x in arquivos]
    dff = pandas.concat(dfs)
    saida = Arquivo_candidatos_cassacoes().parsear(dff)

    folder = './Parseados'
    try:
        os.makedirs(folder)
    except FileExistsError:
        pass
    for ano in saida[0]['Eleicao_ano'].unique():
        this = saida[0][saida[0]['Eleicao_ano']==ano]
        fname = os.path.join(folder, 'Cassacoes_{}.csv'.format(ano))
        this.to_csv(fname, index=False, **csv_params)
    for label, val in saida[1].items():
        fname = os.path.join(folder, 'utils_{}.csv'.format(label))
        val.to_csv(fname, index=False, **csv_params)

    return saida

if __name__ == '__main__':
    main()
