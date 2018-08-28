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


class Arquivo_candidatos_bens():

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
                "ANO_ELEICAO",
                "DESCRICAO_ELEICAO",
                "SIGLA_UF",
                "SQ_CANDIDATO",
                "CD_TIPO_BEM_CANDIDATO",
                "DS_TIPO_BEM_CANDIDATO",
                "DETALHE_BEM",
                "VALOR_BEM",
                "DATA_ULTIMA_ATUALIZACAO",
                "HORA_ULTIMA_ATUALIZACAO"
            ]
            rename = {
                "DATA_GERACAO": 'Geracao_data',
                "HORA_GERACAO": 'Geracao_hora',
                "DATA_ULTIMA_ATUALIZACAO": 'Atualizacao_data',
                "HORA_ULTIMA_ATUALIZACAO": 'Atualizacao_hora',
                "ANO_ELEICAO": 'Eleicao_ano',
                "DESCRICAO_ELEICAO": 'Eleicao_descricao',
                "SQ_CANDIDATO": 'Candidato_id',
                "CD_TIPO_BEM_CANDIDATO": 'Bem_codigo',
                "DS_TIPO_BEM_CANDIDATO": 'Bem_descricao',
                "DETALHE_BEM": 'Bem_detalhe',
                "VALOR_BEM": 'Bem_valor',
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
                "DT_ELEICAO",
                "SG_UF",
                "SG_UE",
                "NM_UE",
                "SQ_CANDIDATO",
                "NR_ORDEM_CANDIDATO",
                "CD_TIPO_BEM_CANDIDATO",
                "DS_TIPO_BEM_CANDIDATO",
                "DS_BEM_CANDIDATO",
                "VR_BEM_CANDIDATO",
                "DT_ULTIMA_ATUALIZACAO",
                "HH_ULTIMA_ATUALIZACAO"
            ]
            rename = {
                "DT_GERACAO": 'Geracao_data',
                "HH_GERACAO": 'Geracao_hora',
                "DT_ULTIMA_ATUALIZACAO": 'Atualizacao_data',
                "HH_ULTIMA_ATUALIZACAO": 'Atualizacao_hora',
                "ANO_ELEICAO": 'Eleicao_ano',
                "DS_ELEICAO": 'Eleicao_descricao',
                "SQ_CANDIDATO": 'Candidato_id',
                "CD_TIPO_BEM_CANDIDATO": 'Bem_codigo',
                "DS_TIPO_BEM_CANDIDATO": 'Bem_descricao',
                "DS_BEM_CANDIDATO": 'Bem_detalhe',
                "VR_BEM_CANDIDATO": 'Bem_valor',
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

        def func(row):
            try:
                ret = datetime.strptime(row['Atualizacao_data']+row['Atualizacao_hora'],'%d/%m/%Y%H:%M:%S'),
            except ValueError:
                ret = (None,)
            return ret
        saida['Atualizacao'] = dado.apply(lambda x: func(x)[0], axis=1)
        print_log('Feito: atualizacao')

        saida['Eleicao_ano'] = dado['Eleicao_ano'].astype(int)
        print_log('Feito: Eleicao_ano')

        saida['Eleicao_descricao'] = dado['Eleicao_descricao'].apply(convert_eleicao_descricao)
        print_log('Feito: Eleicao_descricao')

        saida['Candidato_id'] = dado['Candidato_id'].astype(int)
        print_log('Feito: Candidato_id')

        tabela_depara['Bem_codigo'], coluna = normalizar_bem_tipo(dado)
        saida['Bem_codigo'] = coluna
        print_log('Feito: Bem_codigo')

        saida['Bem_detalhe'] = dado['Bem_detalhe'].apply(limpa_string).astype(str)
        print_log('Feito: Bem_detalhe')

        def func(value):
            try:
                ret = float(str(value).strip().replace(',','.'))
                return max(ret, 0)
            except ValueError as e:
                print('Problema com "{}": {}'.format(value, e))
                return 0
        saida['Bem_valor'] = dado['Bem_valor'].apply(func).astype(float)
        print_log('Feito: Bem_valor')

        return saida, tabela_depara


tabela_eleicao = pandas.read_csv('Parseados/utils_Eleicao_descricao.csv')
tabela_eleicao['norm'] = tabela_eleicao['Valor'].apply(
    utils.normalize_names
)
eleicao_dict = {x: y for x,y in zip(tabela_eleicao['norm'],tabela_eleicao['Codigo'])}
def convert_eleicao_descricao(value):
    comp = utils.normalize_names(limpa_string(value))
    ret = eleicao_dict.get(comp, None)
    if not ret:
        print('Problema: {}'.format(value))
    return ret


def normalizar_bem_tipo(dado):
    tabela = pandas.DataFrame()
    for ano in dado['Eleicao_ano'].unique():
        this = dado[dado['Eleicao_ano']==ano]
        depara1 = utils.tabela_depara(
            this,
            'Bem_codigo',
            'Bem_descricao'
        )
        depara = pandas.DataFrame()
        depara['Eleicao_ano'] = [ano]*len(depara1)
        depara['Codigo'] = depara1['Bem_codigo']
        depara['Valor'] = depara1['value'].apply(limpa_string)
        tabela = pandas.concat([tabela, depara])
    tabela.sort_values(by=['Eleicao_ano','Codigo'], inplace=True)
    coluna = dado['Bem_codigo'].apply(
        lambda x: max(0,x)
    )
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
        if '/candidatos/bens' in x \
        and (x.endswith('txt') or x.endswith('csv'))
    ]
    func = Arquivo_candidatos_bens().ler
    def read(arq):
        try:
            return func(arq)
        except:
            print('Problema com {}'.format(arq))
    dfs = [read(x) for x in arquivos]
    dff = pandas.concat(dfs)
    saida = Arquivo_candidatos_bens().parsear(dff)

    folder = './Parseados'
    try:
        os.makedirs(folder)
    except FileExistsError:
        pass
    for ano in saida[0]['Eleicao_ano'].unique():
        this = saida[0][saida[0]['Eleicao_ano']==ano]
        fname = os.path.join(folder, 'Bens_{}.csv'.format(ano))
        this.to_csv(fname, index=False, **csv_params)
    for label, val in saida[1].items():
        fname = os.path.join(folder, 'utils_{}.csv'.format(label))
        val.to_csv(fname, index=False, **csv_params)

    return saida

if __name__ == '__main__':
    main()
