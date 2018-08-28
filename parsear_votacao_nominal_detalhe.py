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


class Arquivo_votacao_nominal_detalhe():

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
        rename = {
            0: 'Geracao_data',
            1: 'Geracao_hora',
            2: 'Eleicao_ano',
            3: 'Eleicao_turno',
            4: 'Eleicao_descricao',
            6: 'Regiao_UE',
            7: 'Local_municicio',
            9: 'Local_zona',
            10: 'Cargo',
            12: 'Eleitores_aptos',
            13: 'Secoes',
            14: 'Secoes_agregadas',
            15: 'Eleitores_aptos_totalizados',
            16: 'Secoes_totalizadas',
            17: 'Eleitores_comparecimento',
            18: 'Eleitores_abstencoes',
            19: 'Eleitores_nominais',
            20: 'Eleitores_brancos',
            21: 'Eleitores_nulos',
            22: 'Eleitores_legenda',
            23: 'Eleitores_anulados',
        }
        if ano <= 2012:
            size = 27
        elif ano == 2014:
            size = 28
        else:
            size = 29
        dado.columns = [str(x) for x in range(len(dado.columns))]
        dado = dado.rename(columns={str(x):y for x,y in rename.items()})
        dado = dado[list(rename.values())]
        return dado

    def parsear(self, dado):
        dado = dado[dado['Cargo']<=13]
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

        saida['Eleicao_turno'] = dado['Eleicao_turno'].astype(int)
        print_log('Feito: Eleicao_turno')

        saida['Eleicao_descricao'] = dado['Eleicao_descricao'].apply(convert_eleicao_descricao)
        print_log('Feito: Eleicao_descricao')

        saida['Regiao_UE'] = dado['Regiao_UE'].astype(str)
        print_log('Feito: Regiao_UE')

        saida['Regiao_UE'] = dado['Regiao_UE'].astype(str)
        print_log('Feito: Regiao_UE')

        saida['Local_municicio'] = dado['Local_municicio'].astype(str)
        print_log('Feito: Local_municicio')

        saida['Local_zona'] = dado['Local_zona'].astype(int)
        print_log('Feito: Local_zona')

        saida['Cargo'] = dado['Cargo'].astype(int)
        print_log('Feito: Cargo')

        outros = {
            12: 'Eleitores_aptos',
            13: 'Secoes',
            14: 'Secoes_agregadas',
            15: 'Eleitores_aptos_totalizados',
            16: 'Secoes_totalizadas',
            17: 'Eleitores_comparecimento',
            18: 'Eleitores_abstencoes',
            19: 'Eleitores_nominais',
            20: 'Eleitores_brancos',
            21: 'Eleitores_nulos',
            22: 'Eleitores_legenda',
            23: 'Eleitores_anulados',
        }

        def func(val):
            try:
                return max(int(val),0)
            except ValueError:
                return 0
        for col in outros.values():
            saida[col] = dado[col].apply(func).astype(int)
            print_log(f'Feito: {col}')


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
        if '/votacao/detalhe/zona' in x \
        and (x.endswith('txt') or x.endswith('csv'))
    ]
    func = Arquivo_votacao_nominal_detalhe().ler
    def read(arq):
        try:
            return func(arq)
        except Exception as e:
            print('Problema com {}: {}'.format(arq, e))
    dfs = [read(x) for x in arquivos]
    dff = pandas.concat(dfs)
    saida = Arquivo_votacao_nominal_detalhe().parsear(dff)

    folder = './Parseados'
    try:
        os.makedirs(folder)
    except FileExistsError:
        pass
    for ano in saida[0]['Eleicao_ano'].unique():
        this = saida[0][saida[0]['Eleicao_ano']==ano]
        fname = os.path.join(folder, 'Votacao_detalhe_{}.csv'.format(ano))
        this.to_csv(fname, index=False, **csv_params)
    for label, val in saida[1].items():
        fname = os.path.join(folder, 'utils_{}.csv'.format(label))
        val.to_csv(fname, index=False, **csv_params)

    return saida

if __name__ == '__main__':
    main()
