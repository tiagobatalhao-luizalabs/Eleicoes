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

class Arquivo_candidatos_lista():

    def get_colunas_tse(self, ano):
        """
        Retorna as colunas do arquivo de candidatos em cada ano
        """
        base = [
            ('DATA_GERACAO', 'datetime'),
            ('HORA_GERACAO', 'datetime'),
            ('ANO_ELEICAO', 'integer'),
            ('NUM_TURNO', 'integer'),
            ('DESCRICAO_ELEICAO', 'string'),
            ('SIGLA_UF', 'string'),
            ('SIGLA_UE', 'string'),
            ('DESCRICAO_UE', 'string'),
            ('CODIGO_CARGO', 'integer'),
            ('DESCRICAO_CARGO', 'string'),
            ('NOME_CANDIDATO', 'string'),
            ('SEQUENCIAL_CANDIDATO', 'integer'),
            ('NUMERO_CANDIDATO', 'integer'),
            ('CPF_CANDIDATO', 'string'),
            ('NOME_URNA_CANDIDATO', 'string'),
            ('COD_SITUACAO_CANDIDATURA', 'integer'),
            ('DES_SITUACAO_CANDIDATURA', 'string'),
            ('NUMERO_PARTIDO', 'integer'),
            ('SIGLA_PARTIDO', 'string'),
            ('NOME_PARTIDO', 'string'),
            ('CODIGO_LEGENDA', 'integer'),
            ('SIGLA_LEGENDA', 'string'),
            ('COMPOSICAO_LEGENDA', 'string'),
            ('NOME_LEGENDA', 'string'),
            ('CODIGO_OCUPACAO', 'integer'),
            ('DESCRICAO_OCUPACAO', 'string'),
            ('DATA_NASCIMENTO', 'datetime'),
            ('NUM_TITULO_ELEITORAL_CANDIDATO', 'string'),
            ('IDADE_DATA_ELEICAO', 'integer'),
            ('CODIGO_SEXO', 'integer'),
            ('DESCRICAO_SEXO', 'string'),
            ('COD_GRAU_INSTRUCAO', 'integer'),
            ('DESCRICAO_GRAU_INSTRUCAO', 'string'),
            ('CODIGO_ESTADO_CIVIL', 'integer'),
            ('DESCRICAO_ESTADO_CIVIL', 'string'),
            ('CODIGO_NACIONALIDADE', 'integer'),
            ('DESCRICAO_NACIONALIDADE', 'string'),
            ('SIGLA_UF_NASCIMENTO', 'string'),
            ('CODIGO_MUNICIPIO_NASCIMENTO', 'integer'),
            ('NOME_MUNICIPIO_NASCIMENTO', 'string'),
            ('DESPESA_MAX_CAMPANHA', 'float'),
            ('COD_SIT_TOT_TURNO', 'integer'),
            ('DESC_SIT_TOT_TURNO', 'string'),
        ]
        if ano <= 2010:
            colunas = base
        if ano == 2012:
            colunas = base + [
                ('NM_EMAIL', 'string'),
            ]
        if ano >= 2014 and ano < 2018:
            colunas = base[:-8] + [
                ('CODIGO_COR_RACA', 'integer'),
                ('DESCRICAO_COR_RACA', 'string'),
            ] + base[-8:] + [
                ('NM_EMAIL', 'string'),
            ]
        if ano == 2018:
            colunas = [
                ('DT_GERACAO', 'datetime'),
                ('HH_GERACAO', 'datetime'),
                ('ANO_ELEICAO', 'integer'),
                ('CD_TIPO_ELEICAO', 'integer'),
                ('NM_TIPO_ELEICAO', 'string'),
                ('NR_TURNO', 'integer'),
                ('CD_ELEICAO', 'integer'),
                ('DS_ELEICAO', 'string'),
                ('DT_ELEICAO', 'datetime'),
                ('TP_ABRANGENCIA', 'string'),
                ('SG_UF', 'string'),
                ('SG_UE', 'string'),
                ('NM_UE', 'string'),
                ('CD_CARGO', 'integer'),
                ('DS_CARGO', 'string'),
                ('SQ_CANDIDATO', 'integer'),
                ('NR_CANDIDATO', 'integer'),
                ('NM_CANDIDATO', 'string'),
                ('NM_URNA_CANDIDATO', 'string'),
                ('NM_SOCIAL_CANDIDATO', 'string'),
                ('NR_CPF_CANDIDATO', 'string'),
                ('NM_EMAIL', 'string'),
                ('CD_SITUACAO_CANDIDATURA', 'integer'),
                ('DS_SITUACAO_CANDIDATURA', 'string'),
                ('CD_DETALHE_SITUACAO_CAND', 'integer'),
                ('DS_DETALHE_SITUACAO_CAND', 'string'),
                ('TP_AGREMIACAO', 'string'),
                ('NR_PARTIDO', 'integer'),
                ('SG_PARTIDO', 'string'),
                ('NM_PARTIDO', 'string'),
                ('SQ_COLIGACAO', 'integer'),
                ('NM_COLIGACAO', 'string'),
                ('DS_COMPOSICAO_COLIGACAO', 'string'),
                ('CD_NACIONALIDADE', 'integer'),
                ('DS_NACIONALIDADE', 'string'),
                ('SG_UF_NASCIMENTO', 'string'),
                ('CD_MUNICIPIO_NASCIMENTO', 'integer'),
                ('NM_MUNICIPIO_NASCIMENTO', 'string'),
                ('DT_NASCIMENTO', 'datetime'),
                ('NR_IDADE_DATA_POSSE', 'integer'),
                ('NR_TITULO_ELEITORAL_CANDIDATO', 'string'),
                ('CD_GENERO', 'integer'),
                ('DS_GENERO', 'string'),
                ('CD_GRAU_INSTRUCAO', 'integer'),
                ('DS_GRAU_INSTRUCAO', 'string'),
                ('CD_ESTADO_CIVIL', 'integer'),
                ('DS_ESTADO_CIVIL', 'string'),
                ('CD_COR_RACA', 'integer'),
                ('DS_COR_RACA', 'string'),
                ('CD_OCUPACAO', 'integer'),
                ('DS_OCUPACAO', 'string'),
                ('NR_DESPESA_MAX_CAMPANHA', 'float'),
                ('CD_SIT_TOT_TURNO', 'integer'),
                ('DS_SIT_TOT_TURNO', 'string'),
                ('ST_REELEICAO', 'string'),
                ('ST_DECLARAR_BENS', 'string'),
                ('NR_PROTOCOLO_CANDIDATURA', 'integer'),
                ('NR_PROCESSO', 'integer'),
            ]
        return colunas

    def get_colunas_labs(self, ano):
        """
        Retorna as colunas do arquivo de candidatos em cada ano
        """
        base = [
            ('Geracao_data', 'datetime'),
            ('Geracao_hora', 'datetime'),
            ('Eleicao_ano', 'integer'),
            ('Eleicao_turno', 'integer'),
            ('Eleicao_descricao', 'string'),
            ('Regiao_UF_sigla', 'string'),
            ('Regiao_UE_sigla', 'string'),
            ('Regiao_UE_descricao', 'string'),
            ('Cargo_codigo', 'integer'),
            ('Cargo_descricao', 'string'),
            ('Candidato_nome_completo', 'string'),
            ('Candidato_id', 'integer'),
            ('Candidato_numero', 'integer'),
            ('Candidato_CPF', 'string'),
            ('Candidato_nome_urna', 'string'),
            ('Candidatura_situacao_codigo', 'integer'),
            ('Candidatura_situacao_descricao', 'string'),
            ('Partido_numero', 'integer'),
            ('Partido_sigla', 'string'),
            ('Partido_nome', 'string'),
            ('Legenda_codigo', 'integer'),
            ('Legenda_sigla', 'string'),
            ('Legenda_composicao', 'string'),
            ('Legenda_nome', 'string'),
            ('Candidato_ocupacao_codigo', 'integer'),
            ('Candidato_ocupacao_descricao', 'string'),
            ('Candidato_nascimento_data', 'datetime'),
            ('Candidato_tituloeleitoral', 'string'),
            ('Candidato_idade', 'integer'),
            ('Candidato_sexo_codigo', 'integer'),
            ('Candidato_sexo_descricao', 'string'),
            ('Candidato_instrucao_codigo', 'integer'),
            ('Candidato_instrucao_descricao', 'string'),
            ('Candidato_estadocivil_codigo', 'integer'),
            ('Candidato_estadocivil_descricao', 'string'),
            ('Candidato_nacionalidade_codigo', 'integer'),
            ('Candidato_nacionalidade_descricao', 'string'),
            ('Candidato_nascimento_UF', 'string'),
            ('Candidato_nascimento_municipio_codigo', 'integer'),
            ('Candidato_nascimento_municipio_nome', 'string'),
            ('Candidatura_despesa_maxima', 'float'),
            ('Candidatura_eleito_codigo', 'integer'),
            ('Candidatura_eleito_descricao', 'string'),
        ]
        if ano <= 2010:
            colunas = base
        if ano == 2012:
            colunas = base + [
                ('Candidato_email', 'string'),
            ]
        if ano >= 2014 and ano < 2018:
            colunas = base[:-8] + [
                ('Candidato_cor_codigo', 'integer'),
                ('Candidato_cor_descricao', 'string'),
            ] + base[-8:] + [
                ('Candidato_email', 'string'),
            ]
        if ano == 2018:
            colunas = [
                ('Geracao_data', 'datetime'),
                ('Geracao_hora', 'datetime'),
                ('Eleicao_ano', 'integer'),
                ('Eleicao_tipo_codigo', 'integer'),
                ('Eleicao_tipo_descricao', 'string'),
                ('Eleicao_turno', 'integer'),
                ('Eleicao_codigo', 'integer'),
                ('Eleicao_descricao', 'string'),
                ('Eleicao_data', 'datetime'),
                ('Eleicao_abrangencia', 'string'),
                ('Regiao_UF_sigla', 'string'),
                ('Regiao_UE_sigla', 'string'),
                ('Regiao_UE_descricao', 'string'),
                ('Cargo_codigo', 'integer'),
                ('Cargo_descricao', 'string'),
                ('Candidato_id', 'integer'),
                ('Candidato_numero', 'integer'),
                ('Candidato_nome_completo', 'string'),
                ('Candidato_nome_urna', 'string'),
                ('Candidato_nome_social', 'string'),
                ('Candidato_CPF', 'string'),
                ('Candidato_email', 'string'),
                ('Candidatura_situacao_codigo', 'integer'),
                ('Candidatura_situacao_descricao', 'string'),
                ('Candidatura_detalhe_codigo', 'integer'),
                ('Candidatura_detalhe_descricao', 'string'),
                ('Legenda_tipo', 'string'),
                ('Partido_numero', 'integer'),
                ('Partido_sigla', 'string'),
                ('Partido_nome', 'string'),
                ('Legenda_codigo', 'integer'),
                ('Legenda_nome', 'string'),
                ('Legenda_composicao', 'string'),
                ('Candidato_nacionalidade_codigo', 'integer'),
                ('Candidato_nacionalidade_descricao', 'string'),
                ('Candidato_nascimento_UF', 'string'),
                ('Candidato_nascimento_municipio_codigo', 'integer'),
                ('Candidato_nascimento_municipio_nome', 'string'),
                ('Candidato_nascimento_data', 'datetime'),
                ('Candidato_idade', 'integer'),
                ('Candidato_tituloeleitoral', 'string'),
                ('Candidato_sexo_codigo', 'integer'),
                ('Candidato_sexo_descricao', 'string'),
                ('Candidato_instrucao_codigo', 'integer'),
                ('Candidato_instrucao_descricao', 'string'),
                ('Candidato_estadocivil_codigo', 'integer'),
                ('Candidato_estadocivil_descricao', 'string'),
                ('Candidato_cor_codigo', 'integer'),
                ('Candidato_cor_descricao', 'string'),
                ('Candidato_ocupacao_codigo', 'integer'),
                ('Candidato_ocupacao_descricao', 'string'),
                ('Candidatura_despesa_maxima', 'float'),
                ('Candidatura_eleito_codigo', 'integer'),
                ('Candidatura_eleito_descricao', 'string'),
                ('Candidatura_reeleicao', 'string'),
                ('Candidatura_declaracaobens', 'string'),
                ('Candidatura_protocolo', 'integer'),
                ('Candidatura_processo', 'integer'),
            ]
        return colunas

    def ler(self, arquivo):
        func_colunas = self.get_colunas_labs

        expression = re.compile('20[0-9]{2}')
        ano = int(expression.search(arquivo).group())
        colunas = func_colunas(ano)
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
        dado.columns = [x[0] for x in colunas]
        def limpar_inteiro(x):
            try:
                x = int(x)
                if x in [-1, -3]:
                    return -1
                return x
            except:
                return -1
        def limpar_string(x):
            if x in ['#NULO', '#NE']:
                return np.nan
            else:
                return str(x)
        for coluna, tipo in colunas:
            if tipo == 'integer':
                try:
                    dado[coluna] = dado[coluna].apply(limpar_inteiro).astype(int)
                except ValueError:
                    print(arquivo, coluna)
            if tipo == 'string':
                dado[coluna] = dado[coluna].astype(str)

        colunas_ordem = [x[0] for x in func_colunas(2018)]
        ordem = [x for x in colunas_ordem if x in dado.columns]
        outras = [x for x in dado.columns if x not in colunas_ordem]
        return dado[ordem + outras]

    def parsear(self, dado):
        dado = dado[dado['Cargo_codigo'] <= 13]
        saida = pandas.DataFrame()
        tabela_depara = {}

        saida['Geracao'] = dado.apply(
            lambda row: datetime.strptime(row['Geracao_data']+row['Geracao_hora'],'%d/%m/%Y%H:%M:%S'),
            axis=1,
        )
        print_log('Feito: geracao')

        saida['Eleicao_ano'] = dado['Eleicao_ano'].astype(int)
        saida['Eleicao_turno'] = dado['Eleicao_turno'].astype(int)
        tabela_depara['Eleicao_descricao'], coluna = normalizar_eleicao_descricao(dado)
        saida['Eleicao_descricao'] = coluna
        print_log('Feito: eleicao')

        tabela_depara['UE'], coluna = normalizar_UE(dado)
        saida['Regiao_UE'] = coluna
        print_log('Feito: regiao')

        tabela_depara['Cargo'], coluna = normalizar_cargo(dado)
        saida['Cargo'] = coluna
        print_log('Feito: cargo')

        tabela_depara['Situacao'], coluna = normalizar_situacao_historico(dado)
        saida['Candidatura_situacao'] = coluna
        index = dado['Eleicao_ano']==2018
        saida.loc[index,'Candidatura_situacao'] = dado.loc[index,'Candidatura_detalhe_codigo'].astype(int)
        print_log('Feito: situacao')

        saida['Candidatura_despesamaxima'] = dado['Candidatura_despesa_maxima'].astype(float)
        print_log('Feito: despesa')

        tabela_depara['Eleito'], coluna = normalizar_eleito(dado)
        saida['Candidatura_eleito'] = coluna
        print_log('Feito: eleito')

        func = lambda x: 1 if x=='S' else 0 if x=='N' else -1
        saida['Candidato_2018_reeleicao'] = dado['Candidatura_reeleicao'].apply(func).astype(int)
        saida['Candidato_2018_bens'] = dado['Candidatura_declaracaobens'].apply(func).astype(int)
        saida['Candidato_2018_processo'] = dado['Candidatura_processo'].apply(
            lambda x: str(int(round(x))) if x>0 else ''
        ).astype(str)
        print_log('Feito: informacao 2018')

        saida['Candidato_id'] = dado['Candidato_id'].astype(int)
        saida['Candidato_numero'] = dado['Candidato_numero'].astype(int)
        saida['Candidato_nome_completo'] = dado['Candidato_nome_completo'].apply(limpa_string).astype(str)
        saida['Candidato_nome_urna'] = dado['Candidato_nome_urna'].apply(limpa_string).astype(str)
        saida['Candidato_nome_social'] = dado['Candidato_nome_social'].apply(limpa_string).astype(str)
        func = lambda x: str(x).zfill(11) if len(x)>7 else ''
        saida['Candidato_CPF'] = dado['Candidato_CPF'].apply(func).apply(limpa_string).astype(str)
        func = lambda x: x.lower() if isinstance(x,str) else ''
        saida['Candidato_email'] = dado['Candidato_email'].apply(func).apply(limpa_string).astype(str)
        print_log('Feito: Candidato_nome')

        tabela_depara['Nacionalidade'], coluna = normalizar_nacionalidade(dado)
        saida['Candidato_nacionalidade'] = coluna
        print_log('Feito: Candidato_nacionalidade')

        saida['Candidato_nascimento_municipio'] = dado['Candidato_nascimento_municipio_nome'].apply(limpa_string)
        saida['Candidato_nascimento_UF'] = dado['Candidato_nascimento_UF'].apply(limpa_string).apply(
            lambda x: x if (len(x)==2 and x!='-1') else ''
        )
        saida['Candidato_nascimento_data'] = dado['Candidato_nascimento_data'].apply(
            # lambda x: dateparser.parse(x).date()
            lambda x: convert_date(x)
        )
        saida['Candidato_idade'] = dado['Candidato_idade'].astype(int)
        print_log('Feito: Candidato_nascimento')

        saida['Candidato_tituloeleitoral'] = dado['Candidato_tituloeleitoral'].apply(limpa_string).astype(str)
        print_log('Feito: Candidato_tituloeleitoral')

        tabela_depara['Genero'], coluna = normalizar_sexo(dado)
        saida['Candidato_genero'] = coluna
        print_log('Feito: Candidato_genero')

        tabela_depara['Cor'], coluna = normalizar_cor(dado)
        saida['Candidato_cor'] = coluna
        print_log('Feito: Candidato_cor')

        tabela_depara['Instrucao'], coluna = normalizar_instrucao(dado)
        saida['Candidato_instrucao'] = coluna
        print_log('Feito: Candidato_instrucao')

        tabela_depara['Estadocivil'], coluna = normalizar_estadocivil(dado)
        saida['Candidato_estadocivil'] = coluna
        print_log('Feito: Candidato_estadocivil')

        tabela_depara['Ocupacao'], coluna = normalizar_ocupacao(dado)
        saida['Candidato_ocupacao'] = coluna
        print_log('Feito: Candidato_ocupacao')

        tabela_depara['Partido'], coluna = normalizar_partido(dado)
        saida['Partido'] = coluna
        print_log('Feito: Partido')

        def func(legenda):
            ls = legenda.split('/')
            ls = [x.strip().strip("'") for x in ls]
            ls = [x for x in ls if len(x)>0]
            return '-'.join(ls)
        saida['Legenda_composicao'] = dado['Legenda_composicao'].apply(limpa_string).apply(
            func
        )
        saida['Legenda_nome'] = dado['Legenda_nome'].apply(limpa_string)
        print_log('Feito: Legenda')

        return saida, tabela_depara

def limpa_string(s):
    if not isinstance(s, str):
        return ''
    elif s=='nan':
        return ''
    elif s.startswith('#n') or s.startswith('#N'):
        return ''
    else:
        return s


def normalizar_eleicao_descricao(dado):
    ls = dado['Eleicao_descricao'].unique()
    codigo_tse_dict = {
        'ELEICOES 2002': 21,
        'ELEICOES 2004': 22,
        'ELEICOES 2006': 23,
        'Eleições 2008': 24,
        'ELEIÇÕES 2010': 25,
        'ELEIÇÃO MUNICIPAL 2012': 26,
        'Eleições Gerais 2014': 27,
        'Eleições Municipais 2016': 28,
        'Eleições Gerais Estaduais 2018': 29,
        'Eleição Geral Federal 2018': 29,
        'nan': -1,
    }
    dic = {
        -1: '',
        21: 'Eleições 2002',
        22: 'Eleições 2004',
        23: 'Eleições 2006',
        24: 'Eleições 2008',
        25: 'Eleições 2010',
        26: 'Eleições 2012',
        27: 'Eleições 2014',
        28: 'Eleições 2016',
        29: 'Eleições 2018',
    }
    outras = sorted([x for x in ls if x not in codigo_tse_dict.keys()])
    for i,label in enumerate(outras):
        codigo_tse_dict[label] = 9001+i
        dic[9001+i] = label
    coluna = dado['Eleicao_descricao'].apply(lambda x: codigo_tse_dict[x])
    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())[1:]
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    return tabela, coluna

def normalizar_UE(dado):
    tabela = pandas.DataFrame()
    for ano in dado['Eleicao_ano'].unique():
        this = dado[dado['Eleicao_ano']==ano]
        depara1 = utils.tabela_depara(
            this,
            'Regiao_UE_sigla',
            'Regiao_UE_descricao'
        )
        depara2 = utils.tabela_depara(
            this,
            'Regiao_UE_sigla',
            'Regiao_UF_sigla'
        )
        depara = pandas.DataFrame()
        depara['Eleicao_ano'] = [ano]*len(depara1)
        depara['Regiao_UE_codigo'] = depara1['Regiao_UE_sigla']
        depara['Regiao_UE_descricao'] = depara1['value'].apply(limpa_string)
        depara['Regiao_UF_sigla'] = depara2['value'].apply(limpa_string)
        tabela = pandas.concat([tabela, depara])
    tabela.sort_values(by=['Eleicao_ano','Regiao_UE_codigo'], inplace=True)
    return tabela, dado['Regiao_UE_sigla']

def normalizar_cargo(dado):
    dic = {
        1: 'PRESIDENTE',
        2: 'VICE-PRESIDENTE',
        3: 'GOVERNADOR',
        4: 'VICE-GOVERNADOR',
        5: 'SENADOR',
        6: 'DEPUTADO FEDERAL',
        7: 'DEPUTADO ESTADUAL',
        8: 'DEPUTADO DISTRITAL',
        9: '1º SUPLENTE SENADOR',
        10: '2º SUPLENTE SENADOR',
        11: 'PREFEITO',
        12: 'VICE-PREFEITO',
        13: 'VEREADOR',
    }
    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    return tabela, dado['Cargo_codigo']


def normalizar_situacao_historico(dado):
    dic = {
        1: 'CADASTRADO',
        2: 'DEFERIDO',
        3: 'INAPTO',
        4: 'SUB JUDICE',
        5: 'CANCELADO',
        6: 'RENÚNCIA',
        7: 'FALECIDO',
        8: 'AGUARDANDO JULGAMENTO',
        9: 'INELEGÍVEL',
        10: 'CASSADO',
        11: 'IMPUGNADO',
        13: 'NÃO CONHECIMENTO DO PEDIDO',
        14: 'INDEFERIDO',
        16: 'DEFERIDO COM RECURSO',
        17: 'PENDENTE DE JULGAMENTO',
        18: 'CASSADO COM RECURSO',
        19: 'CANCELADO COM RECURSO',
    }
    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    return tabela, dado['Candidatura_situacao_codigo']

def normalizar_eleito(dado):
    dic = {
        1: 'ELEITO',
        2: 'SUPLENTE',
        3: '2º TURNO',
        4: 'NÃO ELEITO',
    }
    def func(row):
        if row['Eleicao_ano'] in [2018,2016,2014,2012]:
            sub = {
                1: (1,2,3),
                2: (5,),
                3: (6,),
                4: (4,),
            }
        if row['Eleicao_ano'] in [2010,2008,2006,2004,2002]:
            sub = {
                1: (1,5),
                2: (2,),
                3: (6,),
                4: (3,4,7,8,9,10,11,12),
            }
        sub_table = {}
        for new, old in sub.items():
            for old_elem in old:
                sub_table[old_elem] = new
        element = row['Candidatura_eleito_codigo']
        return sub_table.get(element, element)

    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    coluna = dado.apply(func, axis=1)
    return tabela, coluna

def normalizar_nacionalidade(dado):
    dic = {
        1: 'BRASILEIRA',
        2: 'BRASILEIRA (NATURALIZADA)',
        3: 'PORTUGUESA COM IGUALDADE DE DIREITOS',
        4: 'ESTRANGEIRO',
    }
    sub = {
        1: 1,
        2: 2,
        4: 4,
        0: -1,
        3: 3,
        -1: 1,
        -1: 1,
    }
    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    coluna = dado['Candidato_nacionalidade_codigo'].apply(
        lambda x: sub.get(x,x)
    )
    return tabela, coluna

def normalizar_sexo(dado):
    dic = {
        2: 'Masculino',
        4: 'Feminino',
    }
    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    coluna = dado['Candidato_sexo_codigo'].apply(
        lambda x: {2:2,4:4}.get(x,-1)
    )
    return tabela, coluna


def normalizar_cor(dado):
    dic = {
        1: 'Branca',
        2: 'Preta',
        3: 'Parda',
        4: 'Amarela',
        5: 'Indígena',
    }
    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    coluna = dado['Candidato_cor_codigo'].apply(
        lambda x: {x:x for x in range(1,6)}.get(x,-1)
    )
    return tabela, coluna


def normalizar_instrucao(dado):
    dic = {
        1: 'Analfabeto',
        2: 'Alfabetizado',
        3: 'Fundamental incompleto',
        4: 'Fundamental completo',
        5: 'Médio incompleto',
        6: 'Médio completo',
        7: 'Superior incompleto',
        8: 'Superior completo',
    }
    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    coluna = dado['Candidato_instrucao_codigo'].apply(
        lambda x: {x:x for x in range(1,9)}.get(x,-1)
    )
    return tabela, coluna

def normalizar_estadocivil(dado):
    dic = {
        3: 'Casado',
        9: 'Divorciado',
        7: 'Separado',
        1: 'Solteiro',
        5: 'Viúvo',
    }
    tabela = pandas.DataFrame()
    tabela['Codigo'] = sorted(dic.keys())
    tabela['Valor'] = [limpa_string(dic[x]) for x in tabela['Codigo']]
    coluna = dado['Candidato_estadocivil_codigo'].apply(
        lambda x: {x:x for x in range(1,10)}.get(x,-1)
    )
    return tabela, coluna


def normalizar_ocupacao(dado):
    tabela = pandas.DataFrame()
    for ano in dado['Eleicao_ano'].unique():
        this = dado[dado['Eleicao_ano']==ano]
        depara1 = utils.tabela_depara(
            this,
            'Candidato_ocupacao_codigo',
            'Candidato_ocupacao_descricao'
        )
        depara = pandas.DataFrame()
        depara['Eleicao_ano'] = [ano]*len(depara1)
        depara['Codigo'] = depara1['Candidato_ocupacao_codigo']
        depara['Valor'] = depara1['value'].apply(limpa_string)
        tabela = pandas.concat([tabela, depara])
    tabela.sort_values(by=['Eleicao_ano','Codigo'], inplace=True)
    coluna = dado['Candidato_ocupacao_codigo'].apply(
        lambda x: max(0,x)
    )
    return tabela, dado['Candidato_ocupacao_codigo']


def normalizar_partido(dado):
    tabela = pandas.DataFrame()
    for ano in dado['Eleicao_ano'].unique():
        this = dado[dado['Eleicao_ano']==ano]
        depara1 = utils.tabela_depara(
            this,
            'Partido_numero',
            'Partido_sigla'
        )
        depara2 = utils.tabela_depara(
            this,
            'Partido_numero',
            'Partido_nome'
        )
        depara = pandas.DataFrame()
        depara['Eleicao_ano'] = [ano]*len(depara1)
        depara['Numero'] = depara1['Partido_numero']
        depara['Sigla'] = depara1['value'].apply(limpa_string)
        depara['Nome'] = depara2['value'].apply(limpa_string)
        tabela = pandas.concat([tabela, depara])
    tabela.sort_values(by=['Eleicao_ano','Numero'], inplace=True)
    return tabela, dado['Partido_sigla']

expressions = {
    '[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}': '%d/%m/%Y',
    '[0-9]{1,2}-[A-Za-z]{3}-[0-9]{1,4}': '%d-%b-%y',
    '[0-9]{8}': '%d%m%Y',
    '[0-9]{8}.0': '%d%m%Y.0',
}
def convert_date(x):
    s = str(x).split('.')[0]
#     if len(s)==8:
#         s = s.replace(' ','0')
    for ex, fmt in expressions.items():
        match = re.compile(ex).fullmatch(s)
        if match:
            try:
                return datetime.strptime(match.group(), fmt).date()
            except ValueError as e:
                print('Problema com {}: {}'.format(s,e))
                return None
    if len(s) in [6,7]:
        if s[-4:-2]=='19':
            year = int(s[-4:])
            daymonth = s[:-4]
        else:
            year = int('19'+s[-2:])
            daymonth = s[:-2]
        if len(daymonth) not in [2,3,4]:
            return None
        elif len(daymonth)==4:
            day = int(daymonth[0:2])
            month = int(daymonth[2:4])
            return datetime(year, month, day).date()
        elif len(daymonth)==2:
            day = int(daymonth[0:1])
            month = int(daymonth[1:2])
            return datetime(year, month, day).date()
        elif len(daymonth)==3:
            daymonth = daymonth.replace(' ','0')
            day = int(daymonth[0:1])
            month = int(daymonth[1:3])
            return datetime(year, month, day).date()
    return None

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
        if '/candidatos/lista' in x \
        and (x.endswith('txt') or x.endswith('csv'))
    ]
    func = Arquivo_candidatos_lista().ler
    def read(arq):
        try:
            return func(arq)
        except:
            print('Problema com {}'.format(arq))
    dfs = [read(x) for x in arquivos]
    dff = pandas.concat(dfs)
    saida = Arquivo_candidatos_lista().parsear(dff)

    folder = './Parseados'
    try:
        os.makedirs(folder)
    except FileExistsError:
        pass
    for ano in saida[0]['Eleicao_ano'].unique():
        this = saida[0][saida[0]['Eleicao_ano']==ano]
        fname = os.path.join(folder, 'Candidatos_{}.csv'.format(ano))
        this.to_csv(fname, index=False, **csv_params)
    for label, val in saida[1].items():
        fname = os.path.join(folder, 'utils_{}.csv'.format(label))
        val.to_csv(fname, index=False, **csv_params)

    return saida

if __name__ == '__main__':
    main()
