import pandas
import string

def tabela_depara(dataframe, colunaA, colunaB):
    this = dataframe[[colunaA, colunaB]]
    df1 = this.groupby(colunaA, as_index=False).agg({
        colunaB: pandas.Series.nunique,
    }).rename({
        colunaB: 'count'
    }, axis=1)
    df2 = this.groupby(colunaA, as_index=False).agg({
        colunaB: 'first',
    }).rename({
        colunaB: 'value'
    }, axis=1)
    df1.loc[df1['count'] == 1, 'value'] = df2['value']
    return df1

def normalize_names(name):
    """
    Converte nomes para uppercase e remove acentos
    """
    name = name.lower()
    substitutos = {
        'á': 'a',
        'à': 'a',
        'â': 'a',
        'ã': 'a',

        'é': 'e',
        'è': 'e',
        'ê': 'e',
        'ẽ': 'e',

        'í': 'i',
        'ì': 'i',
        'î': 'i',
        'ĩ': 'i',

        'ó': 'o',
        'ò': 'o',
        'ô': 'o',
        'õ': 'o',

        'ú': 'u',
        'ù': 'u',
        'û': 'u',
        'ũ': 'u',
        'ü': 'u',

        'ç': 'c',
    }
    name = ''.join([substitutos.get(x,x) for x in name]).upper()
    # if not all([x in string.ascii_uppercase+" -'." for x in name]):
    #     print(name)
    return name
