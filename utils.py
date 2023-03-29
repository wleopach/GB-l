import re
import heapq


def normalize(s):
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
    return s


def Limpieza_Final_Str(arg):
    return (re.sub(" +", " ", normalize(arg)).rstrip().lstrip().upper())


def get_df(DF):
    """Retrives relevant  data from the .xlsx stored as values in a dictionary"""

    num_col = [len(DF[key].columns) for key in DF]
    heapq._heapify_max(num_col)
    max = heapq._heappop_max(num_col)
    for key in DF:
        if len(DF[key].columns) == max:
            return DF[key]

    return None


id_nom = {}  ### hashmap id-->Nombre

Dic_Obligacion = {}


def ed_basic_info(DF, sinon, req, oblig_data, sumar_saldo=False):
    """
    Fits the data of a DF to fill the Diccionario_Retornar.

    DF: pandas dataframe
    sinon: dictionary of synonyms
    req: set of required columns
    oblig_data: columns corresponding to an obligation
    sumar_saldo: determines whether or not to sum balances when there is no SALDO_TOTAL
    """
    DF.columns = DF.columns.str.strip()
    DF.columns = [Limpieza_Final_Str(j).replace(" ", "_") for j in list(DF.columns)]
    cols = set(DF.columns)
    for i in cols & req:
        DF[i] = DF[i].astype(str)
    keys = set(sinon.keys())
    for i in keys & cols:
        if sinon[i] not in cols:
            DF[sinon[i]] = DF[i]
        if i == 'NUMERO_DE_IDENTIFICACION':
            DF[sinon[i]] = DF[i]

    ##### if FECHA_CASTIGO_MIN ----> FECHA_CASTIGO= FECHA_CASTIGO_MIN
    if 'FECHA_CASTIGO_MIN' in set(DF.columns):
        DF['FECHA_CASTIGO'] = DF['FECHA_CASTIGO_MIN']

    ##### if not SALDO_TOTAL ==> SUM *SALDO
    if sumar_saldo and 'SALDO_TOTAL' not in set(DF.columns):
        DF['SALDO_TOTAL'] = DF.filter(regex='^SALDO').replace('[\$,.]', '', regex=True).astype(float).sum(axis=1)

    cols = set(DF.columns)
    ##### if not SALDO_CAPITAL_CLIENT ==> GROUPBY ID AND SUM SALDO_CAPITAL_VENDIDO
    if 'SALDO_CAPITAL_VENDIDO' in cols and 'SALDO_CAPITAL_CLIENTE' not in cols:
        qu = DF.groupby('IDENTIFICACION')['SALDO_CAPITAL_VENDIDO'].sum().reset_index()
        DF['SALDO_CAPITAL_CLIENTE'] = DF['IDENTIFICACION'].apply(lambda x: scv(qu, x))
    if DF.IDENTIFICACION.isna().any():
        DF['IDENTIFICACION'] = DF['IDENTIFICACION'].fillna(method='ffill')

    name_parts = {'PRIMER_NOMBRE', 'PRIMER_APELLIDO', 'SEGUNDO_APELLIDO'}
    if name_parts < cols:
        DF['NOMBRE'] = DF[list(name_parts)].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)

    cols = set(DF.columns)
    list_names = list(DF.filter(regex='^NOMBRE').columns)
    if list_names == ['NOMBRE_REC_ANTERIOR']:
        DF['NOMBRE'] = [None] * len(DF)
        list_names = []
    if list_names and 'NOMBRE' not in cols:
        if 'NOMBRE_TITULAR' in list_names:
            DF['NOMBRE'] = DF.pop('NOMBRE_TITULAR')

        else:
            DF['NOMBRE'] = DF.pop(list_names[0])

    DF.drop(columns=[col for col in DF if col not in req], inplace=True)
    for col in req - cols:
        DF[col] = [None] * len(DF)
    for col in req:
        DF[col] = DF[col].astype(str)

    DF.IDENTIFICACION = DF.IDENTIFICACION.apply(lambda v: v.replace('.0', ''))
    DF.NOMBRE = DF.NOMBRE.apply(lambda x: Limpieza_Final_Str(x))
    for row in range(len(DF)):
        names = id_nom.setdefault(DF['IDENTIFICACION'][row], [])
        if len(DF['NOMBRE'][row]) > 0 and DF['NOMBRE'][row] not in names:
            names.append(DF['NOMBRE'][row])
        history = Dic_Obligacion.setdefault(DF['OBLIGACION'][row], {})
        for atribute in oblig_data:
            history[atribute] = history.setdefault(atribute, set())
            history[atribute].add(DF[atribute][row])
    for i in cols & req:
        DF[i] = DF[i].astype(str)


# for key in Diccionario_Com:
#     DF = get_df(Diccionario_Com[key])
#     if 'SALDO_TOTAL' not in DF.filter(regex='SALDO').columns:
#         print(key)
#         print(DF.filter(regex='^SALDO').columns)

def scv(qu, id):
    """Returns result in a query by id"""
    return qu[qu['IDENTIFICACION'] == id]['SALDO_CAPITAL_VENDIDO'].values[0]


def check_cols(data):
    """Prints the name of the archives and the  None columns """
    nf_cols = {}
    for key in data:
        mask = data[key].astype(str).apply(lambda x: x.str.contains('None')).any(axis=0)
        result = data[key].loc[:5, mask]
        nf_cols[key] = set(result.columns)
    for k in nf_cols:
        print(f"IN {k.split('/')[-1]} : {nf_cols[k]}")



