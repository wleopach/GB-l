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
oblig_detail = {} ### hashmap oblig-->Nombre

def ed_basic_info(DF):
    DF.columns = DF.columns.str.strip()
    DF.columns = [Limpieza_Final_Str(j).replace(" ", "_") for j in list(DF.columns)]
    cols = set(DF.columns)
    req = {'IDENTIFICACION', 'OBLIGACION'}
    for i in cols & req:
        DF[i] = DF[i].astype(str)
    if 'NUMERO_DE_IDENTIFICACION' in cols:
        DF['IDENTIFICACION'] = DF['NUMERO_DE_IDENTIFICACION']

    if DF.IDENTIFICACION.isna().any():
        DF['IDENTIFICACION'] = DF['IDENTIFICACION'].fillna(method='ffill')

    name_parts = {'PRIMER_NOMBRE', 'PRIMER_APELLIDO', 'SEGUNDO_APELLIDO'}
    if name_parts < cols:
        DF['NOMBRE'] = DF[list(name_parts)].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)

    if 'OBLIGACION' not in cols and 'CONTRATO' in cols:
        DF['OBLIGACION'] = DF['CONTRATO']
    if 'OBLIGACION' not in cols and 'OBLIGACION16' in cols:
        DF['OBLIGACION'] = DF['OBLIGACION16']
    cols = set(DF.columns)
    list_names = list(DF.filter(regex='^NOMBRE').columns)
    if list_names == ['NOMBRE_REC_ANTERIOR']:
        DF['NOMBRE'] = ['NN']*len(DF)
        list_names = []
    if list_names and 'NOMBRE' not in cols:
        if 'NOMBRE_TITULAR' in list_names:
            DF['NOMBRE'] = DF.pop('NOMBRE_TITULAR')

        else:
            DF['NOMBRE'] = DF.pop(list_names[0])

    important_cols = ['IDENTIFICACION', 'NOMBRE', 'OBLIGACION']
    DF.drop(columns=[col for col in DF if col not in important_cols], inplace=True)

    DF.IDENTIFICACION = DF.IDENTIFICACION.astype(str)
    DF.IDENTIFICACION = DF.IDENTIFICACION.apply(lambda v: v.replace('.0', ''))
    DF.IDENTIFICACION = DF.IDENTIFICACION.astype(str)
    DF.NOMBRE = DF.NOMBRE.astype(str)
    DF.OBLIGACION = DF.OBLIGACION.astype(str)
    DF.NOMBRE = DF.NOMBRE.apply(lambda x: Limpieza_Final_Str(x))
    for row in range(len(DF)):
        names = id_nom.setdefault(DF['IDENTIFICACION'][row], [])
        if len(DF['NOMBRE'][row]) > 0 and DF['NOMBRE'][row] not in names:
            names.append(DF['NOMBRE'][row])
