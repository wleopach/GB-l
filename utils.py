import re

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
    return(re.sub(" +"," ",normalize(arg)).rstrip().lstrip().upper())

def get_df(DF):
    """Retrives relevant  data from the .xlsx"""
    for key in  DF:
        if 'IDENTIFICACION' in DF[key].columns:
            return DF[key]

    return None


def ed_basic_info(DF):
    DF.columns = [Limpieza_Final_Str(j).replace(" ", "_") for j in list(DF.columns)]
    cols = set(DF.columns)
    req = {'IDENTIFICACION', 'OBLIGACION'}
    for i in cols&req:
        DF[i] = DF[i].astype(str)