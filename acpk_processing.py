from data_reader import load_excel
from utils import normalize

dict_df = load_excel('Data/ACPK/ReportesAcpk comites 1704.xlsx')

acpk = dict_df['data']

selection = {'Identificacion',
             'Tasa',
             'Plazo',
             'Estado',
             'Cuota',
             'Monto',
             'Condonación'
             }
dict_rename = {key: normalize(key).replace(' ', '_').upper() for key in selection}

acpk = acpk[list(selection)]

acpk = acpk.rename(columns=dict_rename)

# acpk.to_csv('outputs/acpk.csv')