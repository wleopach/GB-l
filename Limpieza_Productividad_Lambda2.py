from utils import Confirmar_Archivo, Leer_Datos_Carpetas, tables
import ray
import psutil
import pickle
import gc
import pandas as pd

PATH_TO_PRODUCTIVIDAD = "/home/leopach/tulipan/GB/Base_de_Compras/Data_Mar_28/Data_2/Productividad/"

#############################Load directories in Productividad           ####################################


files = tables(PATH_TO_PRODUCTIVIDAD)
years = {'2021', '2022'}
labels = {'Encuesta', 'Acumulado', 'Planta'}


def path_to_tuple(path) -> ():
    """
    Transforms path into a tuple of the form (label,year,path)
    :param path: path to csv or xlsx file
    :return: tuple
    """
    x0 = ''
    x1 = ''
    for year in years:
        if year in path:
            x1 = year
            break
    for label in labels:
        if label in path:
            x0 = label
            break
    return (x0, x1, path)


Lista_Completo_Tuplas = [path_to_tuple(path) for path in files]

num_cpus = psutil.cpu_count(logical=True)
ray.init(num_cpus=num_cpus, ignore_reinit_error=True, include_dashboard=False)
refs = [Leer_Datos_Carpetas.remote(tupla) for tupla in Lista_Completo_Tuplas]
pis = ray.get(refs)
pis = [a for a in pis if a if a != "error"]
Diccionario_Com = {k: v for element in pis for k, v in element.items()}
gc.collect()
while ray.is_initialized():
    ray.shutdown()
    print("Ray_Seguro")
del pis
del refs

Lista_Meses = ["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE",
               "NOVIEMBRE", "DICIEMBRE"]
Diccionario_Nombres_Tuplas = {}
for tupla in Diccionario_Com.keys():
    a = tupla[-1]
    for mes in Lista_Meses:
        if mes in a.upper():
            a = mes
            break
    Diccionario_Nombres_Tuplas[tupla] = (tupla[0], tupla[1], a)

Dicc_Datos_Productividad = {}
for tupla in Diccionario_Com.keys():
    a = Diccionario_Nombres_Tuplas[tupla]
    Dicc_Datos_Productividad[a] = Diccionario_Com[tupla]

Ruta_Guardar = "/home/tulipan1637/storage2/Misc/Velez/smart/192.168.81.150/smartdata/BASES HISTORICAS/Data_2/"
# with open("Dicc_Datos_Productividad" +".pickle", 'wb') as handle:
#     pickle.dump(Dicc_Datos_Productividad, handle, protocol=pickle.HIGHEST_PROTOCOL)


big = Dicc_Datos_Productividad
############################SHEET SELECTION##################################

# ***************************IN Acumulado... *********************************

acumulado = {k: value for k, value in big.items() if k[0] == 'Acumulado'}
tmo = {key: value['TMO'] for key, value in acumulado.items() if 'TMO' in value}
prod = {key: value['PRODUCTIVIDAD'] for key, value in acumulado.items() if 'PRODUCTIVIDAD' in value}
prod[('Acumulado', '2021', 'ENERO')] = acumulado[('Acumulado', '2021', 'ENERO')]['PRODUCTIVIAD']
gestion_whatsapp = {key: value[key2] for key, value in acumulado.items() for key2 in value \
                    if "ID_CUSTOMER" in value[key2].columns and 'WhatsApp' in key2}
gestion = {key: value[key2] for key, value in acumulado.items() for key2 in value \
           if "ID_CUSTOMER" in value[key2].columns and 'WhatsApp' not in key2}

# ***************************IN Encuesta... **********************************
encuesta = {k: value for k, value in big.items() if k[0] == 'Encuesta'}
detalle = {key: v['Detalle Encuestas'] for key, v in encuesta.items()}

# ***************************IN Planta... *********************************

planta = {k: value for k, value in big.items() if k[0] == 'Planta'}
hoja1 = {key: val['Hoja1'] for key, val in planta.items()}

for key in hoja1:
    col_map = {}
    for col in hoja1[key].columns:
        if col.startswith('CARTERA_') or col.startswith('TIPO_CARTERA_'):
            col_map[col] = 'CARTERA'
        if col.startswith('LIDER_'):
            col_map[col] = 'LIDER'
    hoja1[key] = hoja1[key].rename(columns=col_map)

cols_h1 = [set(value.columns) for value in hoja1.values()]
ch1_inter = set.intersection(*cols_h1)

for key in hoja1:
    hoja1[key] = hoja1[key][list(ch1_inter)]

#############################Wrap into a dict################################


### name ('Acumulado', dict, key name)
Dicc_Prod_Nuevo = dict()
fechas = [t[1:] for t in prod]
for fecha in fechas:
    Dicc_Prod_Nuevo[fecha] = dict()
    for name in [('Acumulado', tmo, 'TMO'), ('Acumulado', prod, 'PRODUCCION')]:
        Dicc_Prod_Nuevo[fecha][name[2]] = name[1][(name[0], fecha[0], fecha[1])]

fechas_detalle = {t[1:] for t in detalle}
for fecha in fechas_detalle:
    name = ('Encuesta', detalle, 'DETALLE_ENCUESTA')
    Dicc_Prod_Nuevo.setdefault(fecha, dict())
    Dicc_Prod_Nuevo[fecha][name[2]] = name[1][(name[0], fecha[0], fecha[1])]

fechas_planta = {t[1:] for t in planta}
for fecha in fechas_planta:
    name = ('Planta', planta, 'PERSONAL')
    Dicc_Prod_Nuevo.setdefault(fecha, dict())
    Dicc_Prod_Nuevo[fecha][name[2]] = name[1][(name[0], fecha[0], fecha[1])]

fechas_gestion = {t[1:] for t in gestion}
for fecha in fechas_gestion:
    name = ('Acumulado', gestion, 'GESTION')
    Dicc_Prod_Nuevo.setdefault(fecha, dict())
    Dicc_Prod_Nuevo[fecha][name[2]] = name[1][(name[0], fecha[0], fecha[1])]

fechas_gestionwp = {t[1:] for t in gestion_whatsapp}
for fecha in fechas_gestionwp:
    name = ('Acumulado', gestion_whatsapp, 'GESTION_WHATSAPP')
    Dicc_Prod_Nuevo.setdefault(fecha, dict())
    Dicc_Prod_Nuevo[fecha][name[2]] = name[1][(name[0], fecha[0], fecha[1])]


with open("Dicc_Datos_Productividad_Nuevo" +".pickle", 'wb') as handle:
    pickle.dump(Dicc_Prod_Nuevo, handle, protocol=pickle.HIGHEST_PROTOCOL)