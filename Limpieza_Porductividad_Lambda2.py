
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
    for label in labels:
        if label in path:
            x0 = label
    return (x0, x1, path)


Lista_Completo_Tuplas = [path_to_tuple(path) for path in files]

num_cpus = psutil.cpu_count(logical=True)
ray.init(num_cpus=num_cpus, ignore_reinit_error=True,include_dashboard = False)
refs = [Leer_Datos_Carpetas.remote(tupla) for tupla in Lista_Completo_Tuplas]
pis = ray.get(refs)
pis = [a for a in pis if a if a!= "error"]
Diccionario_Com = {k:v for element in pis for k,v in element.items()}
gc.collect()
while ray.is_initialized():
    ray.shutdown()
    print("Ray_Seguro")
del pis
del refs

Lista_Meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
Diccionario_Nombres_Tuplas = {}
for tupla in Diccionario_Com.keys():
    a = tupla[-1]
    for mes in Lista_Meses:
        if mes in a.upper():
            a = mes
            break
    Diccionario_Nombres_Tuplas[tupla] = (tupla[0],tupla[1],a)

Dicc_Datos_Productividad = {}
for tupla in Diccionario_Com.keys():
    a = Diccionario_Nombres_Tuplas[tupla]
    Dicc_Datos_Productividad[a] = Diccionario_Com[tupla]

Ruta_Guardar ="/home/tulipan1637/storage2/Misc/Velez/smart/192.168.81.150/smartdata/BASES HISTORICAS/Data_2/"
with open("Dicc_Datos_Productividad" +".pickle", 'wb') as handle:
    pickle.dump(Dicc_Datos_Productividad, handle, protocol=pickle.HIGHEST_PROTOCOL)
