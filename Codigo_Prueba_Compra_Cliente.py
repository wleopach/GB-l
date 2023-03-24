# %%
import os

import glob
import pandas as pd
import csv
import pickle
import ray
import psutil
import gc
import statistics as stats
from utils import *


class NoExiste(Exception):
    pass


# %%
# RT = "/media/tulipan1637/Nuevo vol/Documentos/Cobranzas_Beta/BASES HISTORICAS/Propia/Propia/Base_de_Compras/"
RT = "/home/leopach/tulipan/GB/GB-l/data"
# %%
Dict_Tuplas = dict()
Lista_Directorios = list(set([x[0] for x in os.walk(RT)]))
Dicc_Archivos = {}
Dicc_Archivos["Base_de_Compras"] = glob.glob(Lista_Directorios[0] + '/*')


# %%

# %%
def Corregir_Espacios(arg):
    return (re.sub(" +", " ", arg).rstrip().lstrip())


# %%
# %%

# %%
def Confirmar_Archivo(nombre_archivo):
    try:
        if ".csv" in nombre_archivo or ".xlsx" in nombre_archivo:
            return (True)
        else:
            return (False)
    except:
        return (False)


Dicc_Archivos = {j: Dicc_Archivos[j] for j in Dicc_Archivos.keys() if len(Dicc_Archivos[j]) > 0 if
                 len([i for i in Dicc_Archivos[j] if Confirmar_Archivo(i)]) > 0}


# %%
@ray.remote
def Leer_Datos_Carpetas(tupla):
    try:
        archivo = tupla
        Diccionario_Retorno = {}
        if "xlsx" in archivo:
            try:
                filepath = archivo
                df_dict = pd.read_excel(filepath, sheet_name=None)
                # Diccionario_Retorno[archivo.replace(".xlsx","")] = pd.read_excel(archivo)
                for key in df_dict.keys():
                    if len(df_dict[key].columns) <= 2:
                        s = csv.Sniffer()
                        separador = s.sniff(list(df_dict[key].iloc[0])[0]).delimiter
                        df_dict[key] = df_dict[key][df_dict[key].columns[0]].str.split(separador, expand=True)
                    try:
                        for key in df_dict.keys():
                            if len([str(j).upper() for j in list(df_dict[key].columns) if
                                    "FECHA" in str(j).upper()]) == 0:
                                for i in range(8):
                                    if len([str(j).upper() for j in list(df_dict[key].iloc[i]) if
                                            "FECHA" in str(j).upper()]) > 0:
                                        columnas = list(df_dict[key].iloc[i])
                                        df_dict[key] = df_dict[key].iloc[i:]
                                        df_dict[key].columns = columnas
                                        break
                    except:
                        pass
                    try:
                        all_columns = list(df_dict[key])
                        df_dict[key][all_columns] = df_dict[key][all_columns].astype(str)
                    except:
                        pass
                Diccionario_Retorno = df_dict
                for key in Diccionario_Retorno.keys():
                    try:
                        Diccionario_Retorno[key].columns = [normalize(Limpieza_Final_Str(str(arg))).replace(" ", "_")
                                                            for arg in list(Diccionario_Retorno[key].columns)]
                    except:
                        pass
            except:
                pass
        else:
            Diccionario_Retorno["Error"].append(archivo)
        return ({tupla: Diccionario_Retorno})
    except:
        pass


# %%+
num_cpus = psutil.cpu_count(logical=True)
ray.init(num_cpus=num_cpus, ignore_reinit_error=True, include_dashboard=False)
refs = [Leer_Datos_Carpetas.remote(tupla) for tupla in list(Dicc_Archivos.values())[0]]
pis = ray.get(refs)
pis = [a for a in pis if a if a != "error"]
Diccionario_Com = {k: v for element in pis for k, v in element.items()}
gc.collect()
while ray.is_initialized():
    ray.shutdown()
    print("Ray_Seguro")

# %%
Diccionario_Info_Clientes_Compra = {
    "1136883086": {"Nombre": "Santiago Gutierrez", "Lista_Obligaciones": [8888888, 99999999],
                   "888888": {"Fecha_Apertura": "Enreo"}}}

# %%%

key = list(Diccionario_Com.keys())[-1]
Nombre_Archivo = key.split("/")[-1]

data = {}  ### hashmap NombreArchivo--> processed data frame

#hashmap sinonimo---->nombre final
sinon = {'DIAS_DE_MORA': 'DIAS_MORA_ACTUAL',
         'SALDO_CAPITAL': 'SALDO_CAPITAL_VENDIDO',
         'SALDO_DE_CAPITAL_TOTAL_EN_PESOS': 'SALDO_CAPITAL_VENDIDO',
         'SALDO_TTAL_PROD': 'SALDO_TOTAL',
         #'SALDO_CUOTA_MANEJO': 'CUOTA',
         'CONTRATO': 'OBLIGACION',
         'OBLIGACION16': 'OBLIGACION',
         'NUMERO_DE_INTIFICACION': 'IDENTIFICACION',
         'NOMBRE_TITULAR': 'NOMBRE'

         }
#hashset columnas requeridas
req = {'IDENTIFICACION', 'NOMBRE', 'OBLIGACION', 'SALDO_CAPITAL_CLIENTE', 'FECHA_APERTURA', 'SALDO_TOTAL',
       'CUPO_APROBADO', 'FECHA_CASTIGO', 'DIAS_MORA_ACTUAL', 'TASA_INTERES_CTE',
       'CUOTA', 'DESCRIPCION_CONVENIO_CLIENTE', 'DESCRIPCION_PRODUCTO', 'SALDO_CAPITAL_VENDIDO'}

#hashset columnas requeridas para la obligacion
oblig_data = {'SALDO_CAPITAL_CLIENTE', 'FECHA_APERTURA', 'SALDO_TOTAL',
              'CUPO_APROBADO', 'FECHA_CASTIGO', 'DIAS_MORA_ACTUAL', 'TASA_INTERES_CTE',
              'CUOTA', 'DESCRIPCION_CONVENIO_CLIENTE', 'DESCRIPCION_PRODUCTO', 'SALDO_CAPITAL_VENDIDO'}

for key in Diccionario_Com:
    data[key] = get_df(Diccionario_Com[key]).copy()
    ed_basic_info(data[key], sinon, req, oblig_data, True)
    print(key)



Diccionario_Retornar = dict()


Num_Obligaciones = {}  # hashmap Nombre_Archivo --> Num Obligaciones {identificacion: #obligaciones}
List_Obligaciones = {}

for Nombre_Archivo in data:
    try:
        # Obligaciones by identificacion
        d_aux = data[Nombre_Archivo][['IDENTIFICACION',
                                      'OBLIGACION']].drop_duplicates()
        d_aux1 = d_aux.groupby(['IDENTIFICACION']).nunique()
        Num_Obligaciones[Nombre_Archivo] = d_aux1.to_dict()['OBLIGACION']
        List_Obligaciones[Nombre_Archivo] = d_aux.groupby('IDENTIFICACION')['OBLIGACION'].apply(list)


    except:

        raise NoExiste(f'No se ha encontrado Obligaciones en el archivo{Nombre_Archivo}')

for ident in id_nom:
    Diccionario_Retornar[ident] = {}
    for Nombre_Archivo in Num_Obligaciones:
        Diccionario_Retornar[ident][Nombre_Archivo] = {}
        if ident in Num_Obligaciones[Nombre_Archivo]:
            Diccionario_Retornar[ident][Nombre_Archivo]["CANTIDAD_OBLIGACIONES"] = Num_Obligaciones[Nombre_Archivo][
                ident]
            Diccionario_Retornar[ident][Nombre_Archivo]["OBLIGACIONES"] = List_Obligaciones[Nombre_Archivo][
                ident]
            try:
                Diccionario_Retornar[ident][Nombre_Archivo]["NOMBRE"] = stats.mode(id_nom[ident])
            except:
                Diccionario_Retornar[ident][Nombre_Archivo]["NOMBRE"] = id_nom[ident][0]

            for ob in Diccionario_Retornar[ident][Nombre_Archivo]["OBLIGACIONES"]:
                Diccionario_Retornar[ident][Nombre_Archivo][ob] = Dic_Obligacion[ob]


#Diccionario_Retornar
#
#
#
#
#
#
#
#
with open(RT +"Diccionario_Retornar.pickle", 'wb') as handle:
    pickle.dump(Diccionario_Retornar, handle, protocol=pickle.HIGHEST_PROTOCOL)
#
