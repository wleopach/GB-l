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

id_nom = {}  ### hashmap id-->Nombre
data = {}  ### hashmap NombreArchivo-->Data
for key in Diccionario_Com:
    data[key] = get_df(Diccionario_Com[key])
    ed_basic_info(data[key])
    list_names = list(data[key].filter(regex='^NOMBRE').columns)
    if list_names:
        data[key]['NOMBRE'] = data[key].pop(list_names[0])
        print(key)
        for row in range(len(data[key])):
            names = id_nom.setdefault(data[key]['IDENTIFICACION'][row], [])
            if len(data[key]['NOMBRE'][row]) > 0 and data[key]['NOMBRE'][row] not in names:
                names.append(data[key]['NOMBRE'][row])
                # print(len(names))

# DF.columns = [Limpieza_Final_Str(j).replace(" ","_") for j in list(DF.columns)]
# DF.IDENTIFICACION = DF.IDENTIFICACION.astype(str)
# #DF.NOMBRE = DF.NOMBRE.astype(str)
# DF.OBLIGACION = DF.OBLIGACION.astype(str)
#
# DF.NOMBRE = DF.NOMBRE.apply(lambda x: Limpieza_Final_Str(x))

ident = "94373197"

Diccionario_Retornar = dict()

# dato_ident = DF.query("IDENTIFICACION == @ident")

Num_Obligaciones = {}  # hashmap Nombre_Archivo --> Num Obligaciones {identificacion: #obligaciones}
List_Obligaciones = {}
for Nombre_Archivo in data:
    try:

        d_aux = data[Nombre_Archivo][['IDENTIFICACION',
                                      'OBLIGACION']].drop_duplicates()
        d_aux1 = d_aux.groupby(['IDENTIFICACION']).nunique()
        Num_Obligaciones[Nombre_Archivo] = d_aux1.to_dict()['OBLIGACION']
        List_Obligaciones[Nombre_Archivo] = d_aux.groupby('IDENTIFICACION')['OBLIGACION'].apply(list)
    except:

        raise NoExiste(f'No se ha encontrado Obligaciones en el archivo{Nombre_Archivo}')

# print(Num_Obligaciones)
# Diccionario_Retornar[ident][Nombre_Archivo]["Cantidad_Obligaciones"] = len(dato_ident.OBLIGACION.drop_duplicates())
# try:
#     Diccionario_Retornar[ident][Nombre_Archivo]["NOMBRE"] = stats.mode(dato_ident.NOMBRE.tolist())
# except:
#     Diccionario_Retornar[ident][Nombre_Archivo]["NOMBRE"] = dato_ident.NOMBRE.tolist()[0]
#
#
# Diccionario_Retornar[ident][Nombre_Archivo]["OBLIGACIONES"] = dato_ident.OBLIGACION.drop_duplicates().tolist()
# def Encontrar_Info_Obligacion(Ob):
#     try:
#         dato_ob = dato_ident.query("OBLIGACION == @ob")
#         dic_return = {}
#         dic_return["FECHA_APERTURA"] = dato_ob.FECHA_APERTURA.tolist()[0]
#         dic_return["SALDO_TOTAL"] = dato_ob.SALDO_TOTAL.tolist()[0]
#         dic_return["CUPO_APROBADO"] = dato_ob.CUPO_APROBADO.tolist()[0]
#         dic_return["FECHA_CASTIGO"] = dato_ob.FECHA_CASTIGO.tolist()[0]
#         dic_return["DIAS_MORA_ACTUAL"] = dato_ob.DIAS_MORA_ACTUAL.tolist()[0]
#         dic_return["TASA_INTERES_CTE"] = dato_ob.TASA_INTERES_CTE.tolist()[0]
#         dic_return["CUOTA"] = dato_ob.CUOTA.tolist()[0]
#         dic_return["DESCRIPCION_CONVENIO_CLIENTE"] = dato_ob.DESCRIPCION_CONVENIO_CLIENTE.tolist()[0]
#         dic_return["DESCRIPCION_PRODUCTO"] = dato_ob.DESCRIPCION_PRODUCTO.tolist()[0]
#         dic_return["SALDO_CAPITAL_CLIENTE"] = dato_ob.SALDO_CAPITAL_CLIENTE.tolist()[0]
#
#         return(dic_return)
#     except:
#        return("ERROR")
#
#
# for ob in Diccionario_Retornar[ident][Nombre_Archivo]["OBLIGACIONES"]:
#     Diccionario_Retornar[ident][Nombre_Archivo][ob] = Encontrar_Info_Obligacion(ob)
#
#
#
#
#
#
#
#
#
#
# with open(RT +"Diccionario_Com.pickle", 'wb') as handle:
#     pickle.dump(Diccionario_Com, handle, protocol=pickle.HIGHEST_PROTOCOL)
#
