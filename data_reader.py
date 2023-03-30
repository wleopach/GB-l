import os
import pickle
import pandas as pd

PATH = '/home/leopach/tulipan/GB/Base_de_Compras/Data_Mar_28/Data_2'

files = os.listdir(PATH)

paths = {'Pagos': f'{PATH}/Dicc_Datos_Propia_Pagos.pickle',
         'Evolucion': f'{PATH}/Dicc_Datos_Propia_Evolucion.pickle',
         'Asignacion': f'{PATH}/Dicc_Datos_Propia_Asignacion.pickle',
         'Cartera': f'/home/leopach/tulipan/GB/GB-l/Diccionario_Retornar.pickle'
         }


def load_dicts(load, inter=False, union=False):
    """Loads the dicts stored in the set load, and returns them in dicts """
    dicts = {}
    for key in load:
        with open(paths[key], 'rb') as f:
            dicts[key] = pickle.load(f)

    if inter | union:
        # Create a set of keys for each dictionary
        key_sets = [set(d.keys()) for d in dicts.values()]
        # Find the intersection of all the key sets
        if inter:
            print(f"the key intersection size is {len(set.intersection(*key_sets))}")

        if union:
            print(f"the key union size is {len(set.union(*key_sets))}")
    return dicts


# cant_oblig = {}
# for cc in intersection:
#     cant_oblig[cc] = {}
#     for key in dicts:
#         if 'CANTIDAD_OBLIGACIONES' in dicts[key]['51709189'].keys():
#             if dicts[key]['51709189']['CANTIDAD_OBLIGACIONES']!= 1:
#                 print(cc)
#                 break
#             cant_oblig[cc][key] = dicts[key]['51709189']['CANTIDAD_OBLIGACIONES']
#         else:
#             cant = 0
#             for key2 in dicts[key]['51709189']:
#                 cant += dicts[key]['51709189'][key2]['CANTIDAD_OBLIGACIONES']
#             cant_oblig[cc][key] = cant
#             if dicts[key]['51709189'][key2]['CANTIDAD_OBLIGACIONES']!= 1:
#                 print(cc)
#                 break
#
#

# for key in dicts:
#     if key !='Asignacion' and set(dicts[key].keys())-set(dicts['Asignacion'].keys()) != set():
#         df = pd.Series(dicts[key].keys())
#         df.to_csv(f'{key}-Asignacion.csv')

load = {'Evolucion'}

dicts = load_dicts(load)

comp = {'IDENTIFICACION', 'CANTIDAD_OBLIGACIONES', 'NOMBRE'}

des = []
evol = dicts['Evolucion']
for key in evol:
    for key2 in evol[key]:
        if key2 not in comp:
            des.append(evol[key][key2]['DESCRIPCION_PRODUCTO'])

des = pd.Series(des)
des.to_csv('des_raw_evol.csv')
