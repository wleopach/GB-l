import os
import pickle

PATH = '/home/leopach/tulipan/GB/Base_de_Compras/Data_Mar_28/Data_2'

files = os.listdir(PATH)

paths = {'Pagos': f'{PATH}/Dicc_Datos_Propia_Pagos.pickle',
         'Evolucion': f'{PATH}/Dicc_Datos_Propia_Evolucion.pickle',
         'Asignacion': f'{PATH}/Dicc_Datos_Propia_Asignacion.pickle',
         'Cartera': f'/home/leopach/tulipan/GB/GB-l/Diccionario_Retornar.pickle'
         }

dicts = {}
for key in paths:
    with open(paths[key], 'rb') as f:
        dicts[key] = pickle.load(f)

# Create a set of keys for each dictionary
key_sets = [set(d.keys()) for d in dicts.values()]

# Find the intersection of all the key sets
intersection = set.intersection(*key_sets)
union = set.union(*key_sets)

cant_oblig = {}
for cc in intersection:
    cant_oblig[cc] = {}
    for key in dicts:
        if 'CANTIDAD_OBLIGACIONES' in dicts[key]['51709189'].keys():
            if dicts[key]['51709189']['CANTIDAD_OBLIGACIONES']!= 1:
                print(cc)
                break
            cant_oblig[cc][key] = dicts[key]['51709189']['CANTIDAD_OBLIGACIONES']
        else:
            cant = 0
            for key2 in dicts[key]['51709189']:
                cant += dicts[key]['51709189'][key2]['CANTIDAD_OBLIGACIONES']
            cant_oblig[cc][key] = cant
            if dicts[key]['51709189'][key2]['CANTIDAD_OBLIGACIONES']!= 1:
                print(cc)
                break


print(f"Hay {len(union)} cc's en total")
print(f"Hay {len(intersection)} cc's que existen en todos los dicts")
