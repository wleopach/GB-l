from data_reader import load_dicts

load = {'Evolucion'}
evol = load_dicts(load)['Evolucion']

com = {'IDENTIFICACION', 'CANTIDAD_OBLIGACIONES', 'NOMBRE'}
no_pagos = {key:value[key2]['DATOS'] for key,value in evol.items()\
            for key2 in value if key2 not in com and value[key2]['TOTAL_PAGOS']==0}
pagos = {key:value[key2]['DATOS'] for key,value in evol.items() \
         for key2 in value if key2 not in com and value[key2]['TOTAL_PAGOS']!=0}

