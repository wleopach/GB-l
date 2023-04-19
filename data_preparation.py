import random
import numpy as np
import pandas as pd
import config
from data_reader import load_dicts, load_csv

random.seed(5)
np.random.seed(config.SEED)
###### Path to clustering data
PATH = '/home/leopach/tulipan/GB/Base_de_Compras/Data_Mar_28/Data_2/evolucion_pagos2.csv'

##### Set of keys to load
load = {'Cartera'}

##### Dicts load
dicts = load_dicts(load)

cartera = dicts['Cartera']

##### Set of columns to load from evolucion_pagos2.csv
cols_input = {'SALDO_CAPITAL_CLIENTE',
              'PLAZO_INICIAL_ADJ',
              'MESES_INICIALES_NO_PAGO',
              'DIAS_DE_MORA_ACTUAL',
              'PORCION_PAGO',
              'PORCION_PAGOS',
              'CP',
              'ACCION_CONTACTO',
              'MOTIVO',
              'PORTAFOLIO'}

cols = cols_input | {'IDENTIFICACION', 'ID_TABLA'}

df0 = load_csv(PATH, cols)
df0 = df0[df0['SALDO_CAPITAL_CLIENTE'] != 90]

##### Find intersection
ids_cartera = set(cartera.keys())
ids_csv = set(df0['IDENTIFICACION'].unique())
ids_csv = set(df0['IDENTIFICACION'].astype(str).unique())
intersection = ids_cartera & ids_csv

##### Calculate mean of TASA_INTERES_CORRIENTE
tic = []
for key in cartera:
    for key2 in cartera[key]:
        if 'BBVA' not in key2:
            for key3 in cartera[key][key2]:
                if key3 not in {'CANTIDAD_OBLIGACIONES', 'NOMBRE','OBLIGACIONES'}:
                    tic+=list(cartera[key][key2][key3]['TASA_INTERES_CTE'])

tic = [float(x) for x in tic if x != 'None']
tic = pd.DataFrame(tic)
mean = tic.mean()

for key in cartera:
    for key2 in cartera[key]:
        if 'BBVA' in key2:
            for key3 in cartera[key][key2]:
                if key3 not in {'CANTIDAD_OBLIGACIONES', 'NOMBRE','OBLIGACIONES'}:
                    cartera[key][key2][key3]['TASA_INTERES_CTE'] = {mean[0]}

ti_dict = {}
for cc in intersection:
    ti_list = []
    for key in cartera[cc]:

        for key2 in (set(cartera[cc][key]) - {'CANTIDAD_OBLIGACIONES', 'OBLIGACIONES', 'NOMBRE'}):

            val = cartera[cc][key][key2]['TASA_INTERES_CTE'].pop()
            if val != 'None':
                ti_list += [float(val)]

    if ti_list:
        ti_dict[cc] = sum(ti_list) / len(ti_list)

intersection = set(ti_dict.keys())

##### Filter the Dataframe by IDENTIFICACION VALUES in the intersection with TI_MEAN
df0 = df0[df0['IDENTIFICACION'].astype(str).isin(intersection)]
df0['TI_MEAN'] = df0['IDENTIFICACION'].apply(lambda cc: ti_dict[str(cc)])
df0.dropna(inplace=True)
df = df0[list(cols_input) + ['TI_MEAN']].copy()
#df = df0[list(cols_input)].copy()

df.SALDO_CAPITAL_CLIENTE = df.SALDO_CAPITAL_CLIENTE.astype(int)

# df['CALIDAD_DATOS'] = [1] * len(df)

df.PORTAFOLIO = df.PORTAFOLIO.apply(lambda x: x.replace(' - NAN', ''))
df.PORTAFOLIO = df.PORTAFOLIO.apply(lambda x: x.replace('NAN - ', ''))

acpk = ['ACPK BETA - ACUERDO DE PAGO CART CASTIGADA',
        'ACPK - ACUERDO DE PAGO CART CASTIGADA',
        'ACPK BETA - ACUERDO PAGO CAST',
        'ACUERDO DE PAGO CART CASTIGADA - ACUERDO PAGO CAST',
        'ACUERDO DE PAGO CART CASTIGADA',
        'ACUERDO DE PAGO CASTIG CON DTO',
        'ACUERDO PAGO',
        'ACUERDO DE PAGO CART CAST CONV']

for c in acpk:
    df.PORTAFOLIO = df.PORTAFOLIO.apply(lambda x: x.replace(c, 'ACPK BETA'))

# add some Gaussian noise with mean 0 and standard deviation [0.1, 0.1, 0.1, 0.001, 1000, 0.001, 0.01]
numerics = df.select_dtypes(include=[int, float]).columns.tolist()
noise = np.random.normal(loc=0, scale=[0.1, 0.1, 0.1, 0.001, 1000, 0.001, 0.01], size=df[numerics].shape)

df[numerics] = df[numerics] + noise