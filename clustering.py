import random
import numpy as np
import config
from data_reader import load_dicts, load_csv, paths
from utils import tune_HDBSCAN, fit_DenseClus, evaluate_clus, predict_new
from plot import plot_join
import joblib
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

df.SALDO_CAPITAL_CLIENTE = df.SALDO_CAPITAL_CLIENTE.astype(int)

df['CALIDAD_DATOS'] = [1] * len(df)

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

  # set the random seed as best we can

params = dict()
params['cluster_selection_method'] = "eom"
params['min_samples'] = 90
params['n_components'] = 3
params['min_cluster_size'] = 2200
params['umap_combine_method'] = "intersection_union_mapper"
params['SEED'] = config.SEED
embedding, clustered, result, DBCV, coverage, clf = fit_DenseClus(df, params)

plot_join(embedding[clustered, 0], embedding[clustered, 1], result['LABELS'][clustered])
plot_join(embedding[clustered, 1], embedding[clustered, 2], result['LABELS'][clustered])
plot_join(embedding[clustered, 0], embedding[clustered, 2], result['LABELS'][clustered])

categorical = df.select_dtypes(include=["object"])
df["SEGMENT"] = result['LABELS']
numerics = df.select_dtypes(include=[int, float]).drop(["SEGMENT"], 1).columns.tolist()

df['IDENTIFICACION'] = df0['IDENTIFICACION']
df['ID_TABLA'] = df0['ID_TABLA']
dfA = df.loc[df['ID_TABLA'] == 'Evolucion_2022_OCTUBRE']
mm = dfA.index
print((dfA['SEGMENT'] > -1).sum(), ' cartera_en segmento')

medianas = df[numerics + ["SEGMENT"]].groupby(["SEGMENT"]).median()
medianas1 = medianas[1:]

cu75 = df[numerics + ["SEGMENT"]].groupby(["SEGMENT"]).quantile(0.75)
cu751 = cu75[1:]
cu25 = df[numerics + ["SEGMENT"]].groupby(["SEGMENT"]).quantile(0.25)
cu251 = cu25[1:]

print(f"DBCV score {DBCV}")
total_clusters = result['LABELS'].max() + 1
cluster_sizes = np.bincount(result['LABELS'][clustered]).tolist()

print(f"Percent of data retained: {coverage}")
print(f"Total Clusters found: {total_clusters}")
print(f"Cluster splits: {cluster_sizes}")

######################### HDBSCAN Hyperparameter tunning#######################################
param_dist = {'min_samples': [10, 9, 8],
              'min_cluster_size': [5000, 1200, 3000],
              'cluster_selection_method': ['eom', 'leaf'],
              'metric': ['euclidean', 'minkowski'],
              'p': [2]
              }

random_search, best = tune_HDBSCAN(embedding, config.SEED, param_dist, 20)

evaluate_clus(random_search, embedding, True)


test_points = np.random.random(size=(50, 3)) *  10

predict_new(best, test_points)

predict_new(clf.hdbscan_, test_points)


filename = 'model.joblib'
joblib.dump(clf, filename)

clf2 = joblib.load('model.joblib')
predict_new(clf2.hdbscan_, test_points)