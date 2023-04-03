import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import seaborn as sns
from denseclus import DenseClus
from data_reader import load_dicts, load_csv, paths
from utils import tune_HDBSCAN
from plot import plot_join
###### Path to clustering data
PATH = '/home/leopach/tulipan/GB/Base_de_Compras/Data_Mar_28/Data_2/evolucion_pagos2.csv'

##### Set of keys to load
load = {'Cartera'}

##### Dicts load
dicts = load_dicts(load)

cartera = dicts['Cartera']

##### Set of columns to load from csv
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

SEED = 42
np.random.seed(SEED)  # set the random seed as best we can


mcs = 2200
clf = DenseClus(
    cluster_selection_method="eom",
    min_samples=90,
    n_components=3,
    min_cluster_size=mcs,
    umap_combine_method="intersection_union_mapper",
    random_state=SEED
    # metric='euclidean'
)

start = time.time()
clf.fit(df)
print('time fitting ', (time.time() - start) / 60)
print(clf.n_components)

embedding = clf.mapper_.embedding_
labels = clf.score()

result = pd.DataFrame(clf.mapper_.embedding_)
result['LABELS'] = pd.Series(clf.score())
print('clusters ', len(set(result['LABELS'])) - 1)

lab_count = result['LABELS'].value_counts()
lab_count.name = 'LABEL_COUNT'

lab_normalized = result['LABELS'].value_counts(normalize=True)
lab_normalized.name = 'LABEL_PROPORTION'
print('ruido ', lab_normalized[-1])

clustered = result['LABELS'] >= 0
cnts = pd.DataFrame(clf.score())[0].value_counts()
cnts = cnts.reset_index()
cnts.columns = ['CLUSTER', 'COUNT']
print(cnts.sort_values(['CLUSTER']))

coverage = np.sum(clustered) / clf.mapper_.embedding_.shape[0]
print(f"Coverage {coverage}")

categorical = df.select_dtypes(include=["object"])
df["SEGMENT"] = clf.score()
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

print(f"DBCV score {clf.hdbscan_.relative_validity_}")
total_clusters = result['LABELS'].max() + 1
cluster_sizes = np.bincount(result['LABELS'][clustered]).tolist()

print(f"Percent of data retained: {coverage}")
print(f"Total Clusters found: {total_clusters}")
print(f"Cluster splits: {cluster_sizes}")

random_search = tune_HDBSCAN(embedding, SEED, 50)

# evalute the clusters
labels = random_search.best_estimator_.labels_
clustered = (labels >= 0)

coverage = np.sum(clustered) / embedding.shape[0]
total_clusters = np.max(labels) + 1
cluster_sizes = np.bincount(labels[clustered]).tolist()

print(f"Percent of data retained: {coverage}")
print(f"Total Clusters found: {total_clusters}")
print(f"Cluster splits: {cluster_sizes}")

plot_join(embedding[clustered, 0],embedding[clustered, 1], labels[clustered])
plot_join(embedding[clustered, 1],embedding[clustered, 2], labels[clustered])



