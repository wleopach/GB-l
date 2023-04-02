from data_reader import load_dicts, load_csv, paths
import pandas as pd
import numpy as np
import time
import seaborn as sns
from denseclus import DenseClus
import matplotlib.pyplot as plt

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

# clf = DenseClus(
#     random_state=SEED,
#     cluster_selection_method="leaf",
#     min_cluster_size=5,
#     umap_combine_method="intersection_union_mapper",
# )
mcs = 2200
clf = DenseClus(
    cluster_selection_method="eom",
    min_samples=90,
    n_components=3,
    min_cluster_size=mcs,
    umap_combine_method="intersection_union_mapper",
    # metric='euclidean'
)

start = time.time()
clf.fit(df)
print('time fitting ', (time.time() - start) / 60)
print(clf.n_components)

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


####PLOTS
_ = sns.jointplot(
    x=clf.mapper_.embedding_[:, 0], y=clf.mapper_.embedding_[:, -1], kind="kde"
)
# plt.savefig(path_data + '/clustering/jointplot2.png')
plt.show()

_ = sns.jointplot(
    x=clf.mapper_.embedding_[:, 0],
    y=clf.mapper_.embedding_[:, 1],
    hue=labels,
    kind="kde",
)
plt.show()
# plt.savefig(path_data + '/clustering/jointplot2.png')

_ = clf.hdbscan_.condensed_tree_.plot(
    select_clusters=True,
    selection_palette=sns.color_palette("deep", np.unique(labels).shape[0]),
)
plt.show()

dic_cat = {}
for c in categorical.columns:
    # todos los valores de la categoria c frente a cada segmento
    g = df.groupby(["SEGMENT"] + [c]).size()
    # g.plot(
    #     kind="bar", color=sns.color_palette("deep", np.unique(labels).shape[0])
    # )
    # plt.title(c)
    # plt.savefig(path_data + '/'+c+'.png')
    # plt.show()
    # plt.figure(figsize=(30,8))
    # todos los valores de la categoria c frente a la clase 1
    m0 = g[0]
    m0 = m0 / sum(m0)
    m0.name = str(0)
    m0 = m0.to_frame()
    for k in [-1] + list(range(1, max(df['SEGMENT']) + 1)):
        m1 = g[k]
        m1 = m1 / sum(m1)
        m1.name = str(k)
        m1 = m1.to_frame()
        m0 = m0.join(m1, how='outer')
    m0.reset_index(inplace=True)
    dic_cat[c] = m0
    g[k].plot(
        kind="bar", color=sns.color_palette("deep", np.unique(labels).shape[0])
    )
    plt.title(c + ' clase ' + str(k))
    plt.show()
