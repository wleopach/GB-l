import re
import ray
import os
import json
import heapq
import hdbscan
import logging
import time
import joblib
import random
import pandas as pd
import numpy as np
import config
import csv
import copy
from denseclus import DenseClus
from sklearn.metrics import make_scorer
from sklearn.model_selection import RandomizedSearchCV
from plot import plot_join

random.seed(5)
np.random.seed(config.SEED)
PATH_TO_BP = 'Data/Best_Params'
PATH_TO_PLOTS = "Data/Plots"


def normalize(s) -> str:
    """
    replace vowels with spanish accents
    :param s: word with possible spanish accents
    :return: string without spanish accents
    """
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
    return s


def Corregir_Espacios(arg) -> (str):
    """
    Transform a string with spaces into a string without spaces
    :param arg: word with possible spaces
    :return: string without spaces
    """
    return (re.sub(" +", " ", arg).rstrip().lstrip())


def Limpieza_Final_Str(arg) -> (str):
    """
    Transform a string to upper case without spaces
    :param arg: word
    :return: string upper case without spaces
    """
    return (re.sub(" +", " ", normalize(arg)).rstrip().lstrip().upper())


def Confirmar_Archivo(nombre_archivo) -> bool:
    """
    Checks if the file extension is csv or xlsx
    :param nombre_archivo:
    :return:
    """
    try:
        if ".csv" in nombre_archivo or ".xlsx" in nombre_archivo:
            return (True)
        else:
            return (False)
    except:
        return (False)


def get_df(DF):
    """Retrives relevant  data from the .xlsx stored as values in a dictionary"""

    num_col = [len(DF[key].columns) for key in DF]
    heapq._heapify_max(num_col)
    max = heapq._heappop_max(num_col)
    for key in DF:
        if len(DF[key].columns) == max:
            return DF[key]

    return None


id_nom = {}  ### hashmap id-->Nombre

Dic_Obligacion = {}


def ed_basic_info(DF, sinon, req, oblig_data, sumar_saldo=False):
    """
    Fits the data of a DF to fill the Diccionario_Retornar.

    DF: pandas dataframe
    sinon: dictionary of synonyms
    req: set of required columns
    oblig_data: columns corresponding to an obligation
    sumar_saldo: determines whether or not to sum balances when there is no SALDO_TOTAL
    """
    DF.columns = DF.columns.str.strip()
    DF.columns = [Limpieza_Final_Str(j).replace(" ", "_") for j in list(DF.columns)]
    cols = set(DF.columns)
    for i in cols & req:
        DF[i] = DF[i].astype(str)
    keys = set(sinon.keys())
    for i in keys & cols:
        if sinon[i] not in cols:
            DF[sinon[i]] = DF[i]
        if i == 'NUMERO_DE_IDENTIFICACION':
            DF[sinon[i]] = DF[i]

    ##### if FECHA_CASTIGO_MIN ----> FECHA_CASTIGO= FECHA_CASTIGO_MIN
    if 'FECHA_CASTIGO_MIN' in set(DF.columns):
        DF['FECHA_CASTIGO'] = DF['FECHA_CASTIGO_MIN']

    ##### if not SALDO_TOTAL ==> SUM *SALDO
    if sumar_saldo and 'SALDO_TOTAL' not in set(DF.columns):
        DF['SALDO_TOTAL'] = DF.filter(regex='^SALDO').replace('[\$,.]', '', regex=True).astype(float).sum(axis=1)

    cols = set(DF.columns)
    ##### if not SALDO_CAPITAL_CLIENT ==> GROUPBY ID AND SUM SALDO_CAPITAL_VENDIDO
    if 'SALDO_CAPITAL_VENDIDO' in cols and 'SALDO_CAPITAL_CLIENTE' not in cols:
        qu = DF.groupby('IDENTIFICACION')['SALDO_CAPITAL_VENDIDO'].sum().reset_index()
        DF['SALDO_CAPITAL_CLIENTE'] = DF['IDENTIFICACION'].apply(lambda x: scv(qu, x))
    if DF.IDENTIFICACION.isna().any():
        DF['IDENTIFICACION'] = DF['IDENTIFICACION'].fillna(method='ffill')

    name_parts = {'PRIMER_NOMBRE', 'PRIMER_APELLIDO', 'SEGUNDO_APELLIDO'}
    if name_parts < cols:
        DF['NOMBRE'] = DF[list(name_parts)].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)

    cols = set(DF.columns)
    list_names = list(DF.filter(regex='^NOMBRE').columns)
    if list_names == ['NOMBRE_REC_ANTERIOR']:
        DF['NOMBRE'] = [None] * len(DF)
        list_names = []
    if list_names and 'NOMBRE' not in cols:
        if 'NOMBRE_TITULAR' in list_names:
            DF['NOMBRE'] = DF.pop('NOMBRE_TITULAR')

        else:
            DF['NOMBRE'] = DF.pop(list_names[0])

    DF.drop(columns=[col for col in DF if col not in req], inplace=True)
    for col in req - cols:
        DF[col] = [None] * len(DF)
    for col in req:
        DF[col] = DF[col].astype(str)

    DF.IDENTIFICACION = DF.IDENTIFICACION.apply(lambda v: v.replace('.0', ''))
    DF.NOMBRE = DF.NOMBRE.apply(lambda x: Limpieza_Final_Str(x))
    for row in range(len(DF)):
        names = id_nom.setdefault(DF['IDENTIFICACION'][row], [])
        if len(DF['NOMBRE'][row]) > 0 and DF['NOMBRE'][row] not in names:
            names.append(DF['NOMBRE'][row])
        history = Dic_Obligacion.setdefault(DF['OBLIGACION'][row], {})
        for atribute in oblig_data:
            history[atribute] = history.setdefault(atribute, set())
            history[atribute].add(DF[atribute][row])
    for i in cols & req:
        DF[i] = DF[i].astype(str)


# for key in Diccionario_Com:
#     DF = get_df(Diccionario_Com[key])
#     if 'SALDO_TOTAL' not in DF.filter(regex='SALDO').columns:
#         print(key)
#         print(DF.filter(regex='^SALDO').columns)

def scv(qu, id):
    """Returns result in a query by id"""
    return qu[qu['IDENTIFICACION'] == id]['SALDO_CAPITAL_VENDIDO'].values[0]


def check_cols(data):
    """Prints the name of the archives and the  None columns """
    nf_cols = {}
    for key in data:
        mask = data[key].astype(str).apply(lambda x: x.str.contains('None')).any(axis=0)
        result = data[key].loc[:5, mask]
        nf_cols[key] = set(result.columns)
    for k in nf_cols:
        print(f"IN {k.split('/')[-1]} : {nf_cols[k]}")


def tune_HDBSCAN(embedding, SEED, param_dist, n_iter_search=20):
    """Performs the hyperparameter tuning for HDBSCAN based on DBCV(ideal > 4.5)
     embedding = DensClus embedding
     SEED = random seed
     param_dist = dictionary with the params distribution
     n_iter_search = number of iterations through the collection
                     of all possible parameters combinations
     """
    np.random.seed(SEED)
    logging.captureWarnings(True)
    hdb = hdbscan.HDBSCAN(gen_min_span_tree=True, prediction_data=True).fit(embedding)

    # specify parameters and distributions to sample from

    # validity_scroer = "hdbscan__hdbscan___HDBSCAN__validity_index"
    validity_scorer = make_scorer(hdbscan.validity.validity_index, greater_is_better=True)

    random_search = RandomizedSearchCV(hdb
                                       , param_distributions=param_dist
                                       , n_iter=n_iter_search
                                       , scoring=validity_scorer
                                       , random_state=SEED)

    random_search.fit(embedding)

    with open(f"{PATH_TO_BP}/{file_name(random_search.best_params_)}", 'w') as f:
        f.write(f"Best Parameters {random_search.best_params_}\n")
        f.write(f"DBCV score :{random_search.best_estimator_.relative_validity_}\n")
    print(f"Best Parameters {random_search.best_params_}")
    print(f"DBCV score :{random_search.best_estimator_.relative_validity_}")
    DBCV = random_search.best_estimator_.relative_validity_
    best = hdbscan.HDBSCAN(**random_search.best_params_,
                           gen_min_span_tree=True, prediction_data=True).fit(embedding)

    if DBCV > 0.45:
        save_model(best, f"hdbscan{str(DBCV).split('.')[1]}")

    return random_search, best


def file_name(dictionary, ext='.txt'):
    """Returns a filename with the parameters of a model stored in a dictionary"""
    params_str = json.dumps(dictionary, sort_keys=True)
    filename = f"model_params_{params_str}{ext}"
    return filename


def fit_DenseClus(df, params):
    """Fits a DenseClus and returns all relevant information
        df = data
        params = dict with the prameters for the DenseClus

        returns -------------
        embedding =  transformed data points
        clustered = boolean vector decides if  not noise
        result = data frame with the embedding a and LABELS
        DBCV = score
        coverage = notNoise/total-points



    """
    np.random.seed(params['SEED'])
    clf = DenseClus(
        random_state=params['SEED'],
        cluster_selection_method=params['cluster_selection_method'],
        min_samples=params['min_samples'],
        n_components=params['n_components'],
        min_cluster_size=params['min_cluster_size'],
        umap_combine_method=params['umap_combine_method']

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
    DBCV = clf.hdbscan_.relative_validity_
    return embedding, clustered, result, DBCV, coverage, clf


def evaluate_clus(random_search, embedding, plot=False):
    """Evaluates the clusters produced by the best model found by random_search
       random_search = output of the RandomizedSearchCV

    """
    # evalute the clusters
    labels = random_search.best_estimator_.labels_
    clustered = (labels >= 0)

    coverage = np.sum(clustered) / embedding.shape[0]
    total_clusters = np.max(labels) + 1
    cluster_sizes = np.bincount(labels[clustered]).tolist()

    print(f"Percent of data retained: {coverage}")
    print(f"Total Clusters found: {total_clusters}")
    print(f"Cluster splits: {cluster_sizes}")

    if plot:
        plot_join(embedding[clustered, 0], embedding[clustered, 1], labels[clustered],
                  True, f"{PATH_TO_PLOTS}/slice1-{file_name(random_search.best_params_, '.png')}")
        plot_join(embedding[clustered, 1], embedding[clustered, 2], labels[clustered],
                  True, f"{PATH_TO_PLOTS}/slice2-{file_name(random_search.best_params_, '.png')}")
        plot_join(embedding[clustered, 0], embedding[clustered, 2], labels[clustered],
                  True, f"{PATH_TO_PLOTS}/slice3-{file_name(random_search.best_params_, '.png')}")


def predict_new(hdbscan_model, test_points):
    """
    Predicts approximated clusters for new points, given a fitted hdbscan
    :param hdbscan_model: fitted hdbscan, it might be clf.hdbscan_
    :param test_points: new points to be clustered
    :return: labels and strength
    """
    return hdbscan.approximate_predict(hdbscan_model, test_points)


def save_model(model, name):
    """
    saves a model as .joblib file
    :param model: fitted model, such as denseclus or hdbscan
    :param name: name of the file
    :return:
    """
    joblib.dump(model, filename=f"Data/Models/{name}.joblib")


def load_model(path_to_model):
    """
    loads a .joblib model
    :param path_to_model:path to the model
    :return: model
    """
    return joblib.load(path_to_model)


def min_max_scaler(column):
    """
    Min max scaler for a column
    :param column:
    :return: scaled column
    """
    return (column - column.min()) / (column.max() - column.min())


def z_score(column):
    mean = np.mean(column)
    std = np.std(column)
    return (column - mean) / std


def log_transform(column):
    return np.log(column)


Lista_A = ["ID", "FECHA", "CEDULA"]


@ray.remote
def Leer_Datos_Carpetas(tupla):
    archivo = tupla[-1]
    Diccionario_Retorno = dict()
    if ".csv" in archivo:
        try:
            Diccionario_Retorno[archivo] = pd.read_csv(archivo, sep=";")
            if len(Diccionario_Retorno[archivo].columns) <= 2:
                Diccionario_Retorno[archivo] = pd.read_csv(archivo, sep=",")
            try:
                all_columns = list(Diccionario_Retorno[archivo])
                Diccionario_Retorno[archivo][all_columns] = Diccionario_Retorno[archivo][all_columns].astype(str)
            except:
                pass
        except:
            Diccionario_Retorno[archivo] = pd.read_csv(archivo, sep=";", encoding="ISO-8859-1")
            if len(Diccionario_Retorno[archivo].columns) <= 2:
                Diccionario_Retorno[archivo] = pd.read_csv(archivo, sep=",", encoding="ISO-8859-1")
            try:
                all_columns = list(Diccionario_Retorno[archivo])
                Diccionario_Retorno[archivo][all_columns] = Diccionario_Retorno[archivo][all_columns].astype(str)
            except:
                pass

    elif "xlsx" in archivo:
        try:
            filepath = archivo
            df_dict = pd.read_excel(filepath, sheet_name=None, dtype='object')
            # Diccionario_Retorno[archivo.replace(".xlsx","")] = pd.read_excel(archivo)
            for key in df_dict.keys():
                if len(df_dict[key].columns) <= 2:
                    try:
                        s = csv.Sniffer()
                        separador = s.sniff(list(df_dict[key].iloc[0])[0]).delimiter
                        df_dict[key] = df_dict[key][df_dict[key].columns[0]].str.split(separador, expand=True)
                    except:
                        # del df_dict[key]
                        continue
                try:
                    for key in df_dict.keys():
                        if len(set([str(j).upper() for j in list(df_dict[key].columns)]).intersection(
                                set(Lista_A))) == 0:
                            for i in range(8):
                                if len(set([str(j).upper() for j in list(df_dict[key].columns)]).intersection(
                                        set(Lista_A))) > 0:
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
            Diccionario_Retorno = copy.deepcopy(df_dict)
            for key in Diccionario_Retorno.keys():
                try:
                    Diccionario_Retorno[key].columns = [normalize(Limpieza_Final_Str(str(arg))).replace(" ", "_") for
                                                        arg in list(Diccionario_Retorno[key].columns)]
                except:
                    pass
        except:
            pass
    else:
        Diccionario_Retorno["Error"].append(archivo)
    return ({tupla: Diccionario_Retorno})


def tables(path):
    """
    Given a path returns all csv or xlsx files contained in the file structure with root
    at path
    :param path:
    :return: list of paths to tables(csv or xlsx)
    """
    paths = []
    if os.path.isdir(path):
        for filename in os.listdir(path):
            childpath = os.path.join(path, filename)
            if re.search(r'\.csv$|\.xlsx$', filename):
                paths.append(childpath)
            elif os.path.isdir(childpath):
                paths += tables(childpath)
    return paths

def mermaid(path):
    if os.path.isdir(path):
        for filename in os.listdir(path):

            childpath = os.path.join(path, filename)
            with open(f"tree.txt", 'a') as file:
                source = path.split('/')[-1].replace(' ', '')
                target = normalize(filename.replace(' ', ''))
                target = target.replace('(1)','')
                if ".csv" in target or ".xlsx" in target:
                    target = f"file{hash(target)}[({target})]"

                file.write(f"{source}-->{target}\n")
            if os.path.isdir(childpath) and os.listdir(childpath):
                mermaid(childpath)
