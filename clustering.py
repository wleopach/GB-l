import numpy as np
import config
from tqdm import tqdm
from utils import tune_HDBSCAN, fit_DenseClus, evaluate_clus, predict_new, save_model
from plot import plot_join, plot_tree
from data_preparation import df, df0
from clustering_analysis import Analysis

np.random.seed(config.SEED)

########################################Set params for DenseClus####################
params = dict()
params['cluster_selection_method'] = "eom"
params['min_samples'] = 4
params['n_components'] = 3
params['min_cluster_size'] = 3000
params['umap_combine_method'] = "intersection_union_mapper"
params['SEED'] = None
DBCV = -1

progress_bar = tqdm(desc='Fitting DenseClus')

while DBCV < 0.45:
    # call the fit_DenseClus function
    embedding, clustered, result, DBCV, coverage, clf = fit_DenseClus(df, params)

    # print the parameters of the classifier
    print(clf.get_params())
    print(DBCV)

    # update the progress bar
    progress_bar.update(1)

    # close the progress bar
progress_bar.close()
result.to_csv(f'Data/Clustering_Output/results.csv')
df0.to_csv('Data/Clustering_Output/input.csv')
cl_analysis = Analysis(df0, result["LABELS"])
##########################################Plot bivariate and tree ###############################
plot_join(embedding[clustered, 0], embedding[clustered, 1], result['LABELS'][clustered])
plot_join(embedding[clustered, 1], embedding[clustered, 2], result['LABELS'][clustered])
plot_join(embedding[clustered, 0], embedding[clustered, 2], result['LABELS'][clustered])
plot_tree(clf, result['LABELS'])


save_model(clf, f"clf{str(DBCV).split('.')[1]}")
total_clusters = result['LABELS'].max() + 1
cluster_sizes = np.bincount(result['LABELS'][clustered]).tolist()

print(f"Percent of data retained: {coverage}")
print(f"Total Clusters found: {total_clusters}")
print(f"Cluster splits: {cluster_sizes}")

######################### HDBSCAN Hyperparameter tunning#######################################
param_dist = {'min_samples': [7, 8, 9],
              'min_cluster_size': [15000, 5000, 3000],
              'cluster_selection_method': ['eom', 'leaf'],
              'metric': ['euclidean', 'minkowski', 'manhattan'],
              'p': [2]
              }

random_search, best = tune_HDBSCAN(embedding, config.SEED, param_dist, 20)

evaluate_clus(random_search, embedding, True)

#########Compare DensClus with  random_search and choose the best#####################

validity_= random_search.best_estimator_.relative_validity_
if DBCV < validity_:
    labels_ = random_search.best_estimator_.labels_
    result['LABELS'] = labels_

result.to_csv(f'Data/Clustering_Output/results.csv')
df0.to_csv('Data/Clustering_Output/input.csv')
cl_analysis = Analysis(df0, result["LABELS"])

