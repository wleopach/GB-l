from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from utils import z_score
import numpy as np
import pandas as pd


class ClassifierNp:
    """
       A class representing a sklearn Random Forest classifier for mixed dataframes.
       Attributes:
           clustered (pd.DataFrame): A dataframe of clustered data.
           no_clustered (pd.DataFrame): A dataframe of non-clustered data.
       """

    def __init__(self, clustered, no_clustered):
        """
               Initializes a ClassifierNp object.
               Args:
                   clustered (pd.DataFrame): A dataframe of clustered data.
                   no_clustered (pd.DataFrame): A dataframe of non-clustered data.
               Raises:
                   AssertionError:  columns in no_clustered is not contained in the columns in clustered.
               """
        assert set(clustered.columns) >= set(no_clustered.columns), 'Incompatible columns'
        self.clustered = clustered
        self.no_clustered = no_clustered

    def predict(self, classifier: str):
        """
               Predicts the labels of the non-clustered data using the specified classifier.
               Args:
                   classifier (str): The name of the classifier to use.
               Returns:
                   np.ndarray: An array of predicted labels for the non-clustered data.
               """

        columns = self.no_clustered.columns
        features_ = list(columns) + ['LABELS']
        data = self.clustered.copy()[features_]
        # ******************** When there are colissions resolve taking the mode if exists otherwise random **********
        data2 = data.groupby(list(columns))['LABELS'].apply(
            lambda x: x.mode()[0][0]).reset_index()

        y = data2.pop("LABELS")
        X = data2[list(columns)].copy()
        cat_features = X.select_dtypes(include=[object]).columns.tolist()
        num_features = X.select_dtypes(include=[int, float]).columns.tolist()
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        mean = X_train[num_features].mean()
        std = X_train[num_features].std()
        X_train[num_features] = X_train[num_features].apply(z_score)
        X_test[num_features] = (X_test[num_features] - mean) / std
        X_nc = self.no_clustered.copy()
        X_nc[num_features] = (X_nc[num_features] - mean) / std
        for feature in cat_features:
            le = LabelEncoder()
            X_train[feature] = le.fit_transform(X_train[feature])
            X_test[feature] = le.fit_transform(X_test[feature])
            X_nc[feature] = le.fit_transform(X_nc[feature])

        if classifier == 'RandomForest':
            rfc = RandomForestClassifier(n_estimators=100, random_state=42)
            rfc.fit(X_train, y_train)
            y_pred = rfc.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)

            print('Accuracy:', accuracy)

        return rfc.predict(X_nc)




################################UNIT TEST CODE#################################################
clustered_data = pd.DataFrame({
    'Column1': ['A', 'A', 'B', 'B', 'C', 'C', 'D', 'D'],
    'Column2': [1, 1, 2, 2, 3, 3, 4, 4],
    'LABELS': ['Positive', 'Positive', 'Negative', 'Positive', 'Neutral', 'Neutral', 'Positive', 'Negative'],
    'Column3': [0.5, 0.3, 0.1, 0.4, 0.6, 0.7, 0.8, 0.9],
    'Column4': ['X', 'Y', 'Z', 'X', 'Y', 'Z', 'X', 'Z']
})

no_clustered_data = pd.DataFrame({
    'Column2': [1, 2, 3, 4],
    'Column4': ['X', 'Z', 'Y', 'Z']
})

clf = ClassifierNp(clustered_data, no_clustered_data)
predicted_labels = clf.predict('RandomForest')