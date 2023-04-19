from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from utils import z_score


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
        data = self.clustered.copy()
        y = data.pop("LABELS")
        X = data[self.no_clustered.columns].copy()

        cat_features = X.select_dtypes(include=[object]).columns.tolist()
        num_features = X.select_dtypes(include=[int, float]).columns.tolist()
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        mean = X_train[num_features].mean()
        std = X_train[num_features].std()
        X_train[num_features] = X_train[num_features].apply(z_score)
        X_test = (X_test - mean) / std
        X_nc = self.no_clustered.copy()
        X_nc[num_features] = (X_nc[num_features] - mean) / std
        for feature in cat_features:
            le = LabelEncoder()
            X[feature] = le.fit_transform(X[feature])
            X_nc[feature] = le.fit_transform(X_nc[feature])

        if classifier == 'RandomForest':
            rfc = RandomForestClassifier(n_estimators=100, random_state=42)
            rfc.fit(X_train, y_train)
            y_pred = rfc.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            print('Accuracy:', accuracy)

        return rfc.predict(X_nc)
