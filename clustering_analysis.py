import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
class Analysis:
    """
    A class to analyze numerical and categorical data by class labels.

    ...

    Attributes
    ----------
    original_df : pandas.DataFrame
        The original dataframe with the data to be analyzed.
    labels : pandas.Series
        A pandas series with the class labels corresponding to each row in `original_df`.

    Methods
    -------
    num_analysis()
        Returns descriptive statistics (mean, std, median, 25th and 75th percentiles) of the numerical data
        grouped by class labels.
    cat_analysis()
        Returns the proportion of each categorical value for each class label in the original data.

    """

    def __init__(self, original_df, labels):
        """
        Constructs all the necessary attributes for the Analysis object.

        Parameters
        ----------
            original_df : pandas.DataFrame
                The original dataframe with the data to be analyzed.
            labels : pandas.Series
                A pandas series with the class labels corresponding to each row in `original_df`.

        Raises
        ------
            AssertionError
                If the length of `original_df` and `labels` is not the same.

        """

        assert len(original_df) == len(labels), "The labels and the original data must have the same length"
        self.original_df = original_df
        self.labels = labels


    def num_analysis(self):
        """
        Returns descriptive statistics (mean, std, median, 25th and 75th percentiles) of the numerical data
        grouped by class labels.

        Returns
        -------
        tuple
            A tuple with the following pandas DataFrames:
            - std: the standard deviation of each numerical variable for each class label.
            - mean: the mean of each numerical variable for each class label.
            - medians: the median of each numerical variable for each class label.
            - cu25: the 25th percentile of each numerical variable for each class label.
            - cu75: the 75th percentile of each numerical variable for each class label.

        """

        numerics = self.original_df.select_dtypes(include=[int, float]).columns.tolist()
        clustered = self.original_df[numerics]
        clustered['LABELS'] = self.labels
        gb_obj = clustered.groupby(['LABELS'])
        mean = gb_obj.mean()
        std = gb_obj.std()
        medians = gb_obj.median()
        cu75 = gb_obj.quantile(0.75)
        cu25 = gb_obj.quantile(0.25)

        return std, mean, medians, cu25, cu75


    def cat_analysis(self):
        """
        Returns the proportion of each categorical value for each class label in the original data.

        Returns
        -------
        dict
            A dictionary containing a pandas DataFrame for each categorical variable in the original data.
            Each DataFrame has the proportion of each categorical value for each class label.

        """

        categorical = self.original_df.select_dtypes(include=["object"]).columns.tolist()
        clustered = self.original_df[categorical]
        clustered['LABELS'] = self.labels
        dic_cat = {}
        for c in categorical:
            # todos los valores de la categoria c frente a cada segmento
            g = clustered.groupby(["LABELS"] + [c]).size()
            m0 = g[0]
            m0 = m0 / sum(m0)
            m0.name = str(0)
            m0 = m0.to_frame()
            for k in [-1] + list(range(1, int(max(clustered["LABELS"])) + 1)):
                m1 = g[k]
                m1 = m1 / sum(m1)
                m1.name = str(k)
                m1 = m1.to_frame()
                m0 = m0.join(m1, how='outer')
            m0.reset_index(inplace=True)
            dic_cat[c] = m0
            g[k].plot(
                kind="bar", color=sns.color_palette("deep", np.unique(clustered["LABELS"]).shape[0])
            )
            plt.title(c + ' clase ' + str(k))


