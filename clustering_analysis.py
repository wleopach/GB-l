import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import shutil
import zipfile


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
        self.labels = labels.astype(str)

    def num_analysis(self):
        """
        Returns descriptive statistics (mean, std, median, 25th and 75th percentiles) of the numerical data
        grouped by class labels, contained in a dictionary

        Returns
        -------
        dict
            A dict with the following pandas DataFrames:
            - std: the standard deviation of each numerical variable for each class label.
            - mean: the mean of each numerical variable for each class label.
            - medians: the median of each numerical variable for each class label.
            - cu25: the 25th percentile of each numerical variable for each class label.
            - cu75: the 75th percentile of each numerical variable for each class label.

        """

        numerics = self.original_df.select_dtypes(include=[int, float]).columns.tolist()
        clustered = self.original_df[numerics].copy()
        clustered['LABELS'] = self.labels
        gb_obj = clustered.groupby('LABELS')
        mean = gb_obj.mean()
        std = gb_obj.std()
        medians = gb_obj.median()
        cu75 = gb_obj.quantile(0.75)
        cu25 = gb_obj.quantile(0.25)

        return {'std': std, 'mean': mean, 'medians': medians, 'cu25': cu25, 'cu75': cu75}

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
        clustered = self.original_df[categorical].copy()
        clustered['LABELS'] = self.labels
        gb_obj = clustered.groupby('LABELS')
        mode = gb_obj.apply(lambda x: x.mode().iloc[0])
        mode = mode.drop(columns=['LABELS'])

        return {'mode': mode}

    def merge(self):
        """This function merges the numeric and categorical
         analysis results obtained from the num_analysis() and cat_analysis()
          methods of an object, and returns a dictionary and a pandas dataframe.

        :return:
            reform:         A dictionary that contains the merged analysis results.
                            The dictionary has a tuple as the key, where the first element
                            of the tuple is the original key in the numeric or categorical
                            analysis dictionary, and the second element of the tuple is the feature name.
                            The value of each key is a dictionary that contains the analysis results for the
                            corresponding feature.

            merged_df:      A pandas dataframe that contains the merged analysis results.
                            Each row of the dataframe corresponds to a feature, and each column
                            corresponds to an analysis metric.
                """
        numerics = self.num_analysis()
        categorical = self.cat_analysis()
        numerics.update(categorical)
        for k in numerics:
            numerics[k] = numerics[k].to_dict()
        reform = {(outerKey, innerKey): values
                  for outerKey, innerDict in numerics.items()
                  for innerKey, values in
                  innerDict.items()}
        merged_df = pd.DataFrame(reform)
        return reform, merged_df

    def save_res(self, folder_name: str, path='.'):
        """
        Zips the results in a given path with a given name
        :param folder_name: name of the folder
        :param path: path where the zip file is going to be created
        :return: None
        """

        path_to_folder = os.path.join(path, folder_name)

        os.makedirs(path_to_folder, exist_ok=True)
        self.original_df.to_csv(os.path.join(path_to_folder, 'input.csv'), index=True)
        self.labels.to_csv(os.path.join(path_to_folder, 'results.csv'), index=True)

        for key, df in self.num_analysis().items():
            # Create the filename for the CSV file
            filename = f"{key}.csv"
            # Save the DataFrame to a CSV file in the output folder
            df.to_csv(os.path.join(path_to_folder, filename), index=True)
        for key, df in self.cat_analysis().items():
            # Create the filename for the CSV file
            filename = f"{key}.csv"
            # Save the DataFrame to a CSV file in the output folder
            df.to_csv(os.path.join(path_to_folder, filename), index=True)

        _, merged = self.merge()
        merged.to_csv(os.path.join(path_to_folder, 'merged.csv'), index=True)
        zip_filename = f"{path_to_folder}.zip"
        with zipfile.ZipFile(zip_filename, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for root, dirs, files in os.walk(path_to_folder):
                for file in files:
                    zip_file.write(os.path.join(root, file))

    def col_in_clus(self, sel_col: str, sts: set, num: bool):
        """
        Retrievs a set of statistics for a given column
        :param col: attribute  to be observed
        :param sts: set of statistics to include
        :param num: if the column is numerical or not
        :return: data frame with the statistical information
        """
        if num:
            numerics = self.num_analysis()
            num_columns = sts & set(numerics.keys())
            num_series_list = [numerics[st][sel_col] for st in num_columns]
            # Create a dictionary of column names and Series
            data = {col: series for col, series in zip(num_columns, num_series_list)}
            frame = pd.DataFrame(data)
            return frame
        else:
            categorical = self.cat_analysis()

            cat_columns = sts & set(categorical.keys())
            # Define the list of Series

            cat_series_list = [categorical[st][sel_col] for st in cat_columns]


            data = {col: series for col, series in zip(cat_columns, cat_series_list)}
            frame = pd.DataFrame(data)
            return frame

            # Create a DataFrame from the dictionary

# #########################unit test##########################################33
# data = pd.read_csv('outputs/res_clus52-11.csv')
# labels = data.pop('LABELS')
# cols = ['PORCION_PAGOS', 'ACCION_CONTACTO',
#         'PORTAFOLIO', 'DIAS_DE_MORA_ACTUAL', 'CP', 'IDENTIFICACION',
#         'SALDO_CAPITAL_CLIENTE', 'ID_TABLA', 'PORCION_PAGO',
#         'MESES_INICIALES_NO_PAGO', 'PLAZO_INICIAL_ADJ', 'MOTIVO', 'PLAZO',
#         'MONTO', 'CUOTA', 'ESTADO', 'CONDONACION', 'TASA', 'TI_MEAN']
# df_original = data[cols].copy()
#
# cla = Analysis(df_original, labels)
# numerics = cla.num_analysis()
# categorics = cla.cat_analysis()
# numerics.update(categorics)
# for k in numerics:
#     numerics[k] = numerics[k].to_dict()
# reform = {(outerKey, innerKey): values for outerKey, innerDict in numerics.items() for innerKey, values in
#           innerDict.items()}
# merged_df = pd.DataFrame(reform)
