import matplotlib.pyplot as plt
import seaborn as sns

from plotnine import *


def bar(df, col):
    """User ggplot2 to plot categorical column as bars"""
    df2 = df.copy()
    df2[col] = df2[col].astype(float)
    # r(f'''
    # # R script
    #
    # library(ggplot2)
    # ggplot(data={r_dataframe.r_repr()}, aes({col})) +
    # geom_bar()
    # ''')
    pp = (ggplot(df2) +
          aes(x=col) +
          geom_bar())
    pp.plot()


def hist(df, col, title):
    """User ggplot2 to plot categorical column as bars"""
    df2 = df.copy()
    df2[col] = df2[col].astype(float)
    # r(f'''
    # # R script
    #
    # library(ggplot2)
    # ggplot(data={r_dataframe.r_repr()}, aes({col})) +
    # geom_bar()
    # ''')
    pp = (ggplot(df2) +
          aes(x=col, bins=30, fill='DESCRIPCION_PRODUCTO') +
          geom_histogram() +
          labs(
              title=title
          )
          +
          theme_dark() +
          theme(figure_size=(20, 16))

          )
    pp = pp + theme(legend_position='bottom')

    pp.draw()
    plt.figure(figsize=(15, 15))
    plt.show()


# ####PLOTS CLUSTERING
def plot_join(x, y, labels, save=False, path=None):
    """ Draw a plot of two variables with bivariate and univariate graphs
        x =      x coordinate
        y =      y coordinate
        labels = classes of the points
        save = boolean to decide if save
        path = path to store the image
    """
    _ = sns.jointplot(
        x=x, y=y, kind="kde", hue=labels
    )
    if save:
        if not path:
            path = input("please type path to image with file extension")
        plt.savefig(path)
    plt.show()


# _ = clf.hdbscan_.condensed_tree_.plot(
#     select_clusters=True,
#     selection_palette=sns.color_palette("deep", np.unique(labels).shape[0]),
# )
# plt.show()
#
# dic_cat = {}
# for c in categorical.columns:
#     # todos los valores de la categoria c frente a cada segmento
#     g = df.groupby(["SEGMENT"] + [c]).size()
#     # g.plot(
#     #     kind="bar", color=sns.color_palette("deep", np.unique(labels).shape[0])
#     # )
#     # plt.title(c)
#     # plt.savefig(path_data + '/'+c+'.png')
#     # plt.show()
#     # plt.figure(figsize=(30,8))
#     # todos los valores de la categoria c frente a la clase 1
#     m0 = g[0]
#     m0 = m0 / sum(m0)
#     m0.name = str(0)
#     m0 = m0.to_frame()
#     for k in [-1] + list(range(1, max(df['SEGMENT']) + 1)):
#         m1 = g[k]
#         m1 = m1 / sum(m1)
#         m1.name = str(k)
#         m1 = m1.to_frame()
#         m0 = m0.join(m1, how='outer')
#     m0.reset_index(inplace=True)
#     dic_cat[c] = m0
#     g[k].plot(
#         kind="bar", color=sns.color_palette("deep", np.unique(labels).shape[0])
#     )
#     plt.title(c + ' clase ' + str(k))
#     plt.show()
