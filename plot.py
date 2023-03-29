
import matplotlib.pyplot as plt


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
