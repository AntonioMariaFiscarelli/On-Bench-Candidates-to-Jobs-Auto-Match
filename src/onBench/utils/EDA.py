"""Functions for the Exploratory Data Analysis"""

import numpy as np
np.set_printoptions(suppress=True)
import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

import holoviews as hv
from holoviews import opts
import hvplot.pandas
hv.extension('bokeh')
import jupyter_bokeh
from bokeh.models import HoverTool




def general_info(df):
    print("Info: ")
    print(df.info())
    print("\n\nPercentage of missing values:")
    print(np.round(df.isna().sum()/df.shape[0] * 100, 2))
    print("\n\nNumber of duplicated rows:")
    print(df.duplicated().sum())
    print("\n\nCheck number of unique values for each categorical column (absolute and normalized w.r.t number of non-null values)):")
    #print(df.select_dtypes(exclude = "number").nunique())
    print(pd.concat([df.select_dtypes(exclude="number").nunique(), round(df.select_dtypes(exclude="number").nunique() / df.select_dtypes(exclude="number").count(), 2)], axis=1).rename(columns={0:"unique_count",1:"normalized_unique"}))
    print("\n\nBasic statistics for numerical columns: ")
    print(df.select_dtypes(include = "number").describe().apply(lambda s: s.apply(lambda x: format(x, 'f'))))

    
def bar_plot(df, cat, 
             label_range=-1, yrange=[], 
             sort_by = 'count', normalized=False,
             rotation = 90, xticks_size = '12pt',
             height=400, width=800):
        
    counts = df[cat].value_counts()
    if normalized:
        counts= np.round(counts/len(counts))
    counts_df = counts.reset_index()
    counts_df.columns = [cat, 'COUNT']

    if sort_by == 'count':
        counts_df = counts_df.sort_values(by='COUNT', ascending=False)
    elif sort_by == 'label':
        counts_df = counts_df.sort_values(by=cat)

    if (label_range!=-1):
        counts_df = counts_df.head(label_range)
    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[-1]
    else:
        ymin, ymax = np.min(counts_df['COUNT']), np.max(counts_df['COUNT'])

    bars = counts_df.hvplot.bar(cat).opts(xlabel=cat, ylabel='Count', 
                  xrotation=rotation,
                  title='Occurrences of ' + cat,
                  #yformatter='%.0f',
                  fontsize={'xticks': xticks_size},
                  ylim=(ymin, ymax),
                  height=height, width=width)
    
    return bars


def date_bar_plot(df_, cat, by = 'year',
             label_range=-1, yrange=[], normalized=False,
             height=400, width=800):

    df = df_.copy()
    # Ensure the 'cat' column is datetime and extract year
    df[cat] = pd.to_datetime(df[cat])

    # Extract the appropriate time component
    if by == 'month':
        df[by] = df[cat].dt.month
    elif by == 'day':
        df[by] = df[cat].dt.day
    elif by == 'weekday':
        weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        df[by] = df[cat].dt.day_name()
        df[by] = pd.Categorical(df[by], categories=weekday_order, ordered=True)
    else:
        df[by] = df[cat].dt.year


    counts = df[by].value_counts().sort_index()


    if normalized:
        counts = np.round(counts / counts.sum(), 4)
    counts_df = counts.reset_index()
    
    counts_df.columns = [by, 'COUNT']


    counts_df = counts_df.sort_values(by=by)


    # Limit number of labels if specified
    if label_range != -1:
        counts_df = counts_df.head(label_range)

    # Determine y-axis range
    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[-1]
    else:
        ymin, ymax = np.min(counts_df['COUNT']), np.max(counts_df['COUNT'])


    # Dynamically adjust bar width based on number of bars
    num_bars = len(counts_df)
    bar_width = max(100, min(100 * (20 / num_bars), 1.0)) if num_bars > 0 else 100

    bar_width = (
        100 if by == 'year' 
        else 0.8
        )
    
    xticks = (
        list(zip(range(len(counts_df)), counts_df[by].astype(str))) if by == 'weekday'
        else list(zip(counts_df[by], counts_df[by]))
    )


    # Create bar plot
    bars = counts_df.hvplot.bar(x=by, y='COUNT').opts(
        xlabel=by, ylabel='Count',
        xrotation=90,
        title='Vacancies by ' + by,
        #xticks=xticks,
        ylim=(ymin, ymax),
        height=height, width=width,
        bar_width = bar_width
    )
    return bars


def hist_plot(df, cat, bins=200, xrange=[], yrange=[], height=400, width=800):

    df = df[cat][np.isfinite(df[cat])]
    frequencies, edges = np.histogram(df, bins=bins)
    
    if len(xrange) == 2:
        xmin, xmax = xrange[0], xrange[1]
    else:
        xmin, xmax = edges[0], edges[-1]
    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[-1]
    else:
        ymin, ymax = np.min(frequencies), np.max(frequencies)

    plot = hv.Histogram((edges, frequencies), kdims=[cat])
    plot.opts(title="Histogram for " + cat, xlim=(xmin, xmax), ylim=(ymin, ymax), 
              #xformatter='%.1f', yformatter='%.1f',
              height=height, width=width) 

    return plot

def hist_plot_date(df, cat, bins=200, xrange=[], yrange=[], height=400, width=800):

    date_counts = df[cat].value_counts().sort_index()

    if len(xrange) == 2:
        xmin, xmax = xrange[0], xrange[1]
    else:
        xmin, xmax = df[cat].min(), df[cat].max()
    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[-1]
    else:
        ymin, ymax = np.min(date_counts), np.max(date_counts)

    # Create a histogram
    hist = hv.Bars((date_counts.index, date_counts.values), kdims='Date', vdims='Frequency')

    # Customize the plot
    hist.opts(
        opts.Bars(
            xrotation=45,
            xlabel='Date',
            ylabel='Frequency',
            #xlim=(xmin, xmax), ylim=(ymin, ymax),
            width=800,
            height=400,
            tools=['hover']
        )
    )
    return hist

def box_plot(df_, y, by, log=False, label_range=-1, yrange=[], 
            normalized=False,
            height=400, width=800):

    df = df_.copy()
    df = df.sort_values(by=by)

    if(label_range!=-1):
        top_N_groups = df.groupby(by).size().nlargest(label_range).index.tolist()
        df = df[df[by].isin(top_N_groups)]

    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[-1]
    else:
        ymin, ymax = np.min(df[y]), np.max(df[y])

    box_plot =  df.hvplot.box(y=y, by=by,
                  xlabel=by, ylabel=y,
                  rot=0, invert=True, 
                  height=400, width=800)

    box_plot.opts(ylim=(ymin, ymax), #xformatter='%.0f',
                  height=height, width=width, logx=log)
    
    return box_plot 

def scatter_plot(df_, x, y, by="", xrange=[], yrange=[], log=False, slope=False, height=400, width=800, show_xy_coord=False):

    df = df_.copy()
    df['count'] = df.copy().groupby([x, y])[x].transform('count')
    
    if len(xrange) == 2:
        xmin, xmax = xrange[0], xrange[1]
    else:
        xmin, xmax = np.min(df[x]), np.max(df[x])
    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[1]
    else:
        ymin, ymax = np.min(df[y]), np.max(df[y])

    if by != "":
        plot = df.hvplot.scatter(x=x, y=y, by=by, legend='top', hover_cols=['count'])
    else:
        plot = df.hvplot.scatter(x=x, y=y, legend='top', hover_cols=['count'])

    if slope:
        plot = plot * hv.Slope(1, 1).opts(color='red')

    if show_xy_coord:
        hover = HoverTool(tooltips=[("X", "@{"+x+"}"), ("Y", "@{"+y+"}"), ("Count", "@count")])
    else:
        hover = HoverTool(tooltips=[("Count", "@count")])
    
    plot.opts(tools=[hover], xlim=(xmin, xmax), ylim=(ymin, ymax), logx=log, logy=log,
              height=height, width=width)
    
    return plot



def violin_plot(df, y, by, log=False, label_range=-1, yrange=[], 
                height=400, width=800):

    df = df.sort_values(by=by)

    if label_range != -1:
        top_N_groups = df.groupby(by).size().nlargest(label_range).index.tolist()
        df = df[df[by].isin(top_N_groups)]

    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[1]
    else:
        ymin, ymax = np.min(df[y]), np.max(df[y])

    violin_plot = df.hvplot.violin(y=y, by=by,
                                   xlabel=by, ylabel=y,
                                   rot=0, invert=True, 
                                   height=height, width=width)

    violin_plot.opts(ylim=(ymin, ymax), 
                     height=height, width=width, logx=log)
        
    return violin_plot

def ridgeline_plot(df, y, by, log=False, label_range=-1, yrange=[], 
                   height=400, width=800):

    df = df.sort_values(by=by)

    if label_range != -1:
        top_N_groups = df.groupby(by).size().nlargest(label_range).index.tolist()
        df = df[df[by].isin(top_N_groups)]

    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[1]
    else:
        ymin, ymax = np.min(df[y]), np.max(df[y])

    # Create density plots for each group
    ridgeline = hv.NdOverlay({group: df[df[by] == group].hvplot.kde(y=y, label=group)
                              for group in df[by].unique()})

    ridgeline.opts(ylim=(ymin, ymax), 
                   height=height, width=width, logx=log)
        
    return ridgeline

def ridgeline_plot_with_points(df, y, by, log=False, label_range=-1, yrange=[], 
                               height=400, width=800):

    df = df.sort_values(by=by)

    if label_range != -1:
        top_N_groups = df.groupby(by).size().nlargest(label_range).index.tolist()
        df = df[df[by].isin(top_N_groups)]

    if len(yrange) == 2:
        ymin, ymax = yrange[0], yrange[1]
    else:
        ymin, ymax = np.min(df[y]), np.max(df[y])

    # Create scatter plots for each group
    ridgeline = hv.NdOverlay({group: df[df[by] == group].hvplot.scatter(y=y, label=group, alpha=0.5)
                              for group in df[by].unique()})

    ridgeline.opts(ylim=(ymin, ymax), 
                   height=height, width=width, logx=log)
        
    return ridgeline


