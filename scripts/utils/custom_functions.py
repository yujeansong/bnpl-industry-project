# Includes functions that may be used across multiple notebooks

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy import stats

def scatter_density(data: pd.DataFrame, x_axis: str, y_axis: str, use_title: str) -> plt:
    '''
    Produces a scatter plot with a colour scheme for points representing the density of observations
    around that point. Is effective for dense plots with lots of overlapping points
    Arguments:
        data = pandas DataFrame containing plot data
        x_axis = column name for x-axis data
        y_axis = column name for y-axis data
        use_title = title for the plot
    Ouput: a matplotlib plot
    '''
    
    # extract the required data
    x = data[x_axis].to_numpy(dtype='float64')
    y = data[y_axis].to_numpy(dtype='float64')

    # create a density function for the scatter plot
    xy = np.vstack([x,y])
    z = stats.gaussian_kde(xy)(xy)

    # Sort the points by density, so that the densest points are plotted last
    idx = z.argsort()
    x, y, z = x[idx], y[idx], z[idx]

    # plot the points
    scatter_density_data = pd.DataFrame({'x': x, 'y': y, 'scaled density': z/max(z)})
    plot = scatter_density_data.plot.scatter(
        x='x', y='y',
        title = use_title, 
        xlabel=x_axis, ylabel=y_axis,
        c='scaled density', cmap='Spectral_r', s=25)

    fig = plot.get_figure()
    fig.set_size_inches(9, 6)
    return(fig)