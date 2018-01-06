#!/bin/env python
# extras.py

'''
    Custom fitting methods
'''
'''
def get_sine_fit(series):

    x = range(len(series))
    y = series

    def fit_func(x, a, b, c):
        return a*np.sin(b*x) + c

    params = curve_fit(fit_func, x, y)

    [a, b, c] = params[0]
    print(a,b,c)

    return a*np.sin(b*x) + c
'''


def since_bc(dates):

    from datetime import datetime

    if type(dates) is not list:
        dates = [dates]

    return [(datetime.strptime(d, "%Y-%m-%d") - datetime(1, 1, 1)).days for d in dates]


def get_taylor_sine_fit(series):
    if type(series) is not list:
        raise Exception("argument 'series' needs to be of type list")

    import numpy as np
    from scipy.optimize import curve_fit

    x = np.linspace(0, 2 * len(series), len(series) / 2.0)
    y = series

    def fit_func(p, a, b, c, d):
        fit = p - a * np.power(p, 3) + b * np.power(p, 5) - c * np.power(p, 7) + d
        print("fit", fit)
        return fit

    params = curve_fit(fit_func, x, y)

    [a, b, c, d] = params[0]
    [aa, bb, cc] = [2 * 3, 2 * 3 * 4 * 5, 2 * 3 * 4 * 5 * 6 * 7]

    print("Values {0} compared to ideal {1}".format([a, b, c], [1.0 / aa, 1.0 / bb, 1.0 / cc]))

    return fit_func(x, a, b, c, d)


'''
    Custom color maps
'''


def get_yearly():
    '''
            'yearly' color map
        '''

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    import numpy as np

    # sample the colormaps that you want to use. Use 128 from each so we get 256
    # colors in total
    _colors1 = plt.cm.viridis(np.linspace(0., 1, 128))
    _colors2 = plt.cm.viridis_r(np.linspace(0, 1, 128))

    # combine them and build a new colormap
    _colors = np.vstack((_colors1, _colors2))

    return mcolors.LinearSegmentedColormap.from_list('yearly', _colors)


class ccmaps():
    yearly = get_yearly()
