import sys
import numpy as np
import pandas as pd
from scipy import optimize
from defaults import *

def load_history(filename, levels, filters):
    """
    Metoda, ktora nacita v pkl formate zgroupovane denne data v dlhom formate
    Zakladne vycistenie 
    """
    history = pd.read_pickle(filename)
    # filter items
    if len(levels) == 1:
        history = history.loc[history[levels[0]].isin(filters)]
    else:
        history = history
    # pivot
    hpivot = history.pivot(index=['ROK','DATUM_UCTOVANIA','MESIAC','DEN'], columns=levels, values='CUMSUM').reset_index()
    # drop February 29
    hpivot = hpivot.drop(hpivot[(hpivot.MESIAC == 2) & (hpivot.DEN == 29)].index)
    return hpivot

def share_calculation(history):
    """
    Metoda, ktora vypocita z historickych dat denne percentualne trendy 
    Vrati jeden dataset s percentami po dnoch za vsetky roky 
    """
    # maximum levels of each category within a year
    maxima = history.groupby(['ROK']).max()
    # create df with same shape as history
    max_levels = history[['DATUM_UCTOVANIA','ROK','MESIAC','DEN']].merge(maxima.drop(['MESIAC','DEN'], axis=1), on='DATUM_UCTOVANIA', how='left').fillna(method='backfill', axis=0)
    # compute shares
    shares = history.set_index(['DATUM_UCTOVANIA','ROK','MESIAC','DEN']) / max_levels.set_index(['DATUM_UCTOVANIA','ROK','MESIAC','DEN'])
    return shares.reset_index()

def train_test_split(shares, train_yrs=np.arange(2013,2019)):
    """
    Metoda, ktora rozdeli dataset na trenovaciu a testovaciu sadu
    Vrati dva datasety - train a test
    """
    # all years in sample
    years = set(pd.unique(shares.ROK))
    # filter out test years given train years
    test_yrs = years.difference(set(train_yrs))
    # split data
    train = shares.copy()[shares.ROK.isin(train_yrs)].fillna(0.).set_index(['DATUM_UCTOVANIA','ROK','MESIAC','DEN'])
    test = shares.copy()[shares.ROK.isin(test_yrs)].fillna(0.).set_index(['DATUM_UCTOVANIA','ROK','MESIAC','DEN'])
    return train, test 

def compute_prediction(train, coefs):
    """
    Metoda, ktora vypocita predikciu trendov na zaklade trenovacieho setu a koeficientov
    Shapes:
    - train: roky x dni x klasifikacia
    - coefs: 1 x roky 
    - beta_vals: dni x klasifikacia 
    """
    # hodnoty z pd frame
    vals = train.values
    vals = vals.reshape(int(vals.shape[0]/365), 365, vals.shape[1])
    # vypocet predikcie
    beta_vals = (vals.transpose(1,2,0) @ coefs.T).reshape(365, vals.shape[2])
    return beta_vals

def compute_error(beta, test):
    """
    Metoda, ktora porovna vypocitany odhad trendu na zaklade testovacich setov 
    Shapes
    - beta: dni x klasifikacia
    - test: roky x dni x klasifikacia
    Vystupom je RMSE za vsetky roky v test sete
    """
    # number of years in test set 
    years = int(test.shape[0]/365)
    # values of test set, reshaped
    testvals = test.values.reshape(years, 365, test.shape[1])
    
    rmse = np.ones((years,))
    residuals = [0] * years
    # compute rmse for each year
    for y in np.arange(years):
        t = testvals[y]
        delta = t - beta 
        residuals[y] = np.sqrt(delta ** 2)
        rmse[y] = np.sqrt(np.mean(delta ** 2))
    group_rmse = np.mean(rmse)
    return group_rmse, residuals

def trainer(coefs, train, test, results=False):
    """
    Wrapper metoda pre vypocet predikcie a chyb
    Pouzita v optimalizacnom skripte
    """
    beta = compute_prediction(train, coefs)
    rmse, residuals = compute_error(beta, test)

    if not results:
        return rmse
    else:
        return {
            'Xb': beta,
            'coefs': coefs,
            'residuals': residuals
        }

def optimize_train(train, test):
    """
    Metoda pre vypocet optimalnych vah pre jednotlive roky v train sete 
    Vystupom je vazeny priemer trendov za roky v train sete
    """
    # number of years in train set 
    years = int(train.shape[0]/365)
    # optimizer setup
    coefs = np.ones((1, years)) / years 
    bnds = [(0,1)] * years
    cons = lambda x: x.sum() - 1
    # linear optimizer 
    results = optimize.minimize(trainer, coefs, args=(train, test), method='SLSQP', bounds=bnds, constraints=[{'type':'eq','fun':cons}])
    weights = results.x
    # weighted trend
    trends = trainer(weights, train, test, True)
    return trends['Xb'], trends['residuals']

def build_trends(beta, residuals, test):
    """
    Metoda, ktora na zaklade optimalizacie vybuduje hotovy dataframe trendov pre predikcie

    """
    # years in test set 
    years = pd.unique(test.reset_index()['ROK'])
    years.sort()
    # filter one year of test set
    trend_skeleton = test.filter(like=str(years[0]), axis=0)
    trend_skeleton.values[:] = 0. # empty skeleton
    trend_skeleton = trend_skeleton.droplevel([0,1], axis=0) # drop date and year, keep only month and day

    # mean trends
    mean_trend = trend_skeleton.copy()
    mean_trend.values[:] = beta
    # smooth with 7d MA
    smooth_trend = mean_trend.rolling(7, win_type='cosine').mean().fillna(0.)

    # errors
    errors = np.ones((len(years), 365, test.shape[1]))
    for y, year in enumerate(years):
        res = residuals[y]
        testyear = test.filter(like=str(year), axis=0).values
        errors[y,:,:] = res / testyear

    np.nan_to_num(errors, False, 0.,0.,0.)
    mean_error = errors.mean(axis=0)

    # error frame
    error_frame = trend_skeleton.copy()
    error_frame.values[:] = mean_error
    # smooth with 7d MA
    smooth_error = error_frame.rolling(7, win_type='cosine').mean().fillna(0.)

    # master frame
    trend_frame = pd.concat({'Trend': smooth_trend, 'Errors': smooth_error}, names=['Forecast'])
    return trend_frame

if __name__ == "__main__":
    """
    Argument structure
    1. name of historical data file
    2. flag -l
    3. list of levels
    4. flag -f
    5. list of filtered items to use

    if -l not passed, default levels are used
    if -f not passed, all items are used
    """ 

    try:
        arguments = sys.argv[1:]
    except IndexError:
        raise SystemExit("No arguments given")

    history = arguments[0]
    if '-l' in arguments:
        level_position = arguments.index('-l') + 1
        if '-f' in arguments:
            filter_position = arguments.index('-f') + 1
            try:
                levels = arguments[level_position:(filter_position-1)]
                filters = arguments[filter_position:]
            except:
                levels = ['EK3']
                filters = []
        else:
            try:
                levels = arguments[level_position:]
            except:
                levels = []
                filters = []
    else:
        levels = ['EK3']
        filters = []

    print(levels)
    print(filters)

    h = load_history(history, levels, filters)
    shares = share_calculation(h)
    train, test = train_test_split(shares)
    beta, residuals = optimize_train(train, test)
    trends = build_trends(beta, residuals, test)
    filename = 'master_trends.'
    trends.to_pickle(f'{filename}pkl')
    trends.reset_index().to_csv(f'{filename}csv', index_label='ID')
    print(f'{filename} saved as CSV and PKL')
  
 



