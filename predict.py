from datetime import date
import time
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from rozpocet_ucto_sql import * 
from group_rozpocet_ucto import *
from defaults import *
from author import *
from aktualne_cerpanie_export import batch_query as rozpocet

def psql_connect():
    """
    Metoda, ktora sa napoji na postgres a vrati engine
    """
    connection = f'postgresql://{PSQL_NAME}:{PSQL_PW}@{PSQL_SERVER}:{PSQL_PORT}/{PSQL_DB}'
    engine = create_engine(connection)
    return engine

def load_current_sql():
    """
    Z Noris SQL stiahne aktualny rok
    Vrati vycisteny pandas dataframe 
    """
    year = date.today().year
    month = date.today().month

    c, cu = sql_connect()
    data = sql_ucto(cu, 1, month, year)
    c.close()
    
    clean = clean_sql_ucto(data)
    return clean

def group_current(clean_data):
    """
    Spracuje data z Noris do struktury, ktora moze byt pouzita pre predikciu
    
    DPFO mesacne
    Danove denne ako EKRK
    Nedanove denne ako EK3

    Vrati pandas dataframe
    """
    
    # EKRK, vyber danove
    grouped_ekrk = grouper(clean_data, ['EKRK'])
    years = pd.unique(grouped_ekrk.ROK)
    skelet = skeleton(years.min(), years.max())
    final_ekrk = finalize(grouped_ekrk, skelet, ['EKRK'])
    danove = final_ekrk[final_ekrk.EKRK.str[:2].isin(['12','13'])]
    dan_pivot = danove.pivot(index=['ROK','DATUM_UCTOVANIA','MESIAC','DEN'], columns='EKRK', values='CUMSUM')

    # EK3, vyber nedanove
    grouped_ek3 = grouper(clean_data, ['EK3'])
    years = pd.unique(grouped_ek3.ROK)
    skelet = skeleton(years.min(), years.max())
    final_ek3 = finalize(grouped_ek3, skelet, ['EK3'])
    nedanove = final_ek3[final_ek3.EK3.str[:1].isin(['2','3'])]
    nedanove_bezKV = nedanove[~nedanove.EK3.str[:2].isin(['23','32','33'])]
    nedan_pivot = nedanove_bezKV.pivot(index=['ROK','DATUM_UCTOVANIA','MESIAC','DEN'], columns='EK3', values='CUMSUM')

    # vyber DPFO a zgroupuj na mesiac
    dpfo = final_ek3[final_ek3.EK3 == '111']
    dpfo_mesiac = dpfo.groupby(['ROK','MESIAC'])['CUMSUM'].max() * 0.68 # max je vratane DPFO pre mestske casti, preto ponizit
    
    return dpfo_mesiac, dan_pivot, nedan_pivot

def load_trend(current, trend_file):
    """
    Podla aktualnych dat nacitaj tie trendy, ktore potrebujeme 
    Vrati pandas dataframe
    """

    current_levels = current.columns 
    trends = pd.read_pickle(trend_file)
    filtered_trends = trends[current_levels]
    return filtered_trends

def eoy_forecast(current, trends, weights=None):
    """
    Metoda, ktora vypocita EOY prognozu pre jednotlive polozky 
    vrati dennu prognozu a error prognozy
    Weights su specialne upravy prognozy (napr. pre DZN, ktore sa deli na polovicu)
    """
    # aktualny datum pre kontrolu
    _month = date.today().month
    _day = date.today().day
    # najdi posledny datum v aktual datach
    creset = current.reset_index()
    max_m = creset.MESIAC.max()
    max_d = creset[creset.MESIAC == max_m].DEN.max()
    # kontrola ci data nie su "popredu" z nejakeho dovodu
    if max_m == _month and max_d > _day:
        max_d = max(_day - 1, 1) # fix to day before today
    if max_m > _month:
        max_m = _month
        max_d = max(_day - 1, 1)
  
    current_day = creset[(creset.MESIAC == max_m) & (creset.DEN == max_d)]
    cd_values = current_day.values[0,4:].astype(np.float64)
    # vyber vhodny trend
    treset = trends.reset_index()
    relevant_trend = treset[(treset.MESIAC == max_m) & (treset.DEN == max_d) & (treset.Forecast == 'Trend')]
    rt_values = relevant_trend.values[0, 3:].astype(np.float64)
    relevant_error = treset[(treset.MESIAC == max_m) & (treset.DEN == max_d) & (treset.Forecast == 'Errors')]
    re_values = relevant_error.values[0, 3:].astype(np.float64)
    # eoy prognoza
    EOY = cd_values / rt_values
    # denna prognoza
    daily_forecast = treset[treset.Forecast == 'Trend'].set_index(['Forecast','MESIAC','DEN']) * EOY
    # prevazenie
    if weights is not None:
        for lvl in set(daily_forecast.columns).intersection(set(weights.keys())):
            current[lvl] = current[lvl] * weights[lvl]
            daily_forecast[lvl] = daily_forecast[lvl] * weights[lvl]
    
    # chyba prognozy
    errors = daily_forecast * re_values
        
    # napln skutocnostou
    l = len(creset[((creset.MESIAC < max_m) | ((creset.MESIAC == max_m) & (creset.DEN <= max_d)))]) # dlzka skutocnych dat
    daily_forecast[:l] = current.values[:l]
    errors[:l] = 0.
    daily_forecast.replace([np.inf, -np.inf], np.nan, inplace=True) # nahrad inf hodnoty nan
    errors.replace([np.inf, -np.inf], np.nan, inplace=True) # nahrad inf hodnoty nan
    daily_forecast = daily_forecast.fillna(method='pad',axis=0)
    errors = errors.fillna(method='pad',axis=0)

    # vypocet rozptylu
    low = daily_forecast - errors
    high = daily_forecast + errors

    # nech chyby nie su nizsie ako posledna skutocnost
    last_current = current.values[l-1]
    low[l:] = low[l:].apply(lambda x: np.maximum(x, last_current), axis=1) # nahrad nizsi error ako posledna skutocnost poslednou skutocnostou
    high[l:] = high[l:].apply(lambda x: np.maximum(x, last_current), axis=1) # nahrad nizsi error ako posledna skutocnost poslednou skutocnostou



    # spolocny frame
    prediction = pd.concat({'Forecast':daily_forecast, 'Low':low, 'High':high}, names=['Prediction']).droplevel([1],axis=0)
    return prediction

def dpfo_forecast(current, trends):
    """
    Metoda, ktora vypocita EOY prognozu pre DPFO
    vrati EOY prognozu a error prognozy
    """
    # najdi posledny datum v aktual datach
    creset = current.reset_index()
    max_m = creset.MESIAC.max()
    current_day = creset[(creset.MESIAC == max_m)]
    cd_values = current_day.values[0,-1].astype(np.float64)
    # vyber vhodny trend
    treset = trends.reset_index()
    relevant_trend = treset[(treset.MESIAC == max_m) & (treset.Prediction == 'Forecast')]
    rt_values = relevant_trend.values[0, -1]
    relevant_errorH = treset[(treset.MESIAC == max_m) & (treset.Prediction == 'High')]
    reH_values = relevant_errorH.values[0, -1]
    relevant_errorL = treset[(treset.MESIAC == max_m) & (treset.Prediction == 'Low')]
    reL_values = relevant_errorL.values[0, -1]
    # eoy prognoza
    EOY = cd_values / rt_values
    low = cd_values / reL_values
    high = cd_values / reH_values
    # mesacna prognoza
    monthly_forecast = treset[treset.Prediction == 'Forecast'].set_index(['Prediction','MESIAC']) * EOY
    # vypocet rozptylu
    low_forecast = treset[treset.Prediction == 'Forecast'].set_index(['Prediction','MESIAC']) * low
    high_forecast = treset[treset.Prediction == 'Forecast'].set_index(['Prediction','MESIAC']) * high

    # napln skutocnostou
    l = len(current) # dlzka skutocnych dat
    shp = monthly_forecast[:l].shape
    monthly_forecast[:l] = current.values.reshape(shp)
    low_forecast[:l] = current.values.reshape(shp)
    high_forecast[:l] = current.values.reshape(shp)

    # precisti chyby, aby neboli nizsie ako skutocnost 
    last_current = current.values[l-1]
    low_forecast[l:] = np.maximum(low_forecast[l:], last_current)
    high_forecast[l:] = np.maximum(high_forecast[l:], last_current)

    # spolocny frame
    prediction = pd.concat({'Forecast':monthly_forecast, 'Low':low_forecast, 'High':high_forecast}, names=['Prediction']).droplevel([1],axis=0)
    return prediction

def prepare_budget():
    """
    Metoda ktora stiahne aktualny rozpocet a pripravi pre dalsie spracovanie
    """
    # get current budget
    aktual_rozpocet = rozpocet(export_day=1,return_frame=True)
    max_month = aktual_rozpocet.MESIAC.max() # get most current month 

    # dpfo budget
    _dpfo = aktual_rozpocet[(aktual_rozpocet.EK3 == '111') & (aktual_rozpocet.MESIAC == max_month)]
    dpfo_rozpocet = _dpfo.UPRAVENY.values[0] # only need one value

    # miestne dane budget
    _dane = aktual_rozpocet[(aktual_rozpocet.EK2.isin(['12','13'])) & (aktual_rozpocet.MESIAC == max_month)]
    dane_rozpocet = _dane.groupby(['EKRK'])['UPRAVENY'].sum() # budget at EKRK level

    # nedanove budget
    _nedane = aktual_rozpocet[(aktual_rozpocet.EK2.str[0].isin(['2','3'])) & (aktual_rozpocet.MESIAC == max_month)]
    nedane_rozpocet = _nedane.groupby(['EK3'])['UPRAVENY'].sum()

    return dpfo_rozpocet, dane_rozpocet, nedane_rozpocet

def budget_daily(budget, trends):
    """
    Metoda, ktora vypocita ocakavanie pre denne prijmy na zaklade aktualneho rozpoctu 
    """
    level_ix = list(set(trends.columns).intersection(set(budget.index))) # intersection of levels 
    
    # filter trends and budget
    filtered_trend = trends[level_ix]
    filtered_budget = budget[level_ix]

    # multiply eoy budget by daily trends
    daily = filtered_trend * filtered_budget
    return daily

def budget_dpfo(dpfo_budget, dpfo_trends):
    """
    Metoda, ktora vypocita mesacne ocakavanie pre rozpoctovane DPFO
    """
    monthly = dpfo_budget * dpfo_trends
    return monthly
    
def multiple_forecasts(current_files, trend_files, weight_files):
    """
    Metoda, ktora pre kazdu sadu dat (podla urovne podrobnosti) vypocita zvlast prognozu
    - DPFO: na mesacnej baze
    - danove prijmy: na urovni EKRK (EK6)
    - nedanove prijmy a bezne transfery: na urovni EK3
    Prognozy vrati ako list pandas dataframes 
    """
    
    predictions = [] * len(current_files)
    for group in np.arange(len(current_files)):
        predictions[group] = eoy_forecast(current_files[group], trend_files[group], weight_files[group])
    return predictions

def write_forecasts(sql_engine, multi_forecasts, multi_budgets, order=['DPFO','danove','nedanove']):
    """
    Metoda, ktora zapise vsetky aktualne prognozy do SQL databazy
    multi_forecasts = list pandas dataframov
    multi_budgets = list pandas dataframov rozpoctov 
    """

    # get current time
    timestamp = pd.to_datetime(time.strftime("%m-%d-%Y %H:%M:%S",time.localtime()))

    # write forecasts
    for f in np.arange(len(multi_forecasts)):
        if order[f] == 'DPFO':
            forecast = multi_forecasts[f].reset_index().melt(id_vars=['Prediction','MESIAC'], var_name='EKRK')
        else:
            forecast = multi_forecasts[f].reset_index().melt(id_vars=['Prediction','MESIAC','DEN'], var_name='EKRK')
        forecast['PREDICTION_TIME'] = timestamp
        forecast.to_sql(order[f], sql_engine, None, if_exists='append', index=False)
    
    # write budgets
    for b in np.arange(len(multi_forecasts)):
        if order[b] == 'DPFO':
            budget = multi_budgets[b].reset_index().melt(id_vars=['Prediction','MESIAC'], var_name='EKRK')
        else:
            budget = multi_budgets[b].reset_index().melt(id_vars=['Forecast','MESIAC','DEN'], var_name='EKRK')
        budget['PREDICTION_TIME'] = timestamp
        budget.to_sql(f'budget_{order[b]}', sql_engine, None, if_exists='append', index=False)
    
    # write entry into PREDICTIONS table
    timestamp_df = pd.DataFrame({'PREDICTION_TIME':timestamp},index=[0])
    timestamp_df.to_sql('PREDICTIONS',sql_engine, None, if_exists='replace', index=False)
    return None

def master_predict():
    """
    Wrapper metoda ktora spusti cely forecast a ulozi data do databazy
    """

    # get current data
    curr_data = load_current_sql()

    # group data
    dpfo, dan, nedan = group_current(curr_data)

    # get trends
    dpfo_trends = pd.read_pickle('dpfo_trends.pkl')
    dan_trends = load_trend(dan, 'danove_trends.pkl')
    nedan_trends = load_trend(nedan, 'nedanove_trends.pkl')

    # weights for local taxes
    w = {}
    for ekrk in dan.columns:
        w[ekrk] = 1.
    w['121001'] = 0.5 # 50:50 for property taxes
    w['121002'] = 0.5 # 50:50 for property taxes
    w['121003'] = 0.5 # 50:50 for property taxes
    w['133013'] = 0.9 # 90:10 for waste levy

    # forecasts
    predictions = [None,None,None]
    todayday = date.today().day
    if todayday < 24:
        dpfo_to_predict = dpfo.head(len(dpfo)-1) # if it is too early, dont predict DPFO for the current month
    else:
        dpfo_to_predict = dpfo
    predictions[0] = dpfo_forecast(dpfo_to_predict, dpfo_trends) # dpfo 
    predictions[1] = eoy_forecast(dan, dan_trends, w) # local taxes
    predictions[2] = eoy_forecast(nedan, nedan_trends) # non tax revenue

    # ziskaj rozpocty
    dpfo_rozpocet, dan_rozpocet, nedan_rozpocet = prepare_budget()
    # spracuj rozpocty
    budgets = [None, None, None]
    budgets[0] = budget_dpfo(dpfo_rozpocet, dpfo_trends)
    budgets[1] = budget_daily(dan_rozpocet, dan_trends)
    budgets[2] = budget_daily(nedan_rozpocet, nedan_trends)

    # write forecasts
    engine = psql_connect()
    try:
        write_forecasts(engine, predictions, budgets)
        print('SUCCESS: Forecast written to database')
        return True
    except:
        raise('Error in writing forecast to database')


if __name__ == "__main__":
    """
    Ked sa program spusti cez command line
    bez argumentov
    """

    master_predict()
    
    