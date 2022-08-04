import sys
import pandas as pd
from defaults import *


def skeleton(r_start, r_end):
    """
    Vytvori prazdny skeleton vsetkych dni v rozpati rokov, ktore su ako vstup metody
    Vrati pandas dataframe so styrmi stlpcami 
    """
    dates = pd.date_range(start=f"1/1/{r_start}",end=f"12/31/{r_end}")
    datedf = {'DATUM_UCTOVANIA': dates, 'ROK': dates.year, 'MESIAC': dates.month, 'DEN': dates.day}
    return pd.DataFrame(datedf)

def grouper(ucto_data, levels=['PROG','EK1']):
    """
    Metoda, ktora zgrupi rozpocet na urovni dni a levelov, ktore su ako argument
    Vrati pandas dataframe so sumami aj kumulativnymi sumami 
    """
    columns = ['DATUM_UCTOVANIA','ROK','MESIAC','DEN','MDD','PV'] + levels
    # vytvor nove EK stlpce ak nie su 
    ucto_data['EK2'] = ucto_data.EKRK.str[:2]
    g = ucto_data.groupby(columns)['SUMA'].sum()
    g = g.reset_index()
    # nechaj len skutocnost, nie rozpocet (skutocne vydavky su na D, skutocne prijmy na M)
    filter = g.loc[((g.MDD == 'D') & (g.PV == 'V')) | ((g.MDD == 'M') & (g.PV == 'P'))]
    filter.drop(['MDD'],axis=1,inplace=True)
    # kumulativne sumy 
    filter_columns = ['ROK','PV'] + levels
    filter['CUMSUM'] = filter.groupby(filter_columns)['SUMA'].cumsum()
    return filter

def mass_grouper(pickles, levels=['PROG','EK1']):
    """
    Wrapper metoda, ktora prebehne pickles zo zoznamu a vsetky zgrupuje
    Vrati pandas dataframe a ulozi ako pickle
    """
    master = None
    for pick in pickles:
        g = grouper(pd.read_pickle(pick), levels)
        if master is None:
            master = g
        else:
            master = master.append(g, ignore_index=True)
    
    return master

def finalize(master_data, skeleton, levels):
    """
    Metoda ktora vytvori dataset s udajmi v kazdom dni mesiaca
    """

    master_pivot = master_data.drop(['ROK','MESIAC','DEN'], axis=1).groupby(['DATUM_UCTOVANIA'] + levels)['CUMSUM'].sum().reset_index().pivot(index='DATUM_UCTOVANIA',columns=levels,values='CUMSUM').reset_index()
    max_datum = master_pivot.DATUM_UCTOVANIA.max()
    skeleton = skeleton[skeleton.DATUM_UCTOVANIA <= max_datum]
    final_df = skeleton.merge(master_pivot, how='left', on='DATUM_UCTOVANIA')
    final_df[(final_df.MESIAC == 1) & (final_df.DEN == 1)] = final_df[(final_df.MESIAC == 1) & (final_df.DEN == 1)].fillna(0.)
    final_df = final_df.fillna(method='pad', axis=0)
    melted_df = final_df.melt(id_vars=['DATUM_UCTOVANIA','ROK','MESIAC','DEN'], var_name=levels[-1], value_name='CUMSUM') 
    return melted_df


if __name__ == "__main__":
    """
    Argument structure
    1. list of pickle file names 
    2. flag -l
    3. list of levels

    if -l not passed, default levels are used
    """ 

    try:
        arguments = sys.argv[1:]
    except IndexError:
        raise SystemExit("No arguments given")

    if '-l' in arguments:
        flag_position = arguments.index('-l') + 1
        pickles = arguments[:(flag_position-1)]
        try:
            levels = arguments[flag_position:]
        except:
            levels = ['EK3']
    else:
        pickles = arguments
        levels = ['EK3']

    grouped = mass_grouper(pickles, levels)
    years = pd.unique(grouped.ROK)
    skelet = skeleton(years.min(), years.max())
    final = finalize(grouped, skelet, levels)
    filename = f'grouped_budget_{years.min()}_{years.max()}_{levels[-1]}.'
    final.to_pickle(f'{filename}pkl')
    final.to_csv(f'{filename}csv', index_label='ID')
    print(f'{filename} saved as CSV and PKL')
  
  

