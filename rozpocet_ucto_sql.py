import sys
import numpy as np
import pymssql as sql
import pandas as pd

from defaults import *
from sql_rozpocet import *
from author import *

"""
Postup:
1. pripoj sa cez sql_connect
2. stiahni raw data cez sql_ucto
3. vycisti data cez clean_sql_ucto
4. ak chces automaticky stiahnut a cistit viac mesiacov = ucto_builder
"""

def sql_connect():
    """
    Pripoj sa na Noris SQL server 
    Autentifikacia v module "author"
    Vrati pripojenie a cursor
    """
    conn = sql.connect(SERVER, USER, PW, DB)
    cursor = conn.cursor(as_dict=True)
    return conn, cursor

def sql_ucto(cursor, m_start, m_end, rok, top=None):
    """
    Z Noris SQL databazy stiahni individualne uctovne zaznamy do rozpoctu
    Teda ide o uctovne zaznamy, ktore su uctovane proti uctom v triede 8 
    a da s z uctu vycitat rozpoctova klasifikacia

    Metoda je postavena vzdy na urovni jedneho roku a mesacnej baze 

    Top = prvych X vysledkov

    Vrati pandas dataframe
    """
    q = sql_ucto_rozpocet(m_start, m_end, rok, top)
    cursor.execute(q)
    data = cursor.fetchall()
    return pd.DataFrame(data) 


def clean_sql_ucto(ucto_data, redo=False):
    """
    Vycisti data, stiahnute metodou "sql_ucto"
    Redo = ak sa cistenie opakuje, niektore kroky sa preskocia 
    Vrati vycistene data ako pandas dataframe
    """
    clean_ucto = ucto_data.copy()

    # premenuju sa premenne, ak ide o nove cistenie
    if not redo: 
        clean_ucto.columns = DEFAULT_UCTO_NAMES

    # ucet sa rozdeli zlozky cez whitespace
    rozbite = clean_ucto.UCET.str.split(None, 3, True)
    if rozbite.shape[1] < 4:
        clean_ucto[['UCET_KOD','EKRK','PROGRAM']] = rozbite
        clean_ucto['OVERFLOW'] = None
    else:
        clean_ucto[['UCET_KOD','EKRK','PROGRAM','OVERFLOW']] = rozbite
    clean_ucto['PV'] = clean_ucto['UCET_KOD'].apply(lambda x: 'P' if x[:3] == '803' else 'V')
    
    # UCET_KOD : max 8 chars
    clean_ucto['UCET_OF'] = clean_ucto.UCET_KOD.str[8:] #potential EKRK here
    clean_ucto.UCET_KOD = clean_ucto.UCET_KOD.str[:8] #this should be final

    # fix EKRK
    clean_ucto['EKRKFIX'] = clean_ucto[['UCET_OF','EKRK','PROGRAM']].apply(lambda x: x[1] if len(x[0]) == 0 else x[0] if x[2] is None else x[1], axis=1)
    clean_ucto['FIXED'] = clean_ucto.EKRK != clean_ucto.EKRKFIX
    clean_ucto['PROGRAM'] = clean_ucto[['EKRK','FIXED','PROGRAM']].apply(lambda x: x[0] if x[1] else x[2], axis=1)
    # PROGRAM : max 7 chars
    clean_ucto['PRG_OF'] = clean_ucto.PROGRAM.str[:-7]
    clean_ucto.PROGRAM = clean_ucto.PROGRAM.str[-7:] # this should be final
    clean_ucto['OF_PRG'] = clean_ucto.OVERFLOW.str[:7]


    # EKRK : max 6 chars
    clean_ucto['EKRK_OF'] = clean_ucto.EKRKFIX.str[6:] # KZ and PROGRAM here potentially
    clean_ucto['KZ'] = clean_ucto.EKRK_OF.str[:4]
    clean_ucto['KZ_OF'] = clean_ucto.EKRK_OF.str[4:] # PROGRAM here possibly
    clean_ucto['EKRK_MAX6'] = clean_ucto.EKRKFIX.str[:6] 

    clean_ucto['PRG_FIX'] = clean_ucto[['PROGRAM','KZ_OF','OF_PRG', 'EKRK_MAX6', 'OVERFLOW']].apply(lambda x: x[0] if x[0] is not None and (x[4] is None or len(x[4]) < 3) else x[1] if len(x[3]) > 3 else x[2], axis=1)
    
    clean_ucto['PROGRAM'] = clean_ucto['PRG_FIX'] 
    clean_ucto['EKRK'] = clean_ucto['EKRK_MAX6']
    clean_ucto['EK1'] = clean_ucto.EKRK.str[:1]
    clean_ucto['EK3'] = clean_ucto.EKRK.str[:3]
    clean_ucto['PROG'] = clean_ucto.PROGRAM.str[:3]
    clean_ucto['ROK'] = clean_ucto.DATUM_UCTOVANIA.dt.year
    clean_ucto['MESIAC'] = clean_ucto.DATUM_UCTOVANIA.dt.month
    clean_ucto['DEN'] = clean_ucto.DATUM_UCTOVANIA.dt.day

    clean_ucto['SUMA'] = clean_ucto.SUMA.astype(np.float64)

    # vymazu sa zbytocne stlpce ak ide o nove cistenie 
    if not redo:
        clean_ucto.drop(['SID','OID','PID','SUBJEKT','RIADOK','SUBJEKT_NAZOV','SUBJEKT_REFERENCIA1', 'SUBJEKT_REFERENCIA2'], axis=1, inplace=True)
    clean_ucto.drop(['OVERFLOW','PRG_FIX','EKRKFIX','UCET_OF','FIXED','PRG_OF','OF_PRG','KZ_OF','EKRK_OF','EKRK_MAX6'],axis=1,inplace=True)
    return clean_ucto

def ucto_builder(cursor, r_start, r_end):
    """
    Vytvorenie master databazy napriec rokmi 
    Wrapper pre funkcie sql_ucto a clean_sql_ucto
    Ulozi pkl a vrati pandas dataframe 
    """
    master = None
    for rok in np.arange(r_start, r_end+1):
        for mesiac in np.arange(1,13):
            raw = sql_ucto(cursor, mesiac, mesiac, rok)
            clean = clean_sql_ucto(raw)
            if master is None:
                master = clean
            else:
                master = master.append(clean, ignore_index=True)
            master.to_pickle('uctomaster.pkl')
    return master

if __name__ == "__main__":
    """
    Argument structure
    1. flag: 
        -m: individual export of selected months
        -y: group export of multiple years
    2. start (month or year)
    3. end (month or year)
    4. year (when -m flag was used)

    """ 

    try:
        arguments = sys.argv[1:]
    except IndexError:
        raise SystemExit("No arguments given")

    flag = arguments[0]
    if flag == '-m':
        try:
            start = int(arguments[1])
            end = int(arguments[2])
            year = int(arguments[3])
        except:
            raise SystemExit("Wrong or insufficient arguments given")

        c, cu = sql_connect()
        result = sql_ucto(cu, start, end, year)
        c.close()
        clean = clean_sql_ucto(result)
        filename = f'rozpocet_{start}_{end}_{year}.'
        clean.to_csv(f'{filename}csv', index_label="ID")
        clean.to_pickle(f'{filename}pkl')
        print(f'{filename}.csv saved as CSV')

    elif flag == '-y': 
        try:
            start = int(arguments[1])
            end = int(arguments[2])
        except:
            raise SystemExit("Wrong or insufficient arguments given")

        c, cu = sql_connect()
        master_data = ucto_builder(cu, start, end) 
        c.close()
        filename = f'rozpocet_{start}_{end}.'
        master_data.to_csv(f'{filename}csv', index_label="ID")
        master_data.to_pickle(f'{filename}pkl')
        print(f'{filename}.csv saved as CSV')

    else:
        raise SystemExit("Invalid flag")
