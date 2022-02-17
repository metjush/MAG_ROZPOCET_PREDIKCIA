from datetime import date
from pandas import DataFrame
from numpy import float64
import pymssql as sql
from author import *

# GLOBAL PREMENNE

DEFAULT_NAMES_SQL = [
    'SID', 'ROK', 'MESIAC', 'PV', 'R', 
    'PROGRAM', 'PODPROGRAM', 'PRVOK', 
    'COFOG', 'EKRK', 'KZ', 'POPIS', 
    'SCHVALENY', 'UPRAVENY', 'CERPANIE', 'PCT_CERPANIE', 
    'UTVAR_ID', 'UTVAR_NAZOV', 'PLATNOST', 'CAST'
]

EXPORT_DAY = 21 # ktory den mesiaca je dostatocny pre export predosleho mesiaca 

# METODY

def sql_rozpocet(m_start, m_end, rok, PV=None):
    """
    Metoda, ktora vrati query pre aktualne cerpanie rozpoctu

    Vstupy:
    - m_start (int): v ktorom mesiaci ma export zacat
    - m_end (int): v ktorom mesiaci ma export skoncit (vratane)
    - rok (int): v ktorom roku sledujeme cerpanie
    - PV (str, default None): ak je vyplnene, specifikacia ci chceme vytiahnut iba prijmy (P) alebo iba vydavky (V). ak je None, tahame aj aj. 

    Vystup:
    - query (str): SQL query pre vybratie aktualneho cerpania 
    """

    query_base = """
    SELECT lcs.cerp_rozp_21.cislo_nonsubjektu AS cerp_rozp_21_cislo_nonsub,
    ((select lcs.nf_obd2kodobd(lcs.cerp_rozp_21.obdobie))) AS cerp_rozp_21_Exp57614393,
    lcs.cerp_rozp_21.mesiac AS cerp_rozp_21_mesiac,
    ((case when left(gstab_1_1.nazev_subjektu,
    3)='803' then 'P' when left(gstab_1_1.nazev_subjektu,
    3)='802' then 'V' else '?' end)) AS ucet_Exp49561726,
    ((case when substring(gstab_1_1.nazev_subjektu,
    26 ,
    1) like 'M' then 'N' else 'R' end)) AS ucet_Exp62069969,
    (substring(gstab_1_1.nazev_subjektu,
    19 ,
    3 )) AS ucet_Exp35498395,
    (substring(gstab_1_1.nazev_subjektu,
    22 ,
    2 )) AS ucet_Exp14385085,
    (substring(gstab_1_1.nazev_subjektu,
    24 ,
    2 )) AS ucet_Exp20789728,
    (substring(gstab_1_1.nazev_subjektu,
    4 ,
    5 )) AS ucet_Exp73737352,
    (substring(gstab_1_1.nazev_subjektu,
    9 ,
    6 )) AS ucet_Exp55085055,
    (substring(gstab_1_1.nazev_subjektu,
    15,
    4)) AS ucet_Exp61517976,
    gstab_1_2.popis AS gstab_1_2_popis,
    lcs.cerp_rozp_21.schvaleny AS cerp_rozp_21_schvaleny,
    (lcs.cerp_rozp_21.schvaleny+lcs.cerp_rozp_21.zvysenie-lcs.cerp_rozp_21.znizenie) AS cerp_rozp_21_Exp60265715,
    lcs.cerp_rozp_21.suma_kumulovana AS cerp_rozp_21_suma_kumulovana,
    ((case when (lcs.cerp_rozp_21.schvaleny+lcs.cerp_rozp_21.zvysenie-lcs.cerp_rozp_21.znizenie) = 0 then  'bez rozpoctu' else   convert(varchar,
    (lcs.cerp_rozp_21.suma_kumulovana*100/(lcs.cerp_rozp_21.schvaleny+lcs.cerp_rozp_21.zvysenie-lcs.cerp_rozp_21.znizenie)))+ '%' end)) AS cerp_rozp_21_Exp32182883,
    gstab_1_3.reference_subjektu AS gstab_1_3_reference_subjektu,
    gstab_1_3.nazev_subjektu AS gstab_1_3_nazev_subjektu,
    gstab_1_4.platnost_do21 AS gstab_1_4_platnost_do21,
    gstab_1_5.cast AS gstab_1_5_cast 
    FROM lcs.cerp_rozp_21 
    LEFT 
    OUTER 
    JOIN lcs.subjekty gstab_1_1 /*213158;l;*/ 
    ON lcs.cerp_rozp_21.ucet=gstab_1_1.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.ucet gstab_1_2 /*213158;l;*/ 
    ON lcs.cerp_rozp_21.ucet=gstab_1_2.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.subjekty gstab_1_3 /*213159;l;*/ 
    ON lcs.cerp_rozp_21.utvar=gstab_1_3.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.utvar gstab_1_4 /*213159;l;*/ 
    ON lcs.cerp_rozp_21.utvar=gstab_1_4.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.uda_21_ucet_rozp gstab_1_5 /*213158;l;*/ 
    ON lcs.cerp_rozp_21.ucet=gstab_1_5.cislo_subjektu 
    WHERE (lcs.cerp_rozp_21.schvaleny+(lcs.cerp_rozp_21.schvaleny+lcs.cerp_rozp_21.zvysenie-lcs.cerp_rozp_21.znizenie)+lcs.cerp_rozp_21.suma_kumulovana <> 0  ) 
    """

    query_rok = f"(((select lcs.nf_obd2kodobd(lcs.cerp_rozp_21.obdobie))) = {rok})"
    query_mesiac = f"(lcs.cerp_rozp_21.mesiac BETWEEN {m_start} AND {m_end})"
    if PV is not None:
        query_PV = f" and (((case when left(gstab_1_1.nazev_subjektu, 3)='803' then 'P' when left(gstab_1_1.nazev_subjektu, 3) ='802' then 'V' else '?' end)) LIKE '{PV}%') "
    else:
        query_PV = ""
    
    query = query_base + " and " + query_rok + " and " + query_mesiac + query_PV
    return query


def sql_connect():
    """
    Pripoj sa na NORIS databazu pomocou pymssql
    Pristupove udaje su ulozene ako global premenne v author.py

    Vrati:
    - conn (pymssql connection objekt)
    - cursor (pymssql cursor objekt)
    """
    conn = sql.connect(SERVER, USER, PW, DB)
    cursor = conn.cursor(as_dict=True)
    return conn, cursor


def sql_cerpanie(cursor, m_start, m_end, rok=2021, PV=None):
    """
    Vygeneruj a stiahni query pre aktualne cerpanie rozpoctu z pripojenej databazy
    Query sa taha z metody sql_rozpocet 

    Vstupy:
    - cursor: pymssql cursor objekt
    - m_start (int): v ktorom mesiaci ma export zacat
    - m_end (int): v ktorom mesiaci ma export skoncit (vratane)
    - rok (int): v ktorom roku sledujeme cerpanie
    - PV (str, default None): ak je vyplnene, specifikacia ci chceme vytiahnut iba prijmy (P) alebo iba vydavky (V). ak je None, tahame aj aj. 

    Vrati:
    - pandas DataFrame z vystupu z databazy 
    """
    q = sql_rozpocet(m_start, m_end, rok, PV)
    cursor.execute(q)
    data = cursor.fetchall()
    return DataFrame(data)


def clean_sql(sql_data):
    """
    Vycisti stiahnute data o cerpani rozpoctu pre ucely exportu do csv
    Nazvy stplcov su z globalnej premennej DEFAULT_NAMES_SQL

    Vstup:
    - sql_data (pandas DataFrame): dataframe z metody sql_cerpanie()

    Vystup:
    - cleaned (pandas DataFrame): upraveny dataframe, urceny pre export alebo dalsie spracovanie

    """
    cleaned = sql_data.copy()
    cleaned.columns = DEFAULT_NAMES_SQL # premenuj stlpce
    cleaned['SCHVALENY'] = cleaned['SCHVALENY'].astype(float64) # prehod z textu na cislo
    cleaned['UPRAVENY'] = cleaned['UPRAVENY'].astype(float64) # prehod z textu na cislo
    cleaned['CERPANIE'] = cleaned['CERPANIE'].astype(float64) # prehod z textu na cislo
    cleaned['EK3'] = cleaned['EKRK'].str[:3] # pre jednoduchsie filtrovanie, skrat EKRK na 3
    cleaned['EK2'] = cleaned['EKRK'].str[:2] # pre jednoduchsie filtrovanie, skrat EKRK na 2
    cleaned['MINUS'] = cleaned['CERPANIE'] < 0 # pomocna premenna, ci je polozka minusova
    cleaned['POPIS'] = cleaned['POPIS'].str.replace('\s+', ' ',regex=True) # precisti vsetok whitespace na medzeru
    cleaned.drop(['SID', 'PLATNOST', 'CAST'], axis=1, inplace=True) # odstran nepotrebne stlpce
    cleaned.sort_values(['ROK','MESIAC','PV','PROGRAM','PODPROGRAM','PRVOK','EK2'], axis=0,inplace=True,ignore_index=True) # zorad podla mesiacov a programov
    return cleaned


def batch_query(export_name=None, PV=None, export_day=EXPORT_DAY, return_frame=False):
    """
    Wrapper metoda pre komplet proces ziskania cisteho exportu
    Data vycisti a ulozi ako pkl a csv
    CSV je v europskom formate (separator ";" a desatinna ciarka ",")
    
    Vstupy:
    - export_name (str): ak chceme ulozit pod vlastnym menom export, inac sa vytvori standardny. Nazov bez koncovky suboru
    - PV (str): ak je vyplnene, specifikacia ci chceme vytiahnut iba prijmy (P) alebo iba vydavky (V). ak je None, tahame aj aj. 
    
    Vystupy:
    - export_name (str): meno, pod ktorym bol subor ulozeny ako pkl a csv
    """

    # zisti odkedy dokedy exportujeme
    datum = date.today() # aktualny datum 
    if datum.month == 1: # ak je januar, exportujeme este predosly rok
        rok = datum.year - 1
        m_end = 12 - 1*(datum.day < export_day)
    else:
        rok = datum.year 
        m_end = max(datum.month - 1*(datum.day < export_day), 0) # ak je den mensi ako EXPORT_DAY, tak exportujeme skutocnost dva mesiace spatne
        if m_end == 0: # nemame dostatocny datum na januar aktualneho roku
            m_end = 12
            rok = rok - 1
    m_start = 1 # vzdy aktualizuj od zaciatku roka
    
    # ziskaj data
    conn, cursor = sql_connect() # pripoj na databazu
    raw_data = sql_cerpanie(cursor, m_start, m_end, rok, PV) # vytiahni data
    clean_data = clean_sql(raw_data) # vycisti data
    conn.close() # odpoj databazu

    if return_frame:
       return clean_data

    # meno exportu
    if export_name is None:
        export_name = f'cerpanie_{rok}_{m_start}_{m_end}_export_{datum.year}{datum.month}{datum.day}' # standardny nazov
   
    # uloz data
    clean_data.to_pickle(export_name + '.pkl') # uloz backup ako pickle
    clean_data.drop(['MINUS'], axis=1).to_csv(export_name + '.csv', sep=';', decimal=',',float_format='%.3f',index=False,encoding='utf-8-sig') # uloz ako .csv, MINUS premennu pre verejny export netreba
    print(f"Cerpanie uspesne ulozene pod menom {export_name} ako pickle a csv")

    return export_name


if __name__ == "__main__":
    """
    Ked sa program spusti cez command line
    bez argumentov
    """

    batch_query()
    
    