import numpy as np
import pandas as pd

# globals

DEFAULT_NAMES_SQL = [
    'SID', 'ROK', 'MESIAC', 'PV', 'R', 
    'PROGRAM', 'PODPROGRAM', 'PRVOK', 
    'COFOG', 'EKRK', 'KZ', 'POPIS', 
    'SCHVALENY', 'UPRAVENY', 'CERPANIE', 'PCT_CERPANIE', 
    'UTVAR_ID', 'UTVAR_NAZOV', 'PLATNOST', 'CAST'
]

DEFAULT_UCTO_NAMES = [
    'SID', 'OID', 'PID', 'SUBJEKT', 'RIADOK', 'STAV', 
    'UCET', 'MDD', 'SUMA', 'DATUM_UCTOVANIA', 'SUBJEKT_NAZOV',
    'SUBJEKT_REFERENCIA1', 'SUBJEKT_REFERENCIA2',
    'POZNAMKA', 'DATUM_VZNIKU' 
]

DEFAULT_NAMES = [
    'ROK', 'MESIAC', 'PV', 'R', 
    'PROGRAM', 'PODPROGRAM', 'PRVOK', 
    'COFOG', 'EKRK', 'KZ', 'POPIS', 
    'SCHVALENY', 'UPRAVENY', 'CERPANIE', 'PCT_CERPANIE', 
    'UTVAR_ID', 'UTVAR_NAZOV'
]
DEFAULT_DICT = {
    'ROK': np.int,
    'MESIAC': np.int,
    'PV': np.str,
    'R': np.str,
    'PROGRAM': np.str,
    'PODPROGRAM': np.str,
    'PRVOK': np.str,
    'COFOG': np.str,
    'EKRK': np.str,
    'KZ': np.str,
    'POPIS': np.str,
    'SCHVALENY': np.float64,
    'UPRAVENY': np.float64,
    'CERPANIE': np.float64,
    'PCT_CERPANIE': np.str,
    'UTVAR_ID': np.str,
    'UTVAR_NAZOV': np.str
}

DEFAULT_CI = 4.5

IMPORT_DIR = 'IMPORT'
USED_DIR = 'USED'

LEVELS = {
    'EKRK (6 miest)': 'EKRK',
    'EKRK (3 miesta)': 'EK3',
    'EKRK (2 miesta)': 'EK2'
}

LEVELS_INT = {
    2: 'EK2',
    3: 'EK3',
    6: 'EKRK'
}



COEFFICIENTS = {
    '121': 0.5
}



EKRK_DICT = {
    '11' : ["Daň z príjmov fyzických osôb", "BP"],
    '111': ["Daň z príjmov fyzických osôb", "BP"], 
    '12' : ["Daň z nehnuteľnosti", "BP"], 
    '121': ["Daň z nehnuteľnosti", "BP"], 
    '13': ["Miestne dane", "BP"],
    '133': ["Miestne dane", "BP"], 
    '21': ["Príjmy z podnikania a majetku", "BP"], 
    '211': ["Dividendy", "BP"], 
    '212': ["Príjmy z prenájmu", "BP"], 
    '22': ["Administratívne príjmy", "BP"], 
    '221': ["Správne poplatky", "BP"], 
    '222': ["Pokuty a penále", "BP"], 
    '223': ["Príjmy za služby", "BP"], 
    '229': ["Iné poplatky", "BP"],
    '23': ["Kapitálové príjmy", "KP"], 
    '231': ["Kapitálové príjmy (predaj majetku)", "KP"],
    '233': ["Kapitálové príjmy (predaj pozemkov)", "KP"],
    '239': ["Ďalšie kapitálové príjmy", "KP"],
    '24': ["Príjmové úroky", "BP"], 
    '243': ["Príjmové úroky", "BP"],
    '244': ["Príjmové úroky", "BP"],
    '29': ["Iné príjmy", "BP"], 
    '291': ["Iné nedaňové príjmy", "BP"],
    '292': ["Iné ostatné príjmy", "BP"],
    '31': ["Bežné transfery a dotácie", "BP"], 
    '311': ["Granty a dotácie", "BP"],
    '312': ["Bežné transfery", "BP"],
    '32': ["Kapitálové transfery a dotácie", "KP"], 
    '321': ["Kapitálové granty", "KP"],
    '322': ["Kapitálové transfery", "KP"],
    '331': ["Zahraničné granty (bežné)", "BP"],
    '332': ["Zahraničné granty (kapitálové)", "KP"],
    '453': ["","FO"],
    '454': ["","FO"],
    '456': ["","FO"],
    '513': ["","FO"],
    '514': ["","FO"],
    '521': ["","FO"],
    '523': ["","FO"],
    'RO': ["Príjmy rozpočtových organizácií","BP"]
}


EKRK_MASTER = pd.DataFrame(EKRK_DICT).T 
EKRK_MASTER.columns = ['Popis','Kategoria']

API_BASE = "https://rozpocet.sk/api/sam/"
BA_UROVEN = "582000"
HMBA_URAD = "00603481"
RO = ['00490873','00641405','00896276','30775205','30779278','30791898',
'30842344','31755534','31768857','31769403','31780334','31780725',
'31780873','31810209','31810519','31811027','31816088','31816118',
'36067211','36067253','36070939','36071323','36071331','37926012','42174970']