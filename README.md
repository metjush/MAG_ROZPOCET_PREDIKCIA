# Predikčný model príjmov rozpočtu HMBA

Tento model slúži na odhad vývoja bežných príjmov rozpočtu HMBA v rámci jedného rozpočtového roka. Vychádza z historických denných/mesačných trendov vývoja bežných príjmov. Obsahuje aj interval spoľahlivosti. Slúži pre rýchlu diagnostiku vývoja príjmov. Aktualizovať je možné ho denne.  

Vopred sa ospravedlňujem za nekonzistentné miešanie angličtiny a slovenčiny v kóde, komentároch aj názvoch premenných. 

## Netechnický opis spôsobu výpočtu predikcie
Model počíta tri rôzne predikcie pre bežné príjmy: 
- Predikciu podielových daní (DPFO) na mesačnej báze
- Predikciu miestnych daní na dennej báze, v podrobnosti ekonomickej podpoložky (šesťmiestny kód ekonomickej klasifikácie)
- Predikciu nedaňových príjmov a bežných transferov, v podrobnosti ekonomickej položky (trojmiestny kód ekonomickej klasifikácie)

Predpoveď vychádza z aktuálne zaúčtovaného plnenia rozpočtu v jednotlivých ekonomických položkách pre deň, v ktorom bolo plnenie sianuté z databázy. 
Výpočet koncoročnej prognózy aj denného vývoja vychádza z historických trendov (v percentách) pre jednotlivé ekonomické položky, za roky 2013-2018 (trénovacia sada). 
Kombinácia trendov z týchto rokov (vážený priemer) bola vypočítaná s váhami, ktoré najlepšie predikovali vývoj v rokoch 2019-2021 (testovacia sada). 
Pre každú ekonomickú položku je vypočítaná aj horná a spodná hranica intervalu (chyby odhadu). 

DPFO je počítané rovnakým spôsobom, akurát na mesačnej báze, keďže podiel na DPFO prichádza mestu iba raz mesačne. 

Predikcia je vo výslednej prezentácií porovnávaná s aktuálnym rozpočtom, platným v deň výpočtu prognózy. 

## Technický postup výpočtu predikcie
### Zdroje dát
Všetky rozpočtové dáta pochádzajú z databázy IS Noris, ekonomického softvéru HMBA. Táto databáza je prístupná iba cez internú sieť/VPN HMBA, kód preto nie je možné spustiť kdekoľvek. 

Databáza je postavená na MS SQL, jednotlivé dotazy na stiahnutie dát sú v súbore `sql_rozpocet.py`. Pre stiahnutie denných dát sa používa metóda `sql_ucto_rozpocet()`. Pre stiahnutie aktuálne platného rozpočtu sa používa metóda `sql_rozpocet()`.

Autorizačné údaje sú uložené v súbore `author.py`, ktorý nie je nahraný na githube kvôli citlivým údajom.

### Základné čistenie dát
Metódy na prvé stiahnutie a spracovanie dát sú v súbore `rozpocet_ucto_sql.py`. Metoda `sql_connect()` sa pripojí na MS SQL databázu, `sql_ucto()` stiahne dáta z databázy podľa zadaného roku a mesiacov, za ktoré sa majú dáta stiahnuť. Metóda `clean_sql_ucto()` individuálne dáta spracuje, vyčistí, pripraví nové premenné pre ďalšiu analýzu. Metóda `ucto_builder()` predstavuje wrapper metódu pre ostatné metódy, umožňuje viacročné stiahnutie dát naraz. 

### Príprava dát na výpočet trendov a predpovedí
Po stiahnutí dát z databázy je potrebné ich spracovať do štruktúry denného plnenia/čerpania. V súbore `group_rozpocet_ucto.py` sú implementované potrebné metódy. Metóda `skeleton()` vytvorí prázdnu kostru so stĺpcami `ROK, MESIAC, DEN` pre každý deň za riešené obdobie. Metóda `grouper()`, resp. `mass_grouper()` pre viac súborov naraz, spracuje stiahnuté dáta do agregátnej podoby v podrobnosti `ROK, MESIAC, DEN, MDD, PV` a požadovaných úrovní ekonomickej klasifikácie (`levels`). Stĺpec `MDD` predstavuje účtovnú indikáciu má dať/dal a `PV` predstavuje kategorickú premennú príjem/výdavok. `levels` môžu byť ďalšie stĺpce pôvodnej spracovanej databázy:
- `EK1` (ekonomická trieda x00)
- `EK2` (ekonomická kategória xx0)
- `EK3` (ekonomická položka xxx)
- `EKRK` (ekonomická podpoložka xxx xxx)
- `PROG` (program rozpočtu, iba pre výdavky)

Metóda `finalize()` agregované dáta spojí s pripravenou dennou kostrou a vráti denné agregované dáta v dlhom formáte.

### Výpočet trendov 
Metódy pre výpočet denných trendov sú implementované v súbore `build_trends.py`. Postup výpočtu je nasledovný:
1. Metóda `load_history()` načíta dáta z predošlých krokov podľa definovaných `levels`
2. Metóda `share_calculation()` vypočíta denné percentuálne trendy pre jednotlivé úrovne a roky 
3. Metóda `train_test_split()` rozdelí historické dáta podľa rokov na trénovaciu sadu a testovaciu sadu
4. Metóda `optimize_train()` z trénovacej sady vypočíta optimálne váhy jednotlivých rokov, ktoré najlepšie predikujú vývoj v testovacích rokoch (viď nižšie)
5. Metóda `build_trends()` vypočíta optimálne denné trendy pre jednotlivé úrovne, ktoré budú používané pre prognózu vývoja v aktuálnom roku 

Výpočet optimálnych váh pre jednotlivé roky je implementovaný v metódach `compute_prediction()`, `compute_error()`, `trainer()` a zabalený v metóde `optimize_train()`. 

Vstupom sú percentuálne denné váhy jednotlivých rokov a úrovní (ekonomických kategórií) - 3D tenzor `train` v tvare `(roky, dni, kategorie)`. Výpočet vážených denných trendov je súčinom tenzoru `train` a vektoru `coefs`, ktorý obsahuje váhy od `0.0` do `1.0` pre jednotlivé roky (tvar `(1, roky)`). Výsledkom je 2D matica denných trendov pre jednotlivé ekonomické kategórie (tvar `(dni, kategorie)`).

Optimálne váhy sú vypočítané lineárnym programovaním, metódou `SLSQP`, ktorá minimalizuje odchýlku oproti testovacej sade, počítanú ako `root mean squared error (RMSE)` v metóde `compute_error()`. Reziduálne hodnoty (rozdiel medzi skutočnými dennými trendami v testovacej sade a odhadnutými trendami z trénovacej sady) sú použité pre výpočet intervalu spoľahlivosti prognózy. 

Výsledkom je databáza denných trendov a chýb prognózy pre jednotlivé ekonomické kategórie. V našej implementácii počítame trendy pre miestne dane na úrovni `EKRK` a pre nedaňové príjmy a bežné transfery na úrovni `EK3`. Podielové dane (DPFO) sú počítané rovnako ako je opísané vyššie, ale na mesačnej úrovni. 

Použité trendy sú na githube uložené v `pickle` formáte:
- DPFO: `dpfo_trends.pkl`
- Miestne dane: `danove_trends.pkl`
- Nedaňové príjmy a bežné transfery: `nedanove_trends.pkl`

### Výpočet prognózy v aktuálnom roku
Súbor `predict.py` je záverečným súborom, ktorý implementuje výpočet prognózy pre aktuálny rok. Využíva mnohé z metód, opísaných vyššie pre spracovanie dát aktuálneho roku. Celý proces je zhrnutý v metóde `master_predict()`:
1. Metódou `load_current_sql()` stiahne aktuálne plnenie/čerpanie rozpočtu k dnešnému dňu z databázy IS Noris
2. Metódou `group_current()` spracuje, vyčistí a agreguje dáta do trochu kategórii (DPFO, miestne dane, nedaňové príjmy)
3. Načíta trendy z `pickle` súborov metódou `load_trend()`
4. Zohľadní váhy pre vybrané miestne dane, ktoré sa delia medzi mesto a mestské časti (daň z nehnuteľnosti a poplatok za odpad) v premennej `w`
5. Vypočíta prognózu na základe aktuálneho plnenia a optimálnych trendov metódami `dpfo_forecast()` a `eoy_forecast()`
6. Stiahne aktuálne platný rozpočet metódou `prepare_budget()`
7. Spracuje aktuálne rozpočty pre porovnanie s prognózou metódami `budget_dpfo()` a `budget_daily()`
8. Uloží prognózy a aktuálne rozpočty do vlastnej PostgreSQL databázy metódou `write_forecasts()`

PostgreSQL databáza je prístupná aj mimo VPN HMBA a používa sa pre vytvorenie interaktívneho dashboardu cez Deepnote (viď nižšie). 
Zatiaľ je potrebné ho spúšťať ručne. 

### Vybudovanie interaktívneho dashboardu v Deepnote
Cez [Deepnote](https://deepnote.com/) vytvárame interaktívny dashboard, kde je možné vidieť aktuálnu prognózu v grafoch, v porovnaní s aktuálnym rozpočtom. 

Notebook `PSQL_CONNECTOR` cez vstavanú integráciu na Postgres databázu stiahne aktuálnu prognózu a rozpočty pre jednotlivé kategórie príjmov. Kód notebooku je dostupný [tu](https://deepnote.com/project/PSQLCONNECTOR-Bw1iOLjlSAu733MDnPJSTA/%2FDB_connector.ipynb). Následne do "shared dataset" v Deepnote uloží stiahnuté dáta ako `pickle`. Tento notebook je automaticky aktualizovaný každý deň. 

Následne sa každý deň aktualizuje aj [dashboard](https://deepnote.com/@matus-luptak/budgetprediction-NRkVPStbR4GFuA0ybYSf6g), kde sú vizualizované predikcie pre DPFO, miestne dane aj nedaňové príjmy v grafoch. 


