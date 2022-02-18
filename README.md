# Predikčný model príjmov rozpočtu HMBA

Tento model slúži na odhad vývoja bežných príjmov rozpočtu HMBA v rámci jedného rozpočtového roka. Vychádza z historických denných/mesačných trendov vývoja bežných príjmov. Obsahuje aj interval spoľahlivosti. Slúži pre rýchlu diagnostiku vývoja príjmov. Aktualizovať je možné ho denne.  

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

###
