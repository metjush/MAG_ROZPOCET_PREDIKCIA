def sql_rozpocet(m_start, m_end, rok, PV=None):
    """
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


def sql_ucto_rozpocet(m_start, m_end, rok, top=None):
    """
    """
    if top is None:
        query_top = "SELECT "
        query_tail = "ORDER BY 3 ASC, 4 ASC, 5 ASC, 10 ASC"
    else:
        query_top = f"SELECT TOP {top} "
        query_tail = f"ORDER BY 3 ASC, 4 ASC, 5 ASC, 10 ASC OPTION(fast {top})"

    query_base = """
        lcs.subjekt_ucetnipohyb.cislo_subjektu AS subjekt_ucetnipohyb_cislo_subjektu,
        lcs.objekt_ucetnipohyb.cislo_objektu AS x___cislo_objektu___x,
        lcs.objekt_ucetnipohyb.cislo_poradace AS objekt_ucetnipohyb_cislo_poradace,
        lcs.subjekt_ucetnipohyb.reference_subjektu AS subjekt_ucetnipohyb_reference_subjektu,
        lcs.objekt_ucetnipohyb.radka AS objekt_ucetnipohyb_radka,
        lcs.objekt_ucetnipohyb.stav AS objekt_ucetnipohyb_stav,
        gstab_1_1.nazev_subjektu AS gstab_1_1_nazev_subjektu,
        lcs.objekt_ucetnipohyb.tok_objektu AS objekt_ucetnipohyb_tok_objektu,
        lcs.objekt_ucetnipohyb.pocetkc AS objekt_ucetnipohyb_pocetkc,
        lcs.objekt_ucetnipohyb.pripad AS objekt_ucetnipohyb_pripad,
        gstab_1_2.nazev_subjektu AS gstab_1_2_nazev_subjektu,
        gstab_1_3.reference_subjektu AS gstab_1_3_reference_subjektu,
        gstab_1_4.reference_subjektu AS gstab_1_4_reference_subjektu,
        lcs.objekt_ucetnipohyb.poznamka AS objekt_ucetnipohyb_poznamka,
        lcs.subjekty.datum_vzniku AS subjekty_datum_vzniku 
        FROM lcs.subjekty 
        JOIN lcs.subjekt_ucetnipohyb 
        ON lcs.subjekty.cislo_subjektu=lcs.subjekt_ucetnipohyb.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.objekt_ucetnipohyb 
        ON lcs.subjekt_ucetnipohyb.cislo_subjektu=lcs.objekt_ucetnipohyb.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.subjekty gstab_1_1 /*1003;l;*/ 
        ON lcs.objekt_ucetnipohyb.ucet=gstab_1_1.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.organizace gstab_1_2 /*1001;l;*/ 
        ON lcs.objekt_ucetnipohyb.organizace=gstab_1_2.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.subjekty gstab_1_3 /*1113;l;*/ 
        ON lcs.objekt_ucetnipohyb.prvotni_doklad=gstab_1_3.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.subjekty gstab_1_4 /*1002;l;*/ 
        ON lcs.objekt_ucetnipohyb.utvar=gstab_1_4.cislo_subjektu 
        WHERE (LEFT(gstab_1_1.nazev_subjektu, 3) IN (802, 803)) and """
    
    query_rok = f"(YEAR(lcs.objekt_ucetnipohyb.pripad) = {rok}) "
    query_mesiace = f"(MONTH(lcs.objekt_ucetnipohyb.pripad) BETWEEN {m_start} AND {m_end}) "

    query = query_top + query_base + query_rok + "and " + query_mesiace + query_tail
    return query 
        

def sql_vynos_naklad(m_start, m_end, rok, top=None):
    """
    """
    if top is None:
        query_top = "SELECT "
        query_tail = "ORDER BY 3 ASC, 4 ASC, 5 ASC, 10 ASC"
    else:
        query_top = f"SELECT TOP {top} "
        query_tail = f"ORDER BY 3 ASC, 4 ASC, 5 ASC, 10 ASC OPTION(fast {top})"

    query_base = """
        lcs.subjekt_ucetnipohyb.cislo_subjektu AS subjekt_ucetnipohyb_cislo_subjektu,
        lcs.objekt_ucetnipohyb.cislo_objektu AS x___cislo_objektu___x,
        lcs.objekt_ucetnipohyb.cislo_poradace AS objekt_ucetnipohyb_cislo_poradace,
        lcs.subjekt_ucetnipohyb.reference_subjektu AS subjekt_ucetnipohyb_reference_subjektu,
        lcs.objekt_ucetnipohyb.radka AS objekt_ucetnipohyb_radka,
        lcs.objekt_ucetnipohyb.stav AS objekt_ucetnipohyb_stav,
        gstab_1_1.nazev_subjektu AS gstab_1_1_nazev_subjektu,
        lcs.objekt_ucetnipohyb.tok_objektu AS objekt_ucetnipohyb_tok_objektu,
        lcs.objekt_ucetnipohyb.pocetkc AS objekt_ucetnipohyb_pocetkc,
        lcs.objekt_ucetnipohyb.pripad AS objekt_ucetnipohyb_pripad,
        gstab_1_2.nazev_subjektu AS gstab_1_2_nazev_subjektu,
        gstab_1_3.reference_subjektu AS gstab_1_3_reference_subjektu,
        gstab_1_4.reference_subjektu AS gstab_1_4_reference_subjektu,
        lcs.objekt_ucetnipohyb.poznamka AS objekt_ucetnipohyb_poznamka,
        lcs.subjekty.datum_vzniku AS subjekty_datum_vzniku 
        FROM lcs.subjekty 
        JOIN lcs.subjekt_ucetnipohyb 
        ON lcs.subjekty.cislo_subjektu=lcs.subjekt_ucetnipohyb.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.objekt_ucetnipohyb 
        ON lcs.subjekt_ucetnipohyb.cislo_subjektu=lcs.objekt_ucetnipohyb.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.subjekty gstab_1_1 /*1003;l;*/ 
        ON lcs.objekt_ucetnipohyb.ucet=gstab_1_1.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.organizace gstab_1_2 /*1001;l;*/ 
        ON lcs.objekt_ucetnipohyb.organizace=gstab_1_2.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.subjekty gstab_1_3 /*1113;l;*/ 
        ON lcs.objekt_ucetnipohyb.prvotni_doklad=gstab_1_3.cislo_subjektu 
        LEFT 
        OUTER 
        JOIN lcs.subjekty gstab_1_4 /*1002;l;*/ 
        ON lcs.objekt_ucetnipohyb.utvar=gstab_1_4.cislo_subjektu 
        WHERE (LEFT(gstab_1_1.nazev_subjektu, 1) IN (5, 6)) and """
    
    query_rok = f"(YEAR(lcs.objekt_ucetnipohyb.pripad) = {rok}) "
    query_mesiace = f"(MONTH(lcs.objekt_ucetnipohyb.pripad) BETWEEN {m_start} AND {m_end}) "

    query = query_top + query_base + query_rok + "and " + query_mesiace + query_tail
    return query 
        
        
def sql_zmeny_rozpoctu(rok):
    """
    """

    query_base = """
    SELECT lcs.subjekt_ucetnipohyb.cislo_subjektu AS subjekt_ucetnipohyb_cislo_subjektu,
    lcs.objekt_ucetnipohyb.cislo_objektu AS x___cislo_objektu___x,
    lcs.objekt_ucetnipohyb.cislo_poradace AS objekt_ucetnipohyb_cislo_poradace,
    lcs.subjekt_ucetnipohyb.reference_subjektu AS subjekt_ucetnipohyb_reference_subjektu,
    lcs.objekt_ucetnipohyb.radka AS objekt_ucetnipohyb_radka,
    lcs.objekt_ucetnipohyb.stav AS objekt_ucetnipohyb_stav,
    gstab_1_1.nazev_subjektu AS gstab_1_1_nazev_subjektu,
    lcs.objekt_ucetnipohyb.tok_objektu AS objekt_ucetnipohyb_tok_objektu,
    lcs.objekt_ucetnipohyb.pocetkc AS objekt_ucetnipohyb_pocetkc,
    lcs.objekt_ucetnipohyb.pripad AS objekt_ucetnipohyb_pripad,
    gstab_1_2.nazev_subjektu AS gstab_1_2_nazev_subjektu,
    gstab_1_3.reference_subjektu AS gstab_1_3_reference_subjektu,
    gstab_1_4.reference_subjektu AS gstab_1_4_reference_subjektu,
    lcs.objekt_ucetnipohyb.poznamka AS objekt_ucetnipohyb_poznamka,
    lcs.subjekty.datum_vzniku AS subjekty_datum_vzniku 
    FROM lcs.subjekty 
    JOIN lcs.subjekt_ucetnipohyb 
    ON lcs.subjekty.cislo_subjektu=lcs.subjekt_ucetnipohyb.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.objekt_ucetnipohyb 
    ON lcs.subjekt_ucetnipohyb.cislo_subjektu=lcs.objekt_ucetnipohyb.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.subjekty gstab_1_1 /*1003;l;*/ 
    ON lcs.objekt_ucetnipohyb.ucet=gstab_1_1.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.organizace gstab_1_2 /*1001;l;*/ 
    ON lcs.objekt_ucetnipohyb.organizace=gstab_1_2.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.subjekty gstab_1_3 /*1113;l;*/ 
    ON lcs.objekt_ucetnipohyb.prvotni_doklad=gstab_1_3.cislo_subjektu 
    LEFT 
    OUTER 
    JOIN lcs.subjekty gstab_1_4 /*1002;l;*/ 
    ON lcs.objekt_ucetnipohyb.utvar=gstab_1_4.cislo_subjektu 
    WHERE (lcs.subjekt_ucetnipohyb.cislo_poradace = 10287) 
    """

    query_rok = f" AND (YEAR(lcs.objekt_ucetnipohyb.pripad) = {rok})" 
    query = query_base + query_rok
    return query