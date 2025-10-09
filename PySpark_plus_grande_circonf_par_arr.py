#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Afficher les coordonnées GPS des arbres de plus grandes circonférences pour
chaque arrondissement de Paris (format texte ';').

Hypothèses d'après l'échantillon fourni:
- [0]  = "lat, lon" (coordonnées GPS)
- [3]  = libellé commune (ex: "PARIS 18E ARRDT") — sert à extraire le n° d'arrondissement
- [7]  = circonférence (en cm le plus souvent) — à convertir en float

Si votre fichier a une structure différente, ajustez les index en tête de fichier.
Le script gère les ex-aequos (plusieurs arbres avec la même circonférence max
dans un arrondissement) et renvoie toutes leurs coordonnées GPS.
"""

import re
import sys
from typing import Optional
from pyspark import SparkContext


GPS_COL = 0
COMMUNE_COL = 3
CIRC_COL = 7


def _can_float(s: str) -> bool:
    try:
        float(s)
        return True
    except Exception:
        return False


def _extract_arrondissement(fields) -> Optional[str]:
    """Extrait le numéro d'arrondissement depuis le libellé de commune.

    Cherche un nombre suivi de 'E' dans la chaîne, ex: 'PARIS 20E ARRDT' -> '20'.
    Retourne None si non trouvé.
    """
    if len(fields) <= COMMUNE_COL:
        return None
    commune = fields[COMMUNE_COL].strip()
    m = re.search(r"(\d{1,2})\s*[eE]", commune)
    if m:
        num = int(m.group(1))
        if 1 <= num <= 20:
            return f"{num:02d}"  # normalise sur 2 chiffres (ex: '01', '20')
    # fallback: essayer de trouver un entier tout court dans la chaîne
    m2 = re.search(r"\b(\d{1,2})\b", commune)
    if m2:
        num = int(m2.group(1))
        if 1 <= num <= 20:
            return f"{num:02d}"
    return None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit PySpark_plus_grande_circonf_par_arr.py <input_path> [<output_path>]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "sortie_plus_grandes_circonf"

    sc = SparkContext(appName="PlusGrandesCirconferencesParArrondissement")

    try:
        # 1) Lecture et split
        rows = sc.textFile(input_path).map(lambda line: line.split(";"))

        # 2) Lignes valides: circonf non vide/numérique et arrondissement détecté
        def to_record(p):
            arr = _extract_arrondissement(p)
            circ_str = p[CIRC_COL].strip() if len(p) > CIRC_COL else ""
            gps = p[GPS_COL].strip() if len(p) > GPS_COL else ""
            return arr, gps, circ_str

        valid = (
            rows.map(to_record)
            .filter(lambda t: t[0] is not None and t[2] != "" and _can_float(t[2]))
            .map(lambda t: (t[0], (t[1], float(t[2]))))  # (arr, (gps, circ))
        )

        # 3) Max circonférence par arrondissement
        max_by_arr = valid.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda a, b: a if a >= b else b)

        # 4) Joindre pour récupérer les GPS correspondant au max (gère les ex-aequos)
        joined = valid.join(max_by_arr)  # (arr, ((gps, circ), max_circ))
        winners = (
            joined.filter(lambda kv: kv[1][0][1] == kv[1][1])
                  .map(lambda kv: (kv[0], kv[1][0][0]))  # (arr, gps)
                  .groupByKey()
                  .mapValues(lambda it: sorted(set(it)))
        )

        # 5) Mise en forme et sortie triée par arrondissement
        lines = (
            winners.map(lambda kv: (kv[0], ", ".join(kv[1])))
                   .sortByKey()
                   .map(lambda kv: f"Arrondissement {int(kv[0])}: {kv[1]}")
        )

        # Print + sauvegarde (un seul fichier)
        for l in lines.collect():
            print(l)
        lines.coalesce(1).saveAsTextFile(output_path)

    finally:
        sc.stop()
