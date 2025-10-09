#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
from pyspark import SparkContext


GPS_COL = 0
ADDRESS_COL = 6
HEIGHT_COL = 8


def _can_float(s: str) -> bool:
    try:
        float(s)
        return True
    except Exception:
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit PySpark_arbre_plus_grand.py <input_path> [<output_path>]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "sortie_arbre_plus_grand"

    sc = SparkContext(appName="ArbrePlusGrand")

    try:
        # 1) Lecture du fichier et découpage
        rows = sc.textFile(input_path).map(lambda line: line.split(";"))

        # 2) Filtrer lignes valides: suffisamment de colonnes, hauteur non vide et convertible en float
        valid = (
            rows.filter(lambda p: len(p) > max(GPS_COL, ADDRESS_COL, HEIGHT_COL))
                .filter(lambda p: p[HEIGHT_COL].strip() != "")
                .filter(lambda p: _can_float(p[HEIGHT_COL].strip()))
        )

        # 3) Mapper vers (hauteur_float, (gps, adresse, hauteur_str)) pour réduire sur la hauteur max
        keyed = valid.map(
            lambda p: (
                float(p[HEIGHT_COL].strip()),
                (p[GPS_COL].strip(), p[ADDRESS_COL].strip(), p[HEIGHT_COL].strip()),
            )
        )

        # 4) Réduire pour trouver la hauteur max
        #    retourne (max_height, (gps, adresse, height_str))
        def keep_max(a, b):
            return a if a[0] >= b[0] else b

        tallest = keyed.reduce(keep_max)

        max_height, (gps, adresse, height_str) = tallest

        result_line = f"GPS: {gps} | Hauteur (m): {max_height} | Adresse: {adresse}"

        # 5) Sortie: impression + sauvegarde (coalesce pour un seul fichier part-00000)
        print(result_line)
        sc.parallelize([result_line], 1).saveAsTextFile(output_path)

    finally:
        sc.stop()
