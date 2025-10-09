#!/usr/bin/env python
# -*- coding: utf-8 -*-
""""""
import sys
from pyspark import SparkContext


GENRE_COL = 11
ESPECE_COL = 12


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit PySpark_especes_par_genre.py <input_path> [<output_path>]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "sortie_especes_par_genre"

    sc = SparkContext(appName="EspecesParGenre")

    try:
        rows = sc.textFile(input_path).map(lambda line: line.split(";"))

        pairs = (
            rows.filter(lambda p: len(p) > max(GENRE_COL, ESPECE_COL))
                .map(lambda p: (p[GENRE_COL].strip(), p[ESPECE_COL].strip()))
                .filter(lambda t: t[0] != "" and t[1] != "")
                .distinct()
        )

        grouped = (
            pairs.groupByKey()
                 .mapValues(lambda it: sorted(set(it), key=lambda s: s.lower()))
        )

        lines = (
            grouped.map(lambda kv: (kv[0], ", ".join(kv[1])))
                   .sortByKey(keyfunc=lambda s: s.lower())
                   .map(lambda kv: f"{kv[0]}: {kv[1]}")
        )

        for l in lines.collect():
            print(l)

        lines.coalesce(1).saveAsTextFile(output_path)

    finally:
        sc.stop()
