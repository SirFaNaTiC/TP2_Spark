#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
    if len(fields) <= COMMUNE_COL:
        return None
    commune = fields[COMMUNE_COL].strip()
    m = re.search(r"(\d{1,2})\s*[eE]", commune)
    if m:
        num = int(m.group(1))
        if 1 <= num <= 20:
            return f"{num:02d}"
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
        rows = sc.textFile(input_path).map(lambda line: line.split(";"))

        def to_record(p):
            arr = _extract_arrondissement(p)
            circ_str = p[CIRC_COL].strip() if len(p) > CIRC_COL else ""
            gps = p[GPS_COL].strip() if len(p) > GPS_COL else ""
            return arr, gps, circ_str

        valid = (
            rows.map(to_record)
            .filter(lambda t: t[0] is not None and t[2] != "" and _can_float(t[2]))
            .map(lambda t: (t[0], (t[1], float(t[2]))))
        )

        max_by_arr = valid.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda a, b: a if a >= b else b)

        joined = valid.join(max_by_arr)
        winners = (
            joined.filter(lambda kv: kv[1][0][1] == kv[1][1])
                  .map(lambda kv: (kv[0], kv[1][0][0]))
                  .groupByKey()
                  .mapValues(lambda it: sorted(set(it)))
        )

        lines = (
            winners.map(lambda kv: (kv[0], ", ".join(kv[1])))
                   .sortByKey()
                   .map(lambda kv: f"Arrondissement {int(kv[0])}: {kv[1]}")
        )

        for l in lines.collect():
            print(l)
        lines.coalesce(1).saveAsTextFile(output_path)

    finally:
        sc.stop()
