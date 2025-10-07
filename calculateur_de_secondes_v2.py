#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from functools import reduce
import argparse
import sys

def seconds_functional(hms: str) -> int:
    """
    Convertit 'hh:mm:ss' en secondes (approche fonctionnelle: map + reduce).
    """
    parts = hms.strip().split(':')
    if len(parts) != 3:
        raise ValueError("Format attendu: hh:mm:ss")

    try:
        h, m, s = map(int, parts)  # map(...)
    except ValueError:
        raise ValueError("Les composantes doivent être des entiers.")

    if not (0 <= h < 24):
        raise ValueError("Heure invalide: hh doit être entre 0 et 23.")
    if not (0 <= m < 60):
        raise ValueError("Minutes invalides: mm doit être entre 0 et 59.")
    if not (0 <= s < 60):
        raise ValueError("Secondes invalides: ss doit être entre 0 et 59.")

    weights = (3600, 60, 1)
    # reduce: somme des produits valeur*poids
    return reduce(lambda acc, vs: acc + vs[0] * vs[1], zip((h, m, s), weights), 0)

def main() -> int:
    parser = argparse.ArgumentParser(description="Calculateur de secondes (version fonctionnelle).")
    parser.add_argument("time", nargs="?", help="Heure au format hh:mm:ss (ex: 08:19:22)")
    args = parser.parse_args()

    time_str = args.time if args.time else input("Entrez une heure au format hh:mm:ss: ")

    try:
        print(seconds_functional(time_str))
        return 0
    except ValueError as e:
        print(f"Erreur: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
