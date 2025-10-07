#!/usr/bin/env python3

"""
Programme: Calculateur de secondes

Objectif:
- Convertit une heure au format hh:mm:ss en nombre total de secondes.

Exemple:
- 8:19:22 -> 29962
"""

from typing import Tuple


def parse_time_to_seconds(hms: str) -> int:
    """Convertit une chaîne "hh:mm:ss" en nombre de secondes.

    Paramètres:
        hms: Chaîne de caractères représentant l'heure au format hh:mm:ss

    Retour:
        Nombre total de secondes (int)

    Lève:
        ValueError si le format ou les valeurs sont invalides.
    """
    parts = hms.strip().split(":")
    if len(parts) != 3:
        raise ValueError("Format attendu: hh:mm:ss")

    try:
        h, m, s = (int(p) for p in parts)
    except ValueError:
        raise ValueError("Les composantes doivent être des entiers.")

    if not (0 <= h < 24):
        raise ValueError("Heure invalide: hh doit être entre 0 et 23.")
    if not (0 <= m < 60):
        raise ValueError("Minutes invalides: mm doit être entre 0 et 59.")
    if not (0 <= s < 60):
        raise ValueError("Secondes invalides: ss doit être entre 0 et 59.")

    return h * 3600 + m * 60 + s


def main() -> None:
    import sys

    # Permet d'utiliser un argument en ligne de commande ou l'entrée utilisateur
    if len(sys.argv) > 1:
        time_str = sys.argv[1]
    else:
        time_str = input("Entrez une heure au format hh:mm:ss: ")

    try:
        total_seconds = parse_time_to_seconds(time_str)
        print(total_seconds)
    except ValueError as e:
        print(f"Erreur: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
