#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import shutil
from typing import Optional
from pyspark.sql import SparkSession, functions as F, Window as W

def build_spark(app_name: str, master: Optional[str]):
    """
    Construit et retourne une SparkSession.
    - app_name : nom de l'application visible dans l'UI Spark.
    - master   : cible d'exécution Spark (ex: 'local[*]', 'yarn', 'spark://...').
      Si None, Spark utilisera sa config par défaut (souvent 'local[*]' dans un conteneur).
    """
    b = SparkSession.builder.appName(app_name)

    # Permet de surcharger le master depuis la CLI (--master) ou une variable d'env.
    if master:
        b = b.master(master)

    # Petites options génériques adaptées aux containers :
    # - On fige le fuseau horaire SQL en UTC pour éviter les surprises avec les timestamps.
    # - On limite le nombre de partitions de shuffle (200 par défaut ici — ajuste si besoin).
    b = (
        b.config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
    )
    return b.getOrCreate()


def export_single_csv_local(df, target_csv_path: str):
    """
    Écrit un DataFrame Spark 'df' en un SEUL fichier CSV local à l'emplacement 'target_csv_path'.
    Pourquoi cette fonction ?
      - Par défaut, Spark écrit en "répertoire" (plusieurs fichiers part-0000*.csv).
      - Ici, on veut un unique CSV "plat" facile à ouvrir.
    Stratégie :
      1) df.coalesce(1).write.csv(...) -> écrit un répertoire temporaire contenant un part-*.csv
      2) On détecte ce part-*.csv et on le déplace/renomme vers target_csv_path
      3) On supprime le répertoire temporaire
    """
    target_csv_path = os.path.abspath(target_csv_path)
    tmpdir = target_csv_path + ".tmpdir"  # répertoire temporaire d'écriture

    # Nettoyage préalable au cas où un ancien répertoire existe
    shutil.rmtree(tmpdir, ignore_errors=True)

    # Écriture sur le FS LOCAL : on préfixe par 'file://' pour forcer Spark à ne pas utiliser HDFS
    (
        df.coalesce(1)                       # force 1 seul "part" (un seul CSV)
          .write.mode("overwrite")           # écrase si existe
          .option("header", True)            # inclut la ligne d'en-tête
          .csv("file://" + tmpdir)           # chemin local -> répertoire contenant part-*.csv
    )

    # On récupère le nom du fichier part-*.csv écrit par Spark
    parts = [f for f in os.listdir(tmpdir) if f.startswith("part-") and f.endswith(".csv")]
    if not parts:
        # Si aucun part-*.csv trouvé, on remonte une erreur explicite
        raise RuntimeError(f"Aucun part-*.csv écrit dans {tmpdir}")
    part_csv = os.path.join(tmpdir, parts[0])

    # On s’assure que le dossier parent de la destination finale existe
    os.makedirs(os.path.dirname(target_csv_path), exist_ok=True)

    # On déplace/renomme le part-*.csv vers le chemin final demandé
    shutil.move(part_csv, target_csv_path)

    # On supprime le répertoire temporaire (nettoyage)
    shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    # === Parsing des arguments CLI ===
    parser = argparse.ArgumentParser(
        description=("MovieLens — détection des films les plus polarisants (dé-biaisage par utilisateur). "
                     "Exporte top_global.csv et top5_by_genre.csv dans le dossier du script.")
    )

    # Chemin des données (dossier contenant movies.csv et ratings.csv).
    # Peut être un chemin local (/… ou file:///…) ou un préfixe HDFS (hdfs://…).
    parser.add_argument(
        "--data-dir",
        default=os.getenv("MOVIELENS_DIR", "/data/ml-latest-small"),
        help=("Chemin du dossier contenant movies.csv et ratings.csv "
              "(ou un préfixe HDFS). Ex: /data/ml-latest-small ou "
              "hdfs://namenode:8020/datasets/ml-latest-small"),
    )

    # Master Spark (exécution locale par défaut dans un conteneur).
    parser.add_argument(
        "--master",
        default=os.getenv("SPARK_MASTER", "local[*]"),
        help="Spark master (ex: local[*], spark://..., yarn). Défaut: local[*].",
    )

    # Seuil minimal de notes par film (robustesse).
    parser.add_argument(
        "--min-n",
        type=int,
        default=int(os.getenv("MIN_RATINGS", "30")),
        help="Seuil minimal de notes par film (défaut: 30).",
    )

    # Seuil d'extrêmes sur résidus (assure présence d'avis très positifs et très négatifs).
    parser.add_argument(
        "--min-resid-extreme",
        type=float,
        default=float(os.getenv("MIN_RESID_EXTREME", "0.75")),
        help="Seuil d'extrêmes sur résidus (défaut: 0.75).",
    )

    # Nombre d’entrées à produire dans le top global.
    parser.add_argument(
        "--topk",
        type=int,
        default=int(os.getenv("TOPK", "25")),
        help="Nombre de films à afficher dans le top global (défaut: 25).",
    )
    args = parser.parse_args()

    # === Forcer Python 3 côté exécuteurs/driver si plusieurs versions ===
    os.environ.setdefault("PYSPARK_PYTHON", "python3")
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python3")

    # === Création de la session Spark ===
    spark = build_spark("MovieLens-Polarizing-Movies (Docker)", args.master)

    # On calcule le dossier du script pour sortir les CSV à côté du .py
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_csv_global = os.path.join(script_dir, "top_global.csv")
    out_csv_by_genre = os.path.join(script_dir, "top5_by_genre.csv")

    # Résolution des chemins vers les fichiers d'entrée
    base = args.data_dir.rstrip("/")
    ratings_path = f"{base}/ratings.csv"
    movies_path  = f"{base}/movies.csv"

    # === Chargement des données CSV ===
    # ratings: colonnes [userId, movieId, rating, timestamp]
    # movies : colonnes [movieId, title, genres]
    # inferSchema=True infère les types (int/double), pratique pour éviter des casts manuels.
    ratings = spark.read.csv(ratings_path, header=True, inferSchema=True)
    movies  = spark.read.csv(movies_path,  header=True, inferSchema=True)

    # === Débiaisage par utilisateur ===
    # Idée : chaque utilisateur a un "niveau" de notation propre (biais perso).
    # On calcule la moyenne des notes par utilisateur (user_mean),
    # puis on soustrait cette moyenne à chaque note -> "résidu".
    user_stats = ratings.groupBy("userId").agg(F.avg("rating").alias("user_mean"))
    ratings_adj = (
        ratings.join(user_stats, "userId", "inner")
               .withColumn("resid", F.col("rating") - F.col("user_mean"))
    )

    # === Caractéristiques par film sur les résidus ===
    # n         : nombre de notes
    # resid_std : écart-type des résidus (dispersion dé-biaisée)
    # min/max   : bornes des résidus (présence d'avis très négatifs/positifs vs goût habituel)
    # mean_resid: moyenne des résidus (devrait être proche de 0 globalement)
    # prop_pos  : part des résidus > 0 (part d'avis au-dessus du goût habituel)
    movie_dist = ratings_adj.groupBy("movieId").agg(
        F.count("*").alias("n"),
        F.stddev_samp("resid").alias("resid_std"),
        F.min("resid").alias("min_resid"),
        F.max("resid").alias("max_resid"),
        F.avg("resid").alias("mean_resid"),
        F.avg(F.when(F.col("resid") > 0, 1.0).otherwise(0.0)).alias("prop_pos"),
    )

    # === Score de controverse ===
    # Un film polarisant a :
    #   - une forte dispersion des avis (resid_std élevé)
    #   - un équilibre entre avis positifs et négatifs (≈ 50/50)
    # balance = 1 - 2*|prop_pos - 0.5|  -> vaut 1 quand prop_pos=0.5, diminue quand on s'en éloigne.
    # controversy = resid_std * balance
    movie_dist = movie_dist.withColumn(
        "balance", 1.0 - F.abs(F.col("prop_pos") - F.lit(0.5)) * 2.0
    ).withColumn(
        "controversy", F.col("resid_std") * F.col("balance")
    )

    # === Filtrage robuste ===
    # On impose :
    # - au moins 'min_n' notes
    # - présence d'extrêmes sur les résidus (< -thr et > +thr)
    # - resid_std non nul
    thr = float(args.min_resid_extreme)
    filtered = movie_dist.filter(
        (F.col("n") >= args.min_n) &
        (F.col("min_resid") < -thr) &
        (F.col("max_resid") >  thr) &
        F.col("resid_std").isNotNull()
    )

    # === Top global des films polarisants ===
    # Tri principal par controverses décroissantes,
    # puis par resid_std (pour départager), puis n (volume d'avis).
    # On rejoint ensuite pour récupérer title & genres.
    top_global = (
        filtered
        .orderBy(F.col("controversy").desc(), F.col("resid_std").desc(), F.col("n").desc())
        .limit(args.topk)
        .join(movies.select("movieId", "title", "genres"), "movieId", "left")
        .select(
            "movieId", "title", "n", "resid_std", "prop_pos", "balance",
            "controversy", "mean_resid", "min_resid", "max_resid", "genres"
        )
    )

    # === Top 5 par genre ===
    # - On "explose" les genres (séparés par '|') pour avoir une ligne par (film, genre).
    # - On rejoint avec 'filtered' et on classe par genre via une fenêtre analytique.
    movies_exp = (
        movies.select(
            "movieId",
            "title",
            F.split(F.col("genres"), "\\|").alias("genres_arr")
        )
        .withColumn("genre", F.explode("genres_arr"))
    )

    by_genre = filtered.join(movies_exp, "movieId", "left")

    # Fenêtre de ranking : partitionnée par 'genre', triée par controverse puis resid_std puis n
    w = W.partitionBy("genre").orderBy(
        F.col("controversy").desc(), F.col("resid_std").desc(), F.col("n").desc()
    )

    # On garde les 5 premiers par genre (rk <= 5) et on ordonne joliment.
    top5_by_genre = (
        by_genre
        .withColumn("rk", F.row_number().over(w))
        .filter(F.col("rk") <= 5)
        .select("genre", "rk", "movieId", "title", "n", "resid_std", "balance", "controversy")
        .orderBy("genre", "rk")
    )

    # === Affichages console (pratique pour voir dans les logs du conteneur) ===
    print("=== TOP FILMS LES PLUS POLARISANTS (dé-biaisés) ===")
    top_global.show(truncate=False, n=args.topk)

    print("=== TOP 5 PAR GENRE (les plus polarisants) ===")
    top5_by_genre.show(truncate=False, n=200)

    # === Exports CSV "plats" dans le dossier du script ===
    # On capture les erreurs d'export pour éviter de faire échouer tout le job si
    # un problème de permission ou de FS survient (on loggue un WARN).
    try:
        export_single_csv_local(top_global, out_csv_global)
        export_single_csv_local(top5_by_genre, out_csv_by_genre)
        print(f"[INFO] CSV écrits :\n - {out_csv_global}\n - {out_csv_by_genre}")
    except Exception as e:
        print(f"[WARN] Échec export CSV local : {e}")

    # === Arrêt propre de Spark ===
    spark.stop()


if __name__ == "__main__":
    # Point d'entrée du script : on lance la logique principale.
    main()
