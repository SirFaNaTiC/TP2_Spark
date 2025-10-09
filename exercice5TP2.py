#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import argparse
import shutil
from typing import Optional
from pyspark.sql import SparkSession, functions as F, Window as W

def build_spark(app_name: str, master: Optional[str]):
    b = SparkSession.builder.appName(app_name)
    if master:
        b = b.master(master)
    b = (
        b.config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
    )
    return b.getOrCreate()


def export_single_csv_local(df, target_csv_path: str):
    target_csv_path = os.path.abspath(target_csv_path)
    tmpdir = target_csv_path + ".tmpdir"
    shutil.rmtree(tmpdir, ignore_errors=True)
    (
        df.coalesce(1)
          .write.mode("overwrite")
          .option("header", True)
          .csv("file://" + tmpdir)
    )
    parts = [f for f in os.listdir(tmpdir) if f.startswith("part-") and f.endswith(".csv")]
    if not parts:
        raise RuntimeError(f"Aucun part-*.csv écrit dans {tmpdir}")
    part_csv = os.path.join(tmpdir, parts[0])
    os.makedirs(os.path.dirname(target_csv_path), exist_ok=True)
    shutil.move(part_csv, target_csv_path)
    shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(
        description=("MovieLens — détection des films les plus polarisants (dé-biaisage par utilisateur). Exporte top_global.csv et top5_by_genre.csv dans le dossier du script.")
    )
    parser.add_argument(
        "--data-dir",
        default=os.getenv("MOVIELENS_DIR", "/data/ml-latest-small"),
        help=("Chemin du dossier contenant movies.csv et ratings.csv (ou un préfixe HDFS). Ex: /data/ml-latest-small ou hdfs://namenode:8020/datasets/ml-latest-small"),
    )
    parser.add_argument(
        "--master",
        default=os.getenv("SPARK_MASTER", "local[*]"),
        help="Spark master (ex: local[*], spark://..., yarn). Défaut: local[*].",
    )
    parser.add_argument(
        "--min-n",
        type=int,
        default=int(os.getenv("MIN_RATINGS", "30")),
        help="Seuil minimal de notes par film (défaut: 30).",
    )
    parser.add_argument(
        "--min-resid-extreme",
        type=float,
        default=float(os.getenv("MIN_RESID_EXTREME", "0.75")),
        help="Seuil d'extrêmes sur résidus (défaut: 0.75).",
    )
    parser.add_argument(
        "--topk",
        type=int,
        default=int(os.getenv("TOPK", "25")),
        help="Nombre de films à afficher dans le top global (défaut: 25).",
    )
    args = parser.parse_args()
    os.environ.setdefault("PYSPARK_PYTHON", "python3")
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python3")
    spark = build_spark("MovieLens-Polarizing-Movies (Docker)", args.master)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_csv_global = os.path.join(script_dir, "top_global.csv")
    out_csv_by_genre = os.path.join(script_dir, "top5_by_genre.csv")
    base = args.data_dir.rstrip("/")
    ratings_path = f"{base}/ratings.csv"
    movies_path  = f"{base}/movies.csv"
    ratings = spark.read.csv(ratings_path, header=True, inferSchema=True)
    movies  = spark.read.csv(movies_path,  header=True, inferSchema=True)
    user_stats = ratings.groupBy("userId").agg(F.avg("rating").alias("user_mean"))
    ratings_adj = (
        ratings.join(user_stats, "userId", "inner")
               .withColumn("resid", F.col("rating") - F.col("user_mean"))
    )
    movie_dist = ratings_adj.groupBy("movieId").agg(
        F.count("*").alias("n"),
        F.stddev_samp("resid").alias("resid_std"),
        F.min("resid").alias("min_resid"),
        F.max("resid").alias("max_resid"),
        F.avg("resid").alias("mean_resid"),
        F.avg(F.when(F.col("resid") > 0, 1.0).otherwise(0.0)).alias("prop_pos"),
    )
    movie_dist = movie_dist.withColumn(
        "balance", 1.0 - F.abs(F.col("prop_pos") - F.lit(0.5)) * 2.0
    ).withColumn(
        "controversy", F.col("resid_std") * F.col("balance")
    )
    thr = float(args.min_resid_extreme)
    filtered = movie_dist.filter(
        (F.col("n") >= args.min_n) &
        (F.col("min_resid") < -thr) &
        (F.col("max_resid") >  thr) &
        F.col("resid_std").isNotNull()
    )
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
    movies_exp = (
        movies.select(
            "movieId",
            "title",
            F.split(F.col("genres"), "\\|").alias("genres_arr")
        )
        .withColumn("genre", F.explode("genres_arr"))
    )
    by_genre = filtered.join(movies_exp, "movieId", "left")
    w = W.partitionBy("genre").orderBy(
        F.col("controversy").desc(), F.col("resid_std").desc(), F.col("n").desc()
    )
    top5_by_genre = (
        by_genre
        .withColumn("rk", F.row_number().over(w))
        .filter(F.col("rk") <= 5)
        .select("genre", "rk", "movieId", "title", "n", "resid_std", "balance", "controversy")
        .orderBy("genre", "rk")
    )
    print("=== TOP FILMS LES PLUS POLARISANTS (dé-biaisés) ===")
    top_global.show(truncate=False, n=args.topk)
    print("=== TOP 5 PAR GENRE (les plus polarisants) ===")
    top5_by_genre.show(truncate=False, n=200)
    try:
        export_single_csv_local(top_global, out_csv_global)
        export_single_csv_local(top5_by_genre, out_csv_by_genre)
        print(f"[INFO] CSV écrits :\n - {out_csv_global}\n - {out_csv_by_genre}")
    except Exception as e:
        print(f"[WARN] Échec export CSV local : {e}")
    spark.stop()


if __name__ == "__main__":
    main()
