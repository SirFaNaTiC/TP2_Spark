#!/usr/bin/env python
import sys

from pyspark import SparkContext

if __name__ == "__main__":

	# Creation d'un contexte spark
	sc = SparkContext(appName="Spark Count")

	# Lecture du fichier et decomposition du fichier en mots
	tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))

	# Comptage de l'occurence de chaque mots
	wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2 : v1 + v2)

	# Stockage du resultat sur HDFS 
	# ne pas oublier "hadoop fs -rm -r -f sortie" entre 2 exécutions!
	wordCounts.saveAsTextFile("sortie")
	
	# Arret du contexte Spark
	sc.stop()
	
