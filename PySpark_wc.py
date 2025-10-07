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
	
	# Récupération de la valeur n depuis les arguments de ligne de commande
	if len(sys.argv) > 2:
		n = int(sys.argv[2])
		# Filtrage des mots qui apparaissent au moins n fois
		filteredWordCounts = wordCounts.filter(lambda x: x[1] >= n)
	else:
		# Si aucun argument n n'est fourni, on garde tous les mots
		filteredWordCounts = wordCounts

	# Stockage du resultat sur HDFS 
	# ne pas oublier "hadoop fs -rm -r -f sortie" entre 2 exécutions!
	filteredWordCounts.saveAsTextFile("sortie")
	
	# Arret du contexte Spark
	sc.stop()
	
