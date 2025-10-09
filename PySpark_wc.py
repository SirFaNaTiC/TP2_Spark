#!/usr/bin/env python
import sys

from pyspark import SparkContext

if __name__ == "__main__":

	sc = SparkContext(appName="Spark Count")
	tokenized = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))
	wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2 : v1 + v2)
	if len(sys.argv) > 2:
		n = int(sys.argv[2])
		filteredWordCounts = wordCounts.filter(lambda x: x[1] >= n)
	else:
		filteredWordCounts = wordCounts
	filteredWordCounts.saveAsTextFile("sortie")
	sc.stop()
	
