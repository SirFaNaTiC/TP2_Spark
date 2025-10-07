#!/usr/bin/env python3
#-*- coding: utf-8 -*-
import sys
from operator import add
from random import random

from pyspark import SparkContext

def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0

if __name__ == "__main__":

    sc = SparkContext(appName="PySpark_Pi")
    sc.setLogLevel("ERROR") 

    n = 100000
    count = sc.parallelize(range(1, n + 1)).map(f).reduce(add)
    print ("Pi is roughly ", 4.0 * count / n)

    sc.stop()
