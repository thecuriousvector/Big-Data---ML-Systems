from pyspark import SparkContext, SparkConf
from operator import add
import re, sys



sc = SparkContext()
lines=sc.textFile("/user/rdv253/MaxIntegerInput.txt")

numbers = lines.map(lambda num: num.split("\t"))

largest = numbers.map(lambda num: max(num)).saveAsTextFile("largestinteger_output.txt")

