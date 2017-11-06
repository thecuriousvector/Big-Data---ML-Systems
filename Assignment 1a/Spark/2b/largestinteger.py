
from pyspark import SparkContext, SparkConf
from operator import add
import re, sys



sc = SparkContext()
lines=sc.textFile("/user/rdv253/MaxIntegerInput.txt")

numbers = lines.flatMap(lambda num: num.split("\t"))

count = numbers.count()
largest = numbers.reduce(lambda x,y:max(x,y))
sum = numbers.reduce(lambda x,y: float(x)+float(y))

set_distinct = numbers.distinct()
distinctnumber = numbers.distinct().count()

average = float(sum)/count

print "largest Integer:" + str(largest)


print "Average of all integers:"+ str(average)

print "Set of unique integers:" +str(set_distinct.collect())
print "Count of dictinct Integers: " + str(distinctnumber)
